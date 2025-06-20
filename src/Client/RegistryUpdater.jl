module RegistryUpdater

using Downloads, Printf
using Zstd_jll, SHA

export simulate_registry_update, download_missing_chunks

"""
    download_dict(dir::String, dict_id::UInt32; server_base_url::String)

Download a zstd dictionary if it doesn't already exist locally.
"""
function download_dict(dir::String, dict_id::UInt32; server_base_url::String = "http://localhost:8000/server_chunk_store")
    # Nothing to do for dict ID 0, which means no dictionary
    if dict_id == 0
        return
    end

    dict_name = zstd_dict_name(dict_id)
    dict_path = joinpath(dir, dict_name)
    if !isfile(dict_path)
        Downloads.download("http://localhost:8000/server_chunk_store/$(dict_name)", dict_path)
    end
end

"""
    download_missing_chunks(missing_chunks, chunk_store_path::String, temp_dir::String; server_base_url::String)

Download missing chunks from the server and return the total bytes downloaded.
"""
function download_missing_chunks(missing_chunks, chunk_store_path::String, temp_dir::String; server_base_url::String = "http://localhost:8000/server_chunk_store")
    bytes_downloaded = 0
    @info("Downloading $(length(missing_chunks)) chunks")
    
    for missing_chunk in missing_chunks
        chunk_str = string(missing_chunk)
        missing_chunk_path = chunk_path(missing_chunk, chunk_store_path)
        mkpath(dirname(missing_chunk_path))
        missing_chunk_url = chunk_path(missing_chunk, server_base_url)
        Downloads.download(missing_chunk_url, missing_chunk_path)
        bytes_downloaded += filesize(missing_chunk_path)

        # Download any new dictionaries we need for these chunks
        download_dict(temp_dir, only(list_frames(missing_chunk_path)).dictionary_id)
    end
    
    return bytes_downloaded
end

"""
    simulate_registry_update(old_registry_hash::String; server_base_url::String)

Simulate a complete registry update process, starting with an old registry
and downloading only the chunks needed for the new version.
"""
function simulate_registry_update(old_registry_hash::String; server_base_url::String = "http://localhost:8000")
    # It's funny, but if you set this to a completely random file, the script
    # will still work, it just downloads all chunks from the server.  :)
    old_registry = joinpath(registries_dir, "$(old_registry_hash).zst")

    # Download new registry.  We download everything into `dir`, to simulate
    # running on a machine that has nothing but the old registry available.
    dir = mktempdir()
    latest_index = joinpath(dir, "latest.caibx")
    output_file = joinpath(dir, "General.tar.zst")
    chunk_store_path = joinpath(dir, "chunk_store")
    Downloads.download("$(server_base_url)/indexes/latest", latest_index)
    latest_chunks = load_chunk_index(latest_index)

    old_registry_chunks = load_seed_chunks(old_registry)

    # Make sure we have the right dictionary for any chunk we're going to use
    for chunk in old_registry_chunks
        download_dict(dir, chunk.dict_id; server_base_url="$(server_base_url)/server_chunk_store")
    end

    missing_chunks = setdiff(Set(latest_chunks), Set([c.id for c in old_registry_chunks]))
    
    bytes_downloaded = download_missing_chunks(missing_chunks, chunk_store_path, dir; server_base_url="$(server_base_url)/server_chunk_store")

    synth_time = @elapsed synthesize(
        output_file,
        latest_chunks,
        chunk_store_path,
        [old_registry],
    )
    @info("Synthesis complete", synth_time, filesize(output_file))

    # Verify the synthesized file
    zstd_dicts = filter(endswith(".zstdict"), readdir(dir))
    function zstd_content_shasum(path::String)
        io = IOBuffer()
        zstd_cmd = `$(zstd()) -q -d $(path) -c`
        for dict_name in zstd_dicts
            push!(zstd_cmd.exec, "-D")
            push!(zstd_cmd.exec, joinpath(dir, dict_name))
        end
        run(pipeline(zstd_cmd, stdout=io))
        return bytes2hex(sha256(take!(io)))
    end

    # check to make sure the hash of the extracted contents are the same
    @info("Synthesized file:", path=output_file, hash=zstd_content_shasum(output_file))
    ground_truth = joinpath(registries_dir, registry_hashes[end])
    @info("Ground truth file:", path=ground_truth, hash=zstd_content_shasum(ground_truth))

    @info("Final stats",
        bytes_downloaded,
        download_percentage=@sprintf("%.2f%%", bytes_downloaded*100.0/filesize(output_file)),
        synth_time=@sprintf("%.1fms", synth_time*1000.0),
    )
    
    return (bytes_downloaded=bytes_downloaded, output_file=output_file, synth_time=synth_time)
end

end # module RegistryUpdater
