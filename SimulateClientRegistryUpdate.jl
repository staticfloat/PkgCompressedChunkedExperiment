#!/usr/bin/env -S julia --project

# This simulates a registry update on a client's machine
# They will have one registry ("old registry") and will need to download
# a new one ("new registry") from the server, but will only download the
# chunks that it needs.
include("CasyncClient.jl")
using .CasyncClient
using Downloads, Printf

include("registry_hashes.jl")

## Select one of these to run a different simulation
# Small delta
#old_registry_hash = registry_hashes[end-1]

# Medium delta
#old_registry_hash = registry_hashes[end-2]

# Large delta
old_registry_hash = registry_hashes[1]

# It's funny, but if you set this to a completely random file, the script
# will still work, it just downloads all chunks from the server.  :)
old_registry = joinpath(@__DIR__, "registries", "$(old_registry_hash).zst")

# Download new registry.  We download everything into `dir`, to simulate
# running on a machine that has nothing but the old registry available.
dir = mktempdir()
latest_index = joinpath(dir, "latest.caibx")
output_file = joinpath(dir, "General.tar.zst")
chunk_store_path = joinpath(dir, "chunk_store")
Downloads.download("http://localhost:8000/indexes/latest", latest_index)
latest_chunks = CasyncClient.load_chunk_index(latest_index)

old_registry_chunks = load_seed_chunks(old_registry)

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

# Make sure we have the right dictionary for any chunk we're going to use
for chunk in old_registry_chunks
    download_dict(dir, chunk.dict_id)
end

missing_chunks = setdiff(Set(latest_chunks), Set([c.id for c in old_registry_chunks]))
@info("Downloading $(length(missing_chunks)) chunks")

bytes_downloaded = 0
for missing_chunk in missing_chunks
    chunk_str = string(missing_chunk)
    missing_chunk_path = chunk_path(missing_chunk, chunk_store_path)
    mkpath(dirname(missing_chunk_path))
    missing_chunk_url = chunk_path(missing_chunk, "http://localhost:8000/server_chunk_store")
    Downloads.download(missing_chunk_url, missing_chunk_path)
    global bytes_downloaded += filesize(missing_chunk_path)

    # Download any new dictionaries we need for these chunks
    download_dict(dir, only(CasyncClient.list_frames(missing_chunk_path)).dictionary_id)
end

synth_time = CasyncClient.synthesize(
    output_file,
    latest_chunks,
    chunk_store_path,
    [old_registry],
)
synth_time = @elapsed CasyncClient.synthesize(
    output_file,
    latest_chunks,
    chunk_store_path,
    [old_registry],
)
@info("Synthesis complete", synth_time, filesize(output_file))


using Zstd_jll, SHA
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
ground_truth = joinpath(@__DIR__, "registries", registry_hashes[end])
@info("Ground truth file:", path=ground_truth, hash=zstd_content_shasum(ground_truth))

@info("Final stats",
    bytes_downloaded,
    download_percentage=@sprintf("%.2f%%", bytes_downloaded*100.0/filesize(output_file)),
    synth_time=@sprintf("%.1fms", synth_time*1000.0),
)
