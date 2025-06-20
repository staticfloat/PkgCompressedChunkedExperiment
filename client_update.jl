#!/usr/bin/env -S julia --project

# Client-side script: Simulates a registry update by downloading only missing chunks

include("src/Common/Common.jl")
include("test/RegistryUtils.jl")
include("src/Client/ChunkIndexing.jl")
include("src/Client/ChunkSynthesis.jl")
include("src/Client/RegistryUpdater.jl")

using .Common
using .RegistryUtils
using .ChunkIndexing
using .ChunkSynthesis
using .RegistryUpdater
using Downloads, Printf

# Create wrapper functions
function load_chunk_index_wrapper(index_path::String)
    return load_chunk_index(index_path)
end

function load_seed_chunks_wrapper(file::String)
    return load_seed_chunks(file)
end

function synthesize_wrapper(output_path::String, chunks, chunk_store_path::String, seed_files::Vector{String} = String[])
    return synthesize(output_path, chunks, chunk_store_path, seed_files)
end

# Simulate registry update manually since we need the wrappers
function simulate_registry_update_wrapper(old_registry_hash::String; server_base_url::String = "http://localhost:8000")
    old_registry = joinpath(registries_dir, "$(old_registry_hash).zst")

    # Download new registry.  We download everything into `dir`, to simulate
    # running on a machine that has nothing but the old registry available.
    dir = mktempdir()
    latest_index = joinpath(dir, "latest.caibx")
    output_file = joinpath(dir, "General.tar.zst")
    chunk_store_path = joinpath(dir, "chunk_store")
    Downloads.download("$(server_base_url)/indexes/latest", latest_index)
    latest_chunks = load_chunk_index_wrapper(latest_index)

    old_registry_chunks = load_seed_chunks_wrapper(old_registry)

    # Download dictionaries
    for chunk in old_registry_chunks
        if chunk.dict_id != 0
            dict_name = zstd_dict_name(chunk.dict_id)
            dict_path = joinpath(dir, dict_name)
            if !isfile(dict_path)
                Downloads.download("$(server_base_url)/server_chunk_store/$(dict_name)", dict_path)
            end
        end
    end

    missing_chunks = setdiff(Set(latest_chunks), Set([c.id for c in old_registry_chunks]))
    @info("Downloading $(length(missing_chunks)) chunks")

    bytes_downloaded = 0
    for missing_chunk in missing_chunks
        chunk_str = string(missing_chunk)
        missing_chunk_path = chunk_path(missing_chunk, chunk_store_path)
        mkpath(dirname(missing_chunk_path))
        missing_chunk_url = chunk_path(missing_chunk, "$(server_base_url)/server_chunk_store")
        Downloads.download(missing_chunk_url, missing_chunk_path)
        bytes_downloaded += filesize(missing_chunk_path)

        # Download any new dictionaries we need for these chunks
        frame = only(list_frames(missing_chunk_path))
        if frame.dictionary_id != 0
            dict_name = zstd_dict_name(frame.dictionary_id)
            dict_path = joinpath(dir, dict_name)
            if !isfile(dict_path)
                Downloads.download("$(server_base_url)/server_chunk_store/$(dict_name)", dict_path)
            end
        end
    end

    synth_time = @elapsed synthesize_wrapper(
        output_file,
        latest_chunks,
        chunk_store_path,
        [old_registry],
    )
    @info("Synthesis complete", synth_time, filesize(output_file))

    @info("Final stats",
        bytes_downloaded,
        download_percentage=@sprintf("%.2f%%", bytes_downloaded*100.0/filesize(output_file)),
        synth_time=@sprintf("%.1fms", synth_time*1000.0),
    )
    
    return (bytes_downloaded=bytes_downloaded, output_file=output_file, synth_time=synth_time)
end

## Select one of these to run a different simulation
# Small delta
#old_registry_hash = registry_hashes[end-1]

# Medium delta
#old_registry_hash = registry_hashes[end-2]

# Large delta
old_registry_hash = registry_hashes[1]

# Simulate the registry update process
result = simulate_registry_update_wrapper(old_registry_hash)
