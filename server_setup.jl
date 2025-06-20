#!/usr/bin/env -S julia --project

# Simplified server setup script

# Import all the necessary modules
include("src/Common/Common.jl")
include("test/RegistryUtils.jl")
using .Common
using .RegistryUtils

# Import server components
include("src/Server/ChunkingEngine.jl")
include("src/Server/ChunkStore.jl")
using .ChunkingEngine
using .ChunkStore

# Import client components needed for some server operations
include("src/Client/ChunkIndexing.jl")
include("src/Client/ChunkSynthesis.jl")
using .ChunkIndexing
using .ChunkSynthesis

# Set up the chunk store
chunk_store = joinpath(@__DIR__, "server_chunk_store")
dopts = DesyncOptions(;chunk_store)

@info("Setting up server chunk store...")

# Clean and create chunk store directory
rm(dopts.chunk_store; force=true, recursive=true)
mkpath(dopts.chunk_store)

# Create indexes directory
indexes_dir = joinpath(dirname(dopts.chunk_store), "indexes")
rm(indexes_dir; force=true, recursive=true)
mkpath(indexes_dir)

# Download and chunk all registries
for hash in registry_hashes
    reg_file = registry_download_path(hash)
    if !isfile(reg_file)
        download_registry(hash)
        @info("Downloaded", hash)
    end

    index_file = joinpath(indexes_dir, "$(hash).caibx")
    index_path = index(index_file, reg_file; dopts)
    @info("Chunked Registry", hash, index_file=index_path)
end

# Save the last one as "latest"
rm(joinpath(indexes_dir, "latest"); force=true)
rm(joinpath(registries_dir, "latest"); force=true)
symlink(string(registry_hashes[end], ".caibx"), joinpath(indexes_dir, "latest"))
symlink(registry_hashes[end], joinpath(registries_dir, "latest"))

# Recompress the chunk store
recompress_chunks(dopts.chunk_store, dopts.dict_id, dopts.compression_level; verbose=true)

# Create wrapper functions that work with the new module structure
function load_chunk_index_wrapper(index_path::String)
    return load_chunk_index(index_path)
end

function load_seed_chunks_wrapper(file::String)
    return load_seed_chunks(file)
end

function synthesize_wrapper(output_path::String, chunks, chunk_store_path::String, seed_files::Vector{String} = String[])
    return synthesize(output_path, chunks, chunk_store_path, seed_files)
end

# Finally, reconstruct our registries via `synthesize()` so that our
# registry tarballs are in compressed-chunk format.
for hash in registry_hashes
    reg_file = registry_download_path(hash)
    index_file = joinpath(indexes_dir, "$(hash).caibx")
    chunks = load_chunk_index_wrapper(index_file)
    rreg_file = joinpath(registries_dir, "$(hash).zst")
    synthesize_wrapper(rreg_file, chunks, dopts.chunk_store)
    @info("Synthesized registry", hash, filesize(rreg_file))
end

@info("Server setup complete!")
