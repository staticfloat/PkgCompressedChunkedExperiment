#!/usr/bin/env -S julia --project

include("Desync.jl")
using .Desync
using Downloads

# This script sets up a minimal `server_chunk_store` which would supposedly be served by `Pkg`
# It downloads a couple of registries, chunks them, stores the chunks, then recompresses them
# with a zstd dictionary and maximum compression level.
chunk_store = joinpath(@__DIR__, "server_chunk_store")
rm(chunk_store; force=true, recursive=true)
mkpath(chunk_store)
#dopts = DesyncOptions(;chunk_store, chunk_size=ChunkSizeSettings(2, 4, 8))
dopts = DesyncOptions(;chunk_store)

include("registry_hashes.jl")

# Download a bunch of registries to fill in our Desync backend:
indexes_dir = joinpath(@__DIR__, "indexes")
rm(indexes_dir; force=true, recursive=true)
mkpath(indexes_dir)
for hash in registry_hashes
    reg_file = registry_download_path(hash)
    if !isfile(reg_file)
        download_registry(hash)
        @info("Downloaded", hash)
    end

    index_file = joinpath(indexes_dir, "$(hash).caibx")
    chunks = Desync.index(index_file, reg_file; dopts)
    @info("Chunked Registry", hash, num_chunks=length(chunks))
end

# Save the last one as "latest"
rm(joinpath(indexes_dir, "latest"); force=true)
rm(joinpath(registries_dir, "latest"); force=true)
symlink(string(registry_hashes[end], ".caibx"), joinpath(indexes_dir, "latest"))
symlink(registry_hashes[end], joinpath(registries_dir, "latest"))

# Next, compress the chunk store
Desync.recompress_chunks(;dopts, verbose=true)

# Finally, reconstruct our registries via `synthesize()` so that our
# registry tarballs are in compressed-chunk format.
for hash in registry_hashes
    reg_file = registry_download_path(hash)
    index_file = joinpath(indexes_dir, "$(hash).caibx")
    chunks = Desync.CasyncClient.load_chunk_index(index_file)
    rreg_file = joinpath(registries_dir, "$(hash).zst")
    Desync.CasyncClient.synthesize(rreg_file, chunks, dopts.chunk_store)
    @info("Synthesized registry", hash, filesize(rreg_file))
end
