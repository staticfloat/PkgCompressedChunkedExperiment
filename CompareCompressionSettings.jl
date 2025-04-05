#!/usr/bin/env -S julia --project

include("Desync.jl")
using .Desync
using .Desync: CasyncClient
using Zstd_jll

include("registry_hashes.jl")

# We want to see (a) how much transfer we'd need for a delta update in each case,
# and (b) how much disk space the final, assembled tarball requires.

# To test out 
old_targz_path = joinpath(registries_dir, registry_hashes[end-2])
new_targz_path = joinpath(registries_dir, registry_hashes[end])


# Get an uncompressed version of the registry ready
dir = mktempdir()
old_uncompressed_path = joinpath(dir, "General-old.tar")
new_uncompressed_path = joinpath(dir, "General-new.tar")
run(`$(zstd()) -q -d -f $(old_targz_path) -o $(old_uncompressed_path)`)
run(`$(zstd()) -q -d -f $(new_targz_path) -o $(new_uncompressed_path)`)
new_uncompressed_size = filesize(new_uncompressed_path)

stats_file = nothing
function report_stats(name::String, compressed_file::String; data_transfer = filesize(compressed_file))
    global stats_file
    if stats_file === nothing
        stats_file = open(joinpath(@__DIR__, "compression_stats.csv"); write=true)
        println(stats_file, "Scenario,size_on_disk,data_transfer,transfer_portion,transfer_ratio,compression_ratio")
    end
    size_on_disk = filesize(compressed_file)
    transfer_portion = data_transfer/size_on_disk
    transfer_ratio = data_transfer/filesize(new_targz_path)
    compression_ratio = size_on_disk/new_uncompressed_size
    @info(
        name,
        # How much space does this take on the user's disk?
        size_on_disk,
        # How many bytes did we have to transfer?
        data_transfer,
        # How much of the full file did we have to transfer? (only non-1.0 for partial transfer techniques)
        transfer_portion,
        # How much less transfer is this than Scenario 0?
        transfer_ratio,
        # How much did compression help?
        compression_ratio,
        # How much better is this compression than our current zlib?
        compression_improvement_ratio = compression_ratio/(filesize(new_targz_path)/new_uncompressed_size),
    )
    println(stats_file, join(string.([name, size_on_disk, data_transfer, transfer_portion, transfer_ratio, compression_ratio]), ","))
    flush(stats_file)
end

println()
# Scenario 0: What we're currently doing
report_stats(
    "Scenario 0: ship .tar.gz",
    new_targz_path,
)

println()
# Scenario 1: Just zstd it and ship that
function zstd_compress(in_path::String, out_path::String, compression_level::Int)
    run(`$(zstd()) -T0 -c -f --ultra -q -$(compression_level) $(in_path) -o $(out_path)`)
end
compression_levels = (3, 9, 22)
for (idx, compression_level) in enumerate(compression_levels)
    zstd_path = joinpath(dir, "General.tar.zst-$(compression_level)")
    zstd_compress(new_uncompressed_path, zstd_path, compression_level)
    report_stats(
        "Scenario 1.$(idx): ship .tar.zst (compression level $(compression_level))",
        zstd_path,
    )
end

println()
# Scenario 2: Chunk the uncompressed tarball, zstd-compress each piece,
# then re-assemble the tarball, and download that.
indexes_dir = joinpath(dir, "indexes"); mkpath(indexes_dir)
chunk_sizes = [
    "small" => ChunkSizeSettings(2, 4, 8),
    "medium" => ChunkSizeSettings(4, 8, 16),
    "large" => ChunkSizeSettings(8, 64, 256), # default casync/desync size
]
for (idx, (chunk_size_name, chunk_size)) in enumerate(chunk_sizes)
    chunk_store = joinpath(dir, "chunk_store-$(chunk_size_name)"); mkpath(chunk_store)
    dopts = DesyncOptions(;chunk_size, chunk_store)
    for (name, reg_file) in ("old" => old_targz_path, "new" => new_targz_path)
        index_file = joinpath(indexes_dir, "$(name)-$(chunk_size_name).caibx")
        Desync.index(index_file, reg_file; dopts)
    end
    Desync.recompress_indices(; dopts)

    # Reconstruct tarballs using these recompressed indices
    for name in ("old", "new")
        rreg_path = joinpath(dir, "General-chunked-$(chunk_size_name)_$(name).tar.zst")
        index_file = joinpath(indexes_dir, "$(name)-$(chunk_size_name).caibx")
        chunks = CasyncClient.load_chunk_index(index_file)
        CasyncClient.synthesize(rreg_path, chunks, chunk_store)
    end

    # For this scenario, we're only looking at what it costs to download
    # the new `.tar.zst` directly.
    rreg_path = joinpath(dir, "General-chunked-$(chunk_size_name)_new.tar.zst")
    report_stats(
        "Scenario 2.$(idx): ship resynthesized .tar.zst (chunksize $(chunk_size_name))",
        rreg_path,
    )
end


println()
# Scenario 3: Only transfer the chunks we need, resynthesizing from old registry
for (idx, (chunk_size_name, chunk_size)) in enumerate(chunk_sizes)
    chunk_store = joinpath(dir, "chunk_store-$(chunk_size_name)")
    output_file = joinpath(dir, "General-resynthesized-$(chunk_size_name)_new.tar.zst")
    old_reg_path = joinpath(dir, "General-chunked-$(chunk_size_name)_old.tar.zst")
    index_file = joinpath(indexes_dir, "new-$(chunk_size_name).caibx")
    new_chunks = CasyncClient.load_chunk_index(index_file)
    old_chunks = CasyncClient.load_seed_chunks(old_reg_path)
    missing_chunks = setdiff(Set(new_chunks), Set([c.id for c in old_chunks]))

    # Reconstruct 
    CasyncClient.synthesize(
        output_file,
        new_chunks,
        chunk_store,
        [old_reg_path],
    )

    report_stats(
        "Scenario 3.$(idx): ship only new chunks (chunksize $(chunk_size_name))",
        output_file;
        data_transfer = sum([filesize(CasyncClient.chunk_path(c, chunk_store)) for c in missing_chunks])
    )
end
