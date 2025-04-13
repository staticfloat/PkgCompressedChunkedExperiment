module Desync
using desync_jll
using Downloads

include("CasyncClient.jl")
using .CasyncClient

export ChunkSizeSettings
struct ChunkSizeSettings
    min_kb::Int
    avg_kb::Int
    max_kb::Int

    function ChunkSizeSettings(min_kb, avg_kb, max_kb)
        min_kb = Int(min_kb)
        avg_kb = Int(avg_kb)
        max_kb = Int(max_kb)

        if !(min_kb <= avg_kb && avg_kb <= max_kb)
            throw(ArgumentError("Chunk size settings must follow strict ordering (min <= avg <= max)"))
        end
        return new(min_kb, avg_kb, max_kb)
    end
end

# Default values
ChunkSizeSettings() = ChunkSizeSettings(16, 64, 256)

function desync_flags!(flags::Vector{String}, css::ChunkSizeSettings)
    push!(flags, "-m")
    push!(flags, "$(css.min_kb):$(css.avg_kb):$(css.max_kb)")
end

export DesyncOptions
Base.@kwdef struct DesyncOptions
    verbose::Bool = false
    chunk_size::ChunkSizeSettings = ChunkSizeSettings()
    chunk_store::Union{Nothing,String} = nothing

    # Set an explicit dictID to save two bytes, as otherwise it'll be a four-byte dictionary ID.
    # We should bump this number every time we build a new dictionary, so that Pkg can know
    # to download it.
    dict_id::UInt32 = 32800
    compression_level::Int = 22
end

function desync_jll.desync(dopts::DesyncOptions, verb::String)
    cmd = desync()

    if dopts.verbose
        push!(cmd.exec, "--verbose")
    end

    # The verb must come next, then our common options
    push!(cmd.exec, verb)

    # Add flags for chunk size
    desync_flags!(cmd.exec, dopts.chunk_size)

    # Add flags for chunk store
    if dopts.chunk_store !== nothing && verb ∈ ("make",)
        push!(cmd.exec, "-s")
        push!(cmd.exec, dopts.chunk_store)
    end

    return cmd
end

CasyncClient.chunk_path(c::ChunkId, dopts::DesyncOptions) = chunk_path(c, dopts.chunk_store)


using TreeArchival: decompress_cmd
"""
    index(index::String, file::String; dopts::DesyncOptions)

Indexes `file` and stores the index into `index`.  If `file` is a compressed stream,
decompresses it first.  If `dopts.chunk_store` is set, stores the chunks within the
chunk store for serving later.
"""
function index(index::String, file::String; dopts::DesyncOptions = DesyncOptions())
    # Unfortunately, `desync make` doesn't support reading from `stdin`, so we have
    # to actually write out to a file in order to index it:
    mktempdir() do dir
        decompressed_file = joinpath(dir, "decompressed")
        run(`$(zstd()) -q -d $(file) -o $(decompressed_file)`)

        io = IOBuffer()
        if !success(pipeline(`$(desync(dopts, "make")) $(index) $(decompressed_file)`, stderr=io))
            error("indexing failed: $(String(take!(io)))")
        end
    end
    return load_chunk_index(index)
end

function walk_chunks(f::Function; dopts::DesyncOptions = DesyncOptions())
    if dopts.chunk_store === nothing
        return
    end

    for (root, dirs, files) in walkdir(dopts.chunk_store)
        for file in files
            if !endswith(file, ".cacnk")
                continue
            end
            f(root, file)
        end
    end
end

zstd_dict_name(dict_id::UInt32) = "dictionary-$(dict_id).zstdict"
function zstd_dict_path(;dopts::DesyncOptions = DesyncOptions(), dict_id::UInt32 = dopts.dict_id)
    if dopts.chunk_store === nothing
        return nothing
    end
    return joinpath(dopts.chunk_store, zstd_dict_name(dict_id))
end

# X-ref: https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#frames
function zstd_dictionary_id(path::String)
    open(path, read=true) do io
        magic = read(io, UInt32)
        if magic != 0xFD2FB528
            # Not a valid zstd file
            return UInt32(0)
        end

        # Check that a dictionary was actually used, if not, return `0`, which means "no dictionary"
        frame_header_byte = read(io, UInt8)
        dictionary_id_flag = (frame_header_byte & 0x03)
        if dictionary_id_flag == 0
            return UInt32(0)
        end

        # Check whether there's a window descriptor byte, if so, skip it
        if (frame_header_byte & 0x10) == 0
            read(io, UInt8)
        end

        # Next, read as many bytes as `dictionary_id_flag` specifies:
        dictionary_id = UInt32(0)
        for idx in 0:(dictionary_id_flag-1)
            val = UInt32(read(io, UInt8))
            dictionary_id |= (UInt32(val) << (8*idx))
        end
        return dictionary_id
    end
end

macro try_take!(c)
    return quote
        try
            take!($(esc(c)))
        catch e
            if isa(e, InvalidStateException) && e.state == :closed
                continue
            end
            rethrow(e)
        end
    end
end

using Zstd_jll
"""
    recompress_chunks()

This function recompresses all chunks within the given chunk store.
It also trains a zstd dictionary on the uncompressed chunks so that
the recompression is maximally effective.
"""
function recompress_chunks(;dopts::DesyncOptions = DesyncOptions(), train_dict::Bool = false, nworkers=Sys.CPU_THREADS, verbose::Bool = false)
    if dopts.chunk_store === nothing
        return
    end

    # If we have no dict for the given ID, force training of it.
    dict_path = zstd_dict_path(;dopts)
    if !isfile(dict_path)
        train_dict = true
    end

    # We uncompress all `.cacnk` files into an "uncompressed" directory, then train a
    # zstd dictionary on those files.
    mktempdir() do uncompressed_root
        if verbose
            @info("Decompressing chunk store...", nworkers)
        end

        decompression_channel = Channel{String}(2*nworkers)
        original_size_channel = Channel{Int}(2*nworkers)
        uncompressed_size_channel = Channel{Int}(2*nworkers)

        # Uncompress all `.cacnk` files, store them in `.raw` files:
        function chunk_decompressor()
            while isopen(decompression_channel)
                compressed_path = @try_take!(decompression_channel)
                # Only uncompress if we're training a dictionary (so need all uncompressed)
                # or if the compressed file was compressed with a different dictionary.
                if train_dict || (zstd_dictionary_id(compressed_path) != dopts.dict_id)
                    uncompressed_path = joinpath(uncompressed_root, string(basename(compressed_path), ".raw"))
                    run(`$(zstd()) -f -q -d $(compressed_path) -o $(uncompressed_path)`)
                end
                put!(original_size_channel, filesize(compressed_path))
                put!(uncompressed_size_channel, filesize(uncompressed_path))
            end
        end

        # Take in sizes emitted by each decompression worker, accumulate and return them
        function size_accumulator(c::Channel)
            size = 0
            num_elements = 0
            while isopen(c)
                size += @try_take!(c)
                num_elements += 1
            end
            return size, num_elements
        end
        decompression_workers = Task[
            Threads.@spawn(chunk_decompressor()) for _ in 1:nworkers
        ]
        original_size_worker = Threads.@spawn(size_accumulator(original_size_channel))
        uncompressed_size_worker = Threads.@spawn(size_accumulator(uncompressed_size_channel))

        # Launch all of our decompressing workers
        walk_chunks(;dopts) do root, file
            put!(decompression_channel, joinpath(root, file))
        end
        close(decompression_channel)

        # Wait for them all to finish, then close the size collecting channel
        wait.(decompression_workers)
        close(original_size_channel)
        original_size, _ = fetch(original_size_worker)
        close(uncompressed_size_channel)
        uncompressed_size, _ = fetch(uncompressed_size_worker)

        # Train the dictionary
        if train_dict
            if verbose
                @info("Training new zstd dictionary...")
            end
            rm(dict_path; force=true)
            run(`$(zstd()) -q --train -T0 --dictID=$(dopts.dict_id) -$(dopts.compression_level) -r $(uncompressed_root) -o $(dict_path)`)
        end

        # Recompress everything using the dictionary, as long as the original
        # wasn't compressed with that dictionary to begin with
        recompression_channel = Channel{String}(2*nworkers)
        recompressed_size_channel = Channel{Int}(200000*nworkers)
        num_compressed = 0
        if verbose
            @info("Recompressing chunk store...")
        end
        function chunk_recompressor()
            while isopen(recompression_channel)
                compressed_path = @try_take!(recompression_channel)
                uncompressed_path = joinpath(uncompressed_root, string(basename(compressed_path), ".raw"))
                # Sometimes we don't decompress every chunk (e.g. if they were already
                # compressed with the right dictionary, and we're not rebuilding the dictionary)
                # so just silently skip those that don't exist.
                if isfile(uncompressed_path)
                    run(`$(zstd()) --ultra --no-progress -q -f -$(dopts.compression_level) -D $(dict_path) -o $(compressed_path) $(uncompressed_path)`)
                    rm(uncompressed_path; force=true)
                end
                put!(recompressed_size_channel, filesize(compressed_path))
            end
        end
        recompressed_size_worker = Threads.@spawn(size_accumulator(recompressed_size_channel))

        recompression_workers = Task[
            Threads.@spawn(chunk_recompressor()) for _ in 1:nworkers
        ]

        walk_chunks(;dopts) do root, file
            put!(recompression_channel, joinpath(root, file))
        end
        close(recompression_channel)

        # Wait for them all to finish, then close the size collecting channel
        wait.(recompression_workers)
        close(recompressed_size_channel)
        recompressed_size, num_compressed = fetch(recompressed_size_worker)

        if verbose
            @info("Compressed $(num_compressed) chunks",
                original_size,
                recompressed_size,
                uncompressed_size,
                avg_ratio=recompressed_size*1.0/uncompressed_size,
                improvement_ratio=recompressed_size*1.0/original_size,
            )
        end
    end
end

end # module Desync
