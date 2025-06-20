module ChunkStore

# Common functionality needs to be imported by the user
using Zstd_jll
using ..ChunkingEngine: DesyncOptions

export recompress_chunks, walk_chunks, zstd_dictionary_id, zstd_dict_path

function walk_chunks(f::Function, chunk_store::String)
    for (root, dirs, files) in walkdir(chunk_store)
        for file in files
            if !endswith(file, ".cacnk")
                continue
            end
            f(root, file)
        end
    end
end

function zstd_dict_path(chunk_store::String, dict_id::UInt32)
    dict_name = "dictionary-$(dict_id).zstdict"  # zstd_dict_name function
    return joinpath(chunk_store, dict_name)
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

"""
    recompress_chunks(chunk_store::String, dict_id::UInt32, compression_level::Int)

This function recompresses all chunks within the given chunk store.
It also trains a zstd dictionary on the uncompressed chunks so that
the recompression is maximally effective.
"""
function recompress_chunks(chunk_store::String, dict_id::UInt32=32800, compression_level::Int=22; train_dict::Bool = false, nworkers=Sys.CPU_THREADS, verbose::Bool = false)
    # If we have no dict for the given ID, force training of it.
    dict_path = zstd_dict_path(chunk_store, dict_id)
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
                if train_dict || (zstd_dictionary_id(compressed_path) != dict_id)
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
        walk_chunks(chunk_store) do root, file
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
            run(`$(zstd()) -q --train -T0 --dictID=$(dict_id) -$(compression_level) -r $(uncompressed_root) -o $(dict_path)`)
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
                    run(`$(zstd()) --ultra --no-progress -q -f -$(compression_level) -D $(dict_path) -o $(compressed_path) $(uncompressed_path)`)
                    rm(uncompressed_path; force=true)
                end
                put!(recompressed_size_channel, filesize(compressed_path))
            end
        end
        recompressed_size_worker = Threads.@spawn(size_accumulator(recompressed_size_channel))

        recompression_workers = Task[
            Threads.@spawn(chunk_recompressor()) for _ in 1:nworkers
        ]

        walk_chunks(chunk_store) do root, file
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

# Wrapper function that accepts DesyncOptions
function recompress_chunks(; dopts::DesyncOptions = DesyncOptions(), train_dict::Bool = false, nworkers=Sys.CPU_THREADS, verbose::Bool = false)
    if dopts.chunk_store === nothing
        throw(ArgumentError("chunk_store must be specified in DesyncOptions"))
    end
    return recompress_chunks(dopts.chunk_store, dopts.dict_id, dopts.compression_level; train_dict, nworkers, verbose)
end

end # module ChunkStore
