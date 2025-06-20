module ChunkSynthesis

# Import the types and functions we need from Common
using ..Common: ChunkId, CompressedChunk, chunk_path, list_frames
using ..ChunkIndexing: load_seed_chunks

export synthesize

# Constants for chunk ID table and seekable table
const chunk_id_table_skippable_frame_magic = UInt32(0x184D2A5D)
const chunk_id_table_skippable_frame_cookie = UInt32(0xD12FA2A3)
const zstd_seekable_skippable_frame_magic = UInt32(0x184D2A5E)
const zstd_seekable_skippable_frame_cookie = UInt32(0x8F92EAB1)

function synthesize(output_path::String, chunks, chunk_store_path::String, seed_files::Vector{String} = String[])
    # For each seed file, see if there's a compressed chunk index alongside it
    seed_map = Dict{ChunkId,Tuple{CompressedChunk,IO}}()
    for seed_file in seed_files
        # Open a file handle on this seed file, we'll close it at the end.
        seed_file_io = open(seed_file, read=true)

        for cchunk in load_seed_chunks(seed_file)
            seed_map[cchunk.id] = (cchunk, seed_file_io)
        end
    end

    # Start to assemble our output file
    rm(output_path; force=true)
    open(output_path; write=true) do io
        # Store information for each chunk; we want to know (compressed_len, uncompressed_len) for each.
        seekable_table = Tuple{UInt32,UInt32}[]

        function peek_zstd_frame(io::IO)
            mark(io)
            frames = list_frames(io)
            if isempty(frames)
                error("Cannot find zstd frame header in $(io) $(position(io))")
            end
            frame = first(frames)
            reset(io)
            return frame
        end

        function store_seekable_table(io::IO)
            frame = peek_zstd_frame(io)
            push!(seekable_table, (frame.compressed_len, frame.uncompressed_len))
        end

        for chunk in chunks
            # Do we have this chunk available in a chunk store?
            local_path = chunk_path(chunk, chunk_store_path)
            if isfile(local_path)
                open(local_path; read=true) do local_io
                    store_seekable_table(local_io)
                    write(io, local_io)
                end
            elseif haskey(seed_map, chunk)
                seed_chunk, seed_chunk_io = seed_map[chunk]
                seek(seed_chunk_io, seed_chunk.offset)

                # Store compressed_len/uncompressed_len for the seekable table
                store_seekable_table(seed_chunk_io)

                left_to_write = seed_chunk.len
                while left_to_write > 0
                    data = read(seed_chunk_io, left_to_write)
                    write(io, data)
                    left_to_write -= length(data)

                    if eof(seed_chunk_io)
                        @error("Unable to read chunk", seed_chunk, seed_chunk_io)
                        error()
                    end
                end
            else
                @error("Missing chunk", chunk)
                error()
            end
        end

        # Write out our Chunk ID table
        # [magic], [payload length], [hash, ...], [cookie]
        write(io, UInt32(chunk_id_table_skippable_frame_magic))
        chunk_id_table_frame_len = length(chunks) * 32 + sizeof(UInt32)  # chunk_id_hash_len = 32
        write(io, UInt32(chunk_id_table_frame_len))
        for chunk in chunks
            write(io, chunk.hash)
        end
        write(io, chunk_id_table_skippable_frame_cookie)

        # Write out the seekable table format:
        # [magic], [payload length], [(compressed_len, uncompressed_len), ...], [num_frames], [0x00], [cookie]
        seekable_frame_len = length(seekable_table) * 2*sizeof(UInt32) + 2*sizeof(UInt32) + sizeof(UInt8)
        write(io, UInt32(zstd_seekable_skippable_frame_magic))
        write(io, UInt32(seekable_frame_len))
        for (compressed_len, uncompressed_len) in seekable_table
            # Write out data for the ZstdFrame
            write(io, UInt32(compressed_len))
            write(io, UInt32(uncompressed_len))
        end
        write(io, UInt32(length(seekable_table)))
        write(io, UInt8(0x00))
        write(io, UInt32(zstd_seekable_skippable_frame_cookie))
    end

    # Close all our seed file handles
    for (_, seed_file_io) in values(seed_map)
        close(seed_file_io)
    end
end

end # module ChunkSynthesis
