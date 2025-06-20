module ChunkIndexing

# Import the types and functions we need from Common
using ..Common: ChunkId, CompressedChunk, ZstdFrame, ZstdSkippableFrame, list_frames

export load_chunk_index, load_seed_chunks

# We store chunk IDs in skippable frames, identified by this value:
const chunk_id_table_skippable_frame_magic = UInt32(0x184D2A5D)
const chunk_id_table_skippable_frame_cookie = UInt32(0xD12FA2A3)
const zstd_seekable_skippable_frame_magic = UInt32(0x184D2A5E)
const zstd_seekable_skippable_frame_cookie = UInt32(0x8F92EAB1)

"""
    load_chunk_index(index_path::String)

Given a `.caibx` file from `casync` or `desync`, parse its format and return
the list of chunks that will reconstitute the file.
"""
function load_chunk_index(index_path::String)
    CaFormatIndex = 0x96824d9c7b129ff9
    CaFormatTable = 0xe75b9e112f17417d

    chunks = ChunkId[]
    open(index_path; read=true) do io
        # First, ensure that this is a `CaFormatIndex`:
        payload_size = read(io, UInt64)
        header_type = read(io, UInt64)
        if payload_size != 48 || header_type != CaFormatIndex
            throw(ArgumentError("File '$(index_path)' is not a valid index; it has an incorrect header!"))
        end

        # Skip the rest of the header
        seek(io, payload_size)

        # Read the next header, which should be a `CaFormatTable`:
        payload_size = read(io, UInt64)
        header_type = read(io, UInt64)
        if payload_size != typemax(UInt64) || header_type != CaFormatTable
            throw(ArgumentError("File '$(index_path)' is not a valid index; it does not have a table where we expected!"))
        end

        while !eof(io)
            offset = read(io, UInt64)
            if offset == 0
                break
            end
            chunk_id = read(io, 32)
            push!(chunks, ChunkId(chunk_id))
        end
    end
    return chunks
end

"""
    load_seed_chunks(io::IO)

Scan a compressed archive for content-defined chunks embedded within it, which
we refer to as "seed" chunks, which will be used to synthesize a new compressed
archive with.

The seed chunks are identified by their content hash, however to avoid needing
to decompress and hash each chunk, we store a skippable metadata frame at the
end of our compressed archives, identifying the content hash of each chunk.
This method extracts that metadata.
"""
function load_seed_chunks(io::IO)
    # Get the list of frames from the archive, and remove all the skippable ones
    # to get the chunks of actual content, which we will then pair up with their
    # chunk IDs from a skippable frame:
    frames = list_frames(io)
    zstd_frames = filter(frame -> isa(frame, ZstdFrame), frames)

    function get_chunk_ids(frames, num_chunks)
        # The chunk ID table is usually stored at the end of the list of frames:
        for idx in length(frames):-1:1
            if !isa(frames[idx], ZstdSkippableFrame)
                continue
            end
            if frames[idx].magic != chunk_id_table_skippable_frame_magic
                continue
            end
            if length(frames[idx].data) != num_chunks*32 + sizeof(UInt32)  # chunk_id_hash_len = 32
                continue
            end
            if reinterpret(UInt32, frames[idx].data[end-3:end])[1] != chunk_id_table_skippable_frame_cookie
                continue
            end

            # If we've made it through the gauntlet, then `frames[idx].data` contains our Chunk ID table
            return reshape(frames[idx].data[1:end-4], (32, num_chunks))  # chunk_id_hash_len = 32
        end
        return nothing
    end

    # List of chunk IDs
    chunk_id_matrix = get_chunk_ids(frames, length(zstd_frames))

    # Construct a `CompressedChunk` for each zstd frame.
    return CompressedChunk[
        CompressedChunk(
            ChunkId(chunk_id_matrix[:, idx]),
            zstd_frames[idx].dictionary_id,
            zstd_frames[idx].offset,
            zstd_frames[idx].compressed_len,
        ) for idx in 1:length(zstd_frames)
    ]
end
load_seed_chunks(file::String) = 
    open(io -> load_seed_chunks(io), file; read=true)

end # module ChunkIndexing
