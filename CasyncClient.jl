module CasyncClient

# This file contains code related to parsing out chunk IDs from our
# compressed-chunk tarball format, as well as resynthesizing a new
# tarball from older ones.

export ChunkId, chunk_path, load_chunk_index, load_seed_chunks, synthesize, zstd_dict_name

include("LibZstd.jl")
using .LibZstd

# We store chunk IDs in skippable frames, identified by this value:
const chunk_id_table_skippable_frame_magic = UInt32(0x184D2A5D)
const chunk_id_table_skippable_frame_cookie = UInt32(0xD12FA2A3)
const zstd_seekable_skippable_frame_magic = UInt32(0x184D2A5E)
const zstd_seekable_skippable_frame_cookie = UInt32(0x8F92EAB1)
const chunk_id_hash_len = 32

"""
    ChunkId

Represents a content chunk by its hash.  Can be used as a key to a content store
via `chunk_path()` to get the actual content referred to by this chunk ID.
"""
struct ChunkId
    hash::Vector{UInt8}
end

Base.:(==)(a::ChunkId, b::ChunkId) = a.hash == b.hash
Base.hash(c::ChunkId) = Base.hash(c.hash)

function ChunkId(hash::AbstractString)
    # This assumes we always use the default of SHA512-256
    if length(hash) != 2*chunk_id_hash_len
        throw(ArgumentError("Invalid hash length '$(length(hash))', must be exactly 64 hexadecimal characters!"))
    end
    return ChunkId(
        hex2bytes(hash),
    )
end

Base.show(io::IO, c::ChunkId) = print(io, "[$(bytes2hex(c.hash[1:4]))]")
Base.string(c::ChunkId) = bytes2hex(c.hash)

"""
    chunk_path(c::ChunkId, chunk_store_path::String)

Returns a chunk store path conforming to the standard set by tools such as
`casync` and `desync`.  Looks like `\$(chunk_store_path)/1a2b/1a2b3c4d.cacnk`
"""
function chunk_path(c::ChunkId, chunk_store_path::String)
    chunk_string = string(c)
    return joinpath(chunk_store_path, chunk_string[1:4], string(chunk_string, ".cacnk"))
end 


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


struct CompressedChunk
    id::ChunkId
    dict_id::UInt32
    offset::UInt64
    len::UInt32
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
    frames = LibZstd.list_frames(io)
    zstd_frames = filter(frame -> isa(frame, LibZstd.ZstdFrame), frames)

    function get_chunk_ids(frames, num_chunks)
        # The chunk ID table is usually stored at the end of the list of frames:
        for idx in length(frames):-1:1
            if !isa(frames[idx], LibZstd.ZstdSkippableFrame)
                continue
            end
            if frames[idx].magic != chunk_id_table_skippable_frame_magic
                continue
            end
            if length(frames[idx].data) != num_chunks*chunk_id_hash_len + sizeof(UInt32)
                continue
            end
            if reinterpret(UInt32, frames[idx].data[end-3:end])[1] != chunk_id_table_skippable_frame_cookie
                continue
            end

            # If we've made it through the gauntlet, then `frames[idx].data` contains our Chunk ID table
            return reshape(frames[idx].data[1:end-4], (chunk_id_hash_len, num_chunks))
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
load_seed_chunks(file::String) = open(load_seed_chunks, file; read=true)

function synthesize(output_path::String, chunks::Vector{ChunkId}, chunk_store_path::String, seed_files::Vector{String} = String[])
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
        chunk_id_table_frame_len = length(chunks) * chunk_id_hash_len + sizeof(UInt32)
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

# This is just a convention that must be shared between the server and client
zstd_dict_name(dict_id::UInt32) = "dictionary-$(dict_id).zstdict"

end # module CasyncClient
