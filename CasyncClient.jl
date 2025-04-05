module CasyncClient

# This file contains code related to parsing out chunk IDs from our
# compressed-chunk tarball format, as well as resynthesizing a new
# tarball from older ones.

export ChunkId, chunk_path, load_chunk_index, load_seed_chunks, synthesize, zstd_dict_name

include("LibZstd.jl")
using .LibZstd

# We store chunk IDs in skippable frames, identified by this value:
const chunk_id_skippable_frame_magic = UInt32(0x184D2A5E)
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

function load_seed_chunks(seed_file::String)
    compressed_chunks = CompressedChunk[]

    frames = LibZstd.list_frames(seed_file)

    # Look for skippable frames that immediately follow normal
    # zstd frames; these are the ones that contain chunk IDs
    for idx in 2:length(frames)
        function is_chunk_id_frame(idx)
            return isa(frames[idx], LibZstd.ZstdSkippableFrame) &&
                    isa(frames[idx-1], LibZstd.ZstdFrame) &&
                    frames[idx].magic == chunk_id_skippable_frame_magic &&
                    length(frames[idx].data) == chunk_id_hash_len
        end
        if is_chunk_id_frame(idx)
            push!(compressed_chunks, CompressedChunk(
                ChunkId(frames[idx].data),
                frames[idx-1].dictionary_id,
                frames[idx-1].offset,
                frames[idx-1].compressed_len,
            ))
        end
    end
    return compressed_chunks
end


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
        for chunk in chunks
            # Do we have this chunk available in a chunk store?
            local_path = chunk_path(chunk, chunk_store_path)
            if isfile(local_path)
                open(local_path; read=true) do local_io
                    write(io, local_io)
                end
            elseif haskey(seed_map, chunk)
                seed_chunk, seed_chunk_io = seed_map[chunk]
                seek(seed_chunk_io, seed_chunk.offset)
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

            # After writing that zstd frame, write a skippable frame that identifies its hash
            # [skippable magic], [data length], [hash data]
            write(io, UInt32(chunk_id_skippable_frame_magic))
            write(io, UInt32(chunk_id_hash_len))
            write(io, chunk.hash)
        end
    end

    # Close all our seed file handles
    for (_, seed_file_io) in values(seed_map)
        close(seed_file_io)
    end
end

# This is just a convention that must be shared between the server and client
zstd_dict_name(dict_id::UInt32) = "dictionary-$(dict_id).zstdict"

end # module CasyncClient
