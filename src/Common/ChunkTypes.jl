module ChunkTypes

# Core data types shared between server and client

export ChunkId, CompressedChunk, chunk_path, zstd_dict_name

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

struct CompressedChunk
    id::ChunkId
    dict_id::UInt32
    offset::UInt64
    len::UInt32
end

# This is just a convention that must be shared between the server and client
zstd_dict_name(dict_id::UInt32) = "dictionary-$(dict_id).zstdict"

end # module ChunkTypes
