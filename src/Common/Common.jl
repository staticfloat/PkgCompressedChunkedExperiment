module Common

# Re-export all the common functionality
include("ChunkTypes.jl")
include("LibZstd.jl")
include("LibZstdSeekable.jl")
include("TarAdapter.jl")

using .ChunkTypes
using .LibZstd
using .LibZstdSeekable
using .TarAdapter

# Export the main types and functions
export ChunkId, CompressedChunk, chunk_path, zstd_dict_name
export ZstdFrame, ZstdSkippableFrame, list_frames
export ZstdSeekableIO
export TarFilesystem, TarFileIO

end # module Common
