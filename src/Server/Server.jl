module Server

# Server-side functionality for chunking tarballs, recompressing them, and serving chunk indices

include("../Common/Common.jl")
using .Common

include("ChunkingEngine.jl")
include("ChunkStore.jl")
include("HTTPServer.jl")

using .ChunkingEngine
using .ChunkStore
using .HTTPServer

# Export the main server-side functionality
export DesyncOptions, ChunkSizeSettings
export index, recompress_chunks
export serve_chunk_store
export populate_chunk_store

end # module Server
