module Client

# Client-side functionality for downloading chunk indices, identifying missing chunks, 
# downloading them, and reconstituting tarballs

include("../Common/Common.jl")
using .Common

include("ChunkIndexing.jl")
include("ChunkSynthesis.jl")
include("RegistryUpdater.jl")

using .ChunkIndexing
using .ChunkSynthesis
using .RegistryUpdater

# Export the main client-side functionality
export load_chunk_index, load_seed_chunks
export synthesize
export simulate_registry_update, download_missing_chunks

end # module Client
