module ChunkingEngine

using desync_jll
# Common functionality needs to be imported by the user
using TreeArchival: decompress_cmd
using Zstd_jll

export ChunkSizeSettings, DesyncOptions, index

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

"""
    index(index::String, file::String; dopts::DesyncOptions)

Indexes `file` and stores the index into `index`.  If `file` is a compressed stream,
decompresses it first.  If `dopts.chunk_store` is set, stores the chunks within the
chunk store for serving later.

Returns the list of chunks (requires importing Client.ChunkIndexing separately).
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
    # Return the index path so caller can load it with their own method
    return index
end

end # module ChunkingEngine
