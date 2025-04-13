using ZstdSeekable_jll
include("LibZstd.jl")
using .LibZstd
using Random


function zstd_read_func(io_ptr::Ptr{IO}, buff_ptr::Ptr{UInt8}, num_bytes::Csize_t)
    io = unsafe_pointer_to_objref(io_ptr)
    buff = unsafe_wrap(Array, buff_ptr, (num_bytes,))
    try
        read!(io, buff)
        return Cint(1)
    catch e
        @error("zstd_read_func: ", e, buff_ptr, num_bytes)
        return Cint(-1)
    end
end

function zstd_seek_func(io_ptr::Ptr{IO}, offset::Clonglong, origin::Cint)
    io = unsafe_pointer_to_objref(io_ptr)
    if origin == Base.Filesystem.SEEK_SET
        seek(io, offset)
        return Cint(1)
    elseif origin == Base.Filesystem.SEEK_END
        seek(io, filesize(io) + offset)
        return Cint(1)
    else
        @error("zstd_seek_func error!", origin)
        return Cint(-1)
    end
end

struct ZstdSeekableIOFunctions
    io::Ptr{Cvoid}
    read::Ptr{Cvoid}
    seek::Ptr{Cvoid}

    function ZstdSeekableIOFunctions(io::IO)
        return new(
            pointer_from_objref(io),
            @cfunction(zstd_read_func, Cint, (Ptr{IO}, Ptr{UInt8}, Csize_t)),
            @cfunction(zstd_seek_func, Cint, (Ptr{IO}, Clonglong, Cint)),
        )
    end
end


macro check_error(ex)
    return quote
        ret = $(esc(ex))
        if @ccall(libzstd_seekable.ZSTD_isError(ret::Csize_t)::Cuint) != 0
            @error(
                "Zstd error:",
                code=$(string(ex)),
                error_code=ret,
                msg=unsafe_string(@ccall(libzstd_seekable.ZSTD_getErrorName(ret::Csize_t)::Cstring)),
            )
        end
        ret
    end
end

function load_zstd_dict(seekable::Ptr{Cvoid}, dict_path::String)
    # Obtain pointer to `dstream`.  Note; this relies upon the fact that
    # the first element of `seekable` is the `dstream`.  If this is not
    # true, this doesn't work!
    dstream = unsafe_load(convert(Ptr{Ptr{Cvoid}}, seekable), 1)

    # Read in dictionary content tell `dstream` to load from that buffer:
    dict_buff = read(dict_path)
    @check_error @ccall(libzstd_seekable.ZSTD_DCtx_loadDictionary(dstream::Ptr{Cvoid}, dict_buff::Ptr{UInt8}, length(dict_buff)::Csize_t)::Csize_t)
end

mutable struct ZstdSeekableIO <: IO
    seekable::Ptr{Cvoid}
    seekable_funcs::ZstdSeekableIOFunctions
    io::IO

    # read head position within the uncompressed stream
    pos::UInt64

    # Total size of the uncompressed stream
    size::UInt64

    function ZstdSeekableIO(io::IO)
        # Create `seekable` context parameter
        seekable = @ccall(libzstd_seekable.ZSTD_seekable_create()::Ptr{Cvoid})

        # Create our functions struct and tell the `seekable` context about it:
        seekable_funcs = ZstdSeekableIOFunctions(io)
        @check_error @ccall(libzstd_seekable.ZSTD_seekable_initAdvanced(seekable::Ptr{Cvoid}, seekable_funcs::ZstdSeekableIOFunctions)::Csize_t)

        # Parse out the list of frames, use that to get things like total decompressed size
        # and the set of dictionaries needed to be loaded.
        seekstart(io)
        frames = LibZstd.list_frames(io)
        dictionaries = get_dictionary_paths(frames)
        seekstart(io)

        # Figure out total decompressed size:
        uncompressed_len = sum([frame.uncompressed_len for frame in frames if isa(frame, ZstdFrame)])
        
        # Load each dictionary fed to us.
        for dict_path in dictionaries
            load_zstd_dict(seekable, dict_path)
        end

        obj = new(
            seekable,
            seekable_funcs,
            io,
            UInt64(uncompressed_len),
            UInt64(0),
        )
        finalizer(obj) do obj
            @ccall(libzstd_seekable.ZSTD_seekable_free(obj.seekable::Ptr{Cvoid})::Csize_t)
        end
        return obj
    end
end

function get_dictionary_paths(frames::Vector{LibZstd.AbstractZstdHeader}; dictionary_storage_path=joinpath(@__DIR__, "server_chunk_store"))
    dict_ids = Set{UInt32}()
    for frame in frames
        if !isa(frame, LibZstd.ZstdFrame)
            continue
        end

        if frame.dictionary_id != 0
            push!(dict_ids, frame.dictionary_id)
        end
    end

    return joinpath.((dictionary_storage_path,), ["dictionary-$(id).zstdict" for id in dict_ids])
end

function ZstdSeekableIO(file_path::String)
    return ZstdSeekableIO(open(file_path; read=true))
end

Base.filesize(zsio::ZstdSeekableIO) = zsio.size
Base.position(zsio::ZstdSeekableIO) = zsio.pos
function Base.seek(zsio::ZstdSeekableIO, pos)
    zsio.pos = clamp(UInt64(pos), UInt64(0), filesize(zsio))
end
function Base.seekstart(zsio::ZstdSeekableIO)
    zsio.pos = UInt64(0)
end
function Base.seekend(zsio::ZstdSeekableIO)
    zsio.pos = filesize(zsio)
end
Base.skip(zsio::ZstdSeekableIO, offset) = seek(zsio, zsio.pos + offset)

function Base.read!(zsio::ZstdSeekableIO, buff::Vector{UInt8})
    bytes_read = @check_error @ccall(libzstd_seekable.ZSTD_seekable_decompress(zsio.seekable::Ptr{Cvoid}, buff::Ptr{UInt8}, length(buff)::Cint, zsio.pos::Clonglong)::Csize_t)
    zsio.pos += bytes_read
    return buff
end

function Base.read(zsio::ZstdSeekableIO, num_bytes::Integer)
    buff = Vector{UInt8}(undef, num_bytes)
    read!(zsio, buff)
    return buff
end
Base.read(zsio::ZstdSeekableIO) = read(zsio, filesize(zsio))


test_data = "./registries/080dd52fa032049906f0b4cba225cd84e000ddd8.zst"
ground_truth_data = read(`zstd -c -D ./server_chunk_store/dictionary-32800.zstdict -d $(test_data)`)
zsio = ZstdSeekableIO(test_data)

# Randomly sample through the file, reading bits and bobs in (mostly) in-order reads:
timings = Float64[]
for iteration in 1:100
    # Start at a random location within the file
    seek(zsio, round(UInt64, rand()*(filesize(zsio)-102*1000)))

    # Read 100 small chunks, increasing in offset.
    for read_idx in 1:100
        # Skip forward a random amount
        skip(zsio, round(UInt64, rand()*1000))

        # Read ten random bytes
        d_offset = position(zsio)
        t = @elapsed begin
            data = read(zsio, 10)
        end
        push!(timings, t)

        # Ensure that `data` is equal to `ground_truth_data`:
        if data != ground_truth_data[d_offset+1:d_offset+10]
            @error("Data mismatch", iteration, read_idx, d_pos)
            display(data)
            display(ground_truth_data[d_offset+1:d_offset+10])
        end
    end
end

# Inspecting timings, it's predominantly showing timings of ~30us to perform each read.
