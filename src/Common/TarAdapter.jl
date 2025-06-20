module TarAdapter

using Tar
export TarFilesystem, TarFileIO

# This is basically a `Tar.Header`, but we add an offset
struct SeekHeaderEntry
    path::String
    type::Symbol
    mode::UInt16
    size::Int64
    link::String
    offset::UInt64
end

# This is the object that needs to store all information necessary
# to perform file operations upon it such as `open()`, `read()`,
# `stat()`, etc....
struct TarFilesystem
    io::IO
    seek_map::Dict{String,SeekHeaderEntry}
    dir_map::Dict{String,Vector{String}}
end

mutable struct TarFileIO <: IO
    pos::Int64
    entry::SeekHeaderEntry
    tf::TarFilesystem
end

function Base.show(io::IO, tio::TarFileIO)
    print(io, "TarFileIO(<$(tio.entry.type) $(tio.entry.path)>)")
end

# Make `TarFileIO` act like an IO
Base.filesize(tio::TarFileIO) = tio.entry.size
Base.position(tio::TarFileIO) = tio.pos
Base.filesize(tio::TarFileIO) = tio.entry.size
function Base.seek(tio::TarFileIO, n)
    tio.pos = clamp(n, 0, filesize(tio))
end
Base.seekstart(tio::TarFileIO) = tio.pos = 0
Base.seekend(tio::TarFileIO) = tio.pos = filesize(tio)
Base.eof(tio::TarFileIO) = tio.pos >= filesize(tio)
Base.skip(tio::TarFileIO, offset) = seek(tio, tio.pos + offset)

function resolve_links(tio::TarFileIO)
    while islink(tio)
        new_path = normpath(dirname(tio.entry.path), tio.entry.link)
        if !haskey(tio.tf.seek_map, new_path)
            throw(Base._UVError(new_path, -Base.Libc.ENOENT))
        end
        tio = tio.tf[new_path]
    end
    return tio
end

function Base.readlink(tio::TarFileIO)
    if !islink(tio)
        throw(Base._UVError(tio.entry.path, -Base.Libc.EINVAL))
    end
    return tio.entry.link
end
function Base.realpath(tio::TarFileIO)
    return resolve_links(tio).entry.path
end

# To read, we seek to the start of our entry, plus our current position within it.
function Base.unsafe_read(tio::TarFileIO, p::Ptr{UInt8}, n::UInt)
    # Don't let us read past the end of the file
    if n > filesize(tio) - position(tio)
        throw(EOFError())
    end

    seek(tio.tf.io, tio.entry.offset + tio.pos)
    unsafe_read(tio.tf.io, p, n)
    return nothing
end

function Base.read(tio::TarFileIO)
    x = Vector{UInt8}(undef, filesize(tio))
    seekstart(tio)
    Base.unsafe_read(tio, pointer(x), filesize(tio))
    return x
end

# Construct seek table for tarball
function TarFilesystem(tar::IO)
    seek_map = Dict{String,SeekHeaderEntry}()
    dir_map = Dict{String,Vector{String}}()
    function record_path(hdr)
        seek_map[hdr.path] = SeekHeaderEntry(
            hdr.path,
            hdr.type,
            hdr.mode,
            hdr.size,
            hdr.link,
            position(tar),
        )
        path_dirname = dirname(hdr.path)
        push!(get!(dir_map, path_dirname, String[]), hdr.path)
        return false
    end
    Tar.read_tarball(x -> nothing, record_path, tar)
    return TarFilesystem(tar, seek_map, dir_map)
end
TarFilesystem(tarfilesystem::String) = TarFilesystem(open(tarfilesystem; read=true))

# Convenient dict lookup
function Base.getindex(tf::TarFilesystem, path::String)
    return TarFileIO(0, tf.seek_map[path], tf)
end

# open() returns a `TarFileIO`
function Base.open(f::Function, tf::TarFilesystem, path::String;
                   write::Bool = false, create::Bool = false,
                   truncate::Bool = false, append::Bool = false,
                   lock::Bool = false, read::Bool = true)
    if write || create || truncate || append || lock
        throw(ArgumentError("The only valid file mode for a `TarFileIO` is `read = true`."))
    end
    # I'm not going to bother freaking out if `read=false`.
    return f(resolve_links(tf[path]))
end
function Base.open(tf::TarFilesystem, path::String; kwargs...)
    return open(identity, tf, path; kwargs...)
end

function Base.StatStruct(ent::SeekHeaderEntry)
    function typemode(ent_type::Symbol)
        if ent_type == :file || ent_type == :hardlink
            return 0x8000
        elseif ent_type == :directory
            return 0x4000
        elseif ent_type == :symlink
            return 0xa000
        else
            @warn("Unknown entity type :$(ent_type)", path)
            return 0x0000
        end
    end
    return Base.StatStruct(
        ent.path,
        0,
        0,
        typemode(ent.type) | ent.mode,
        0,
        Base.Libc.getuid(),
        Base.Libc.getuid(),
        0,
        ent.size,
        0,
        0,
        0.0,
        0.0,
    )
end

function Base.stat(tf::TarFilesystem, file::String)
    # Special handling; `stat()` of a non-existent file just returns an empty stat struct
    if file ∉ keys(tf.seek_map)
        return Base.StatStruct()
    end
    return Base.StatStruct(resolve_links(tf.seek_map[file]))
end
function Base.lstat(tf::TarFilesystem, file::String)
    # Special handling; `lstat()` of a non-existent file just returns an empty stat struct
    if file ∉ keys(tf.seek_map)
        return Base.StatStruct()
    end
    return Base.StatStruct(tf.seek_map[file])
end
Base.stat(tio::TarFileIO) = Base.StatStruct(resolve_links(tio).entry)
Base.lstat(tio::TarFileIO) = Base.StatStruct(tio.entry)

function Base.readdir(tf::TarFilesystem, path::String)
    entry = tf[path]
    if !isdir(entry)
        throw(Base._UVError(path, -Base.Libc.ENOTDIR))
    end
    return tf.dir_map[path]
end
Base.readdir(tf::TarFilesystem) = tf.dir_map[""]

Base.read(tf::TarFilesystem, path::String) = read(resolve_links(tf[path]))

end # module TarAdapter
