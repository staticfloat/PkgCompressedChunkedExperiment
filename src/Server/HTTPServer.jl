module HTTPServer

using LiveServer

export serve_chunk_store

"""
    serve_chunk_store(;host="127.0.0.1", port=8000, dir=@__DIR__)

Start an HTTP server to serve the chunk store and related files.
"""
function serve_chunk_store(;host="127.0.0.1", port=8000, dir=dirname(dirname(dirname(@__FILE__))))
    @info("Starting HTTP server", host, port, dir)
    LiveServer.serve(;host, port, dir)
end

end # module HTTPServer
