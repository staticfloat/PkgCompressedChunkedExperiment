#!/usr/bin/env -S julia --project

# Server-side script: Serves the chunk store over HTTP

include("src/Common/Common.jl")
include("src/Server/HTTPServer.jl")

using .HTTPServer

serve_chunk_store()
