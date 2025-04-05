#!/usr/bin/env -S julia --project

using LiveServer
LiveServer.serve(;host="127.0.0.1", port=8000, dir=@__DIR__)
