#!/usr/bin/env -S julia --project

# Test script for seekable Zstd and Tar functionality

using Random, Statistics
include("src/Common/Common.jl")
using .Common
using TOML

# This is the tarfile we'll be reading from
test_tarfile = "./registries/080dd52fa032049906f0b4cba225cd84e000ddd8.zst"

# First, a quick test of just pure random access
ground_truth_data = read(`zstd -c -D ./server_chunk_store/dictionary-32800.zstdict -d $(test_tarfile)`)
zsio = ZstdSeekableIO(test_tarfile)

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

@info("Seekable access test completed", avg_timing=mean(timings)*1e6, unit="microseconds")

# Next, a test of using the Tar adapter
seekstart(zsio)
tf = TarFilesystem(zsio)

reg_toml = TOML.parse(open(tf, "Registry.toml"))
@info("Successfully accessed Registry.toml", name=reg_toml["name"])
