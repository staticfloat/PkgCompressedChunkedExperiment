# PkgCompressedChunkedExperiment

This repository's purpose is to prove out a possible forward path on improving registry and package updates for Julia.

## Layout

* Server-side (`src/Server/`)
- `ChunkingEngine.jl`: Content-defined chunking using desync
- `ChunkStore.jl`: Chunk storage management and recompression with zstd dictionaries  
- `HTTPServer.jl`: HTTP server for serving chunks and indices

* Client-side (`src/Client/`)
- `ChunkIndexing.jl`: Loading and parsing chunk indices (.caibx files)
- `ChunkSynthesis.jl`: Reconstituting files from chunks
- `RegistryUpdater.jl`: Registry update simulation and chunk downloading

* Common/Shared (`src/Common/`)
- `ChunkTypes.jl`: Core data types (ChunkId, CompressedChunk)
- `LibZstd.jl`: Low-level Zstd frame parsing
- `LibZstdSeekable.jl`: Seekable Zstd IO implementation
- `TarAdapter.jl`: Tar filesystem adapter for random access

* Test/Utilities (`test/`)
- `RegistryUtils.jl`: Registry hash definitions and download functions

## Running the code

To run the experiment locally, first run:

```bash
julia --project -e 'import Pkg; Pkg.instantiate()'
julia --project server_setup.jl
julia --project server_serve.jl
```

This will download some registries for use, then chunk those and create what a PkgServer would contain; a chunk store and some indexes.
It will then start an HTTP server on port 8000 that serves the current directory, so that we can create this mockup with the greatest possible fidelity.

Next, run:
```bash
julia --project client_update.jl
```

This will simulate a client starting with an old registry, downloading a new index, then identifying which chunks are needed, downloading them, and synthesizing a new registry, then hashing the decompressed output to prove it is bit-for-bit correct.

### Additional Scripts

- `compare_compression.jl` - Compare different compression settings and scenarios
- `test_seekable.jl` - Test seekable Zstd and Tar functionality

# Background

Our hypothesis is that there are significant savings to be had if we can better compress or somehow make use of the previous registry or package data that exists on client machines during the update process.
Previous proposals have included:
* Compressing registries and packages with `xz` or `zstd`.
* [Using `bsdiff`/`bspatch` to generate patches](https://github.com/mendsley/bsdiff) on the server side then transferring those to clients.

In the first case, the benefits did not pass the pain threshold for actual implementation, and in the second case, the complexity of generating patches (Which pairwise combinations do we generate among all the different registry or package versions, how best to allocate CPU time without allowing malicious actors to create denial-of-service attacks) kept the idea from being implemented.

## Content-Defined Chunking and Zstd Frames

The ideas laid out by [the `casync` project](https://0pointer.net/blog/casync-a-tool-for-distributing-file-system-images.html) directly solve the second issue nicely, by providing an efficient way to chunk files into small, content-addressed pieces, then update them by downloading any pieces that are missing, and reassembling the file from the disparate chunks.
We adapt the ideas of `casync` slightly by chunking our (uncompressed) tarfiles, storing the chunks as `zstd` files and then reconstituting our tarfiles by simply concatenating the compressed chunks wihtout needing to decompress each chunk, concatenate the uncompressed pieces, then recompressing.
This is possible due to the useful property of `zstd` that a concatenation of frames must be correctly decoded as the concatenation of their outputs.
We add a chunk ID table at the end of the `zstd` stream, identifying each chunk so that the client does not need to re-chunk the local tarball (indeed, the client does not even need to implement the chunking algorithm, only the server does).
In this way, our synthesized tarballs contain within them the chunk index they are constructed by, and updating a tarball to a new version is a simple as copying pieces out of that tarball and inserting some new ones downloaded from a server.
We also optimize the `zstd` storage slightly by training a `zstd` dictionary on the uncompressed pieces, which improves compression ratio significantly when dealing with such small fragments of files.

### Seekable compressed archives

Because our compressed tarballs are chopped up into small pieces, it makes random access within the tarball much more tractible than it would be if we had to decompress the tarball from the beginning every time to access a file in the middle of it,.
We append a seek table to the end of the `zstd` stream (implementing the [zstd seekable format](https://github.com/facebook/zstd/blob/v1.5.7/contrib/seekable_format/zstd_seekable_compression_format.md#seek-table-format)), allowing the code in `src/Common/TarAdapter.jl` to seek to random files within the tarball with close to native filesystem speed.
We propose that registries and packages could be stored within this compressed tarball format and never unpacked, providing greater package integrity, reduced disk space footprint, and faster installation and upgrades.
Artifacts remain a topic of discussion (and would arguably benefit the most from the casync-style upgrades) and may require the client to perform chunking by re-reading the files on disk into an in-memory tar format for chunking.

## Current results

The `compare_compression.jl` script performs an exhaustive test comparing different scenarios.
It compares the current methodology (transferring a gzipped tarball every time) to alternatives such as transferring a `zstd`-compressed tarball every time, and this content-defined chunking scheme.

A partial view of the results can be seen in this image:
![](./CompressionStats.png)

While a simple `zstd`-compressed tarball can achieve the best on-disk compression ratio, the chunked forms achieve a much lower amount of data transferred for the update process.
This graph is showing results for only a single registry update (simulating a time delta of ~10 days), although the relative ordering of solutions remains constant over a variety of choices.

My current thinking is that we can probably go with the medium chunksize to reduce the number of chunks needed to be handled, as well as slightly improving the on-disk footprint.

