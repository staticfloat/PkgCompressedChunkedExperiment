# Reorganized PkgCompressedChunkedExperiment

This repository has been reorganized into three main pieces for better separation of concerns:

## Architecture

### 1. Server-side (`src/Server/`)
- **ChunkingEngine.jl**: Content-defined chunking using desync
- **ChunkStore.jl**: Chunk storage management and recompression with zstd dictionaries  
- **HTTPServer.jl**: HTTP server for serving chunks and indices

### 2. Client-side (`src/Client/`)
- **ChunkIndexing.jl**: Loading and parsing chunk indices (.caibx files)
- **ChunkSynthesis.jl**: Reconstituting files from chunks
- **RegistryUpdater.jl**: Registry update simulation and chunk downloading

### 3. Common/Shared (`src/Common/`)
- **ChunkTypes.jl**: Core data types (ChunkId, CompressedChunk)
- **LibZstd.jl**: Low-level Zstd frame parsing
- **LibZstdSeekable.jl**: Seekable Zstd IO implementation
- **TarAdapter.jl**: Tar filesystem adapter for random access

### 4. Test/Utilities (`test/`)
- **RegistryUtils.jl**: Registry hash definitions and download functions (moved from common)

## Usage

### Server Setup
```bash
julia --project server_setup.jl
```
This downloads registries, chunks them, creates a chunk store, and recompresses with zstd dictionaries.

### Server Serving
```bash
julia --project server_serve.jl
```
Starts an HTTP server on localhost:8000 to serve the chunk store.

### Client Update Simulation
```bash
julia --project client_update.jl
```
Simulates a client registry update by downloading only missing chunks and reconstituting the new registry.

### Compression Analysis
```bash
julia --project compare_compression.jl
```
Compares different compression scenarios and transfer requirements.

### Seekable Access Test
```bash
julia --project test_seekable.jl  
```
Tests random access to compressed tar files using the seekable Zstd functionality.

## Key Benefits of Reorganization

1. **Clear separation of concerns**: Server handles chunking/serving, client handles downloading/synthesis, common provides shared utilities

2. **Modular design**: Each component can be used independently or replaced without affecting others

3. **Scalable architecture**: Server-side operations are isolated from client-side operations

4. **Maintainable code**: Related functionality is grouped together logically

The reorganization maintains all the original functionality while providing a much cleaner and more maintainable codebase structure.
