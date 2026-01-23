# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FalconFS is a high-performance distributed file system optimized for AI workloads, designed to handle massive small files, provide TB/s throughput, and scale to thousands of NPUs. It consists of four main components:

1. **Metadata Engine** (`falcon/`) - PostgreSQL extension providing distributed metadata with sharded file metadata and replicated directory namespace
2. **File Store** (`falcon_store/`) - C++ static library handling tiered storage (DRAM, SSD, cloud object store)
3. **Client** (`falcon_client/`) - User-space FUSE implementation with libFS API for direct access
4. **Cluster Management** (`cloud_native/`, `deploy/`) - Zookeeper-based membership and Ansible/Kubernetes deployment

**Documentation**: See `docs/design.md` for architecture details and `docs/setup.md` for cluster setup.

## Build Commands

**Development Environment**: Uses container `ghcr.io/falcon-infra/falconfs-dev:ubuntu24.04`

```bash
# Full build (PostgreSQL + FalconFS)
./build.sh

# Build only PostgreSQL plugin
./build.sh build pg [--debug|--deploy]

# Build only FalconFS client
./build.sh build falcon [--debug|--release|--relwithdebinfo] [--with-fuse-opt] [--with-zk-init] [--with-rdma] [--with-prometheus]

# Run tests (executables in build/tests/falcon_store/)
./build.sh test

# Clean
./build.sh clean [pg|falcon|test|dist]

# Install
./build.sh install [pg|falcon]
```

For clangd: `ln -s build/compile_commands.json .`

**Running individual tests**: Tests are built to `build/tests/falcon_store/`. Run a specific test with `./build/tests/falcon_store/<test_name>`.

**Code formatting**: Uses `.clang-format` (LLVM-style, C++23). Format with: `clang-format -i <files>`

## Architecture Details

### Metadata Engine (`falcon/`)

PostgreSQL extension (`contrib/falcon`) built on top of PostgreSQL as a meta DB. Key subdirectories:

- `metadb/` - Core database logic: `inode_table.h`, `directory_table.h`, `shard_table.h`, `metadata.h`
- `connection_pool/` - Connection management with concurrent request merging: `pg_connection_pool.h`, `falcon_batch_service_def.h`
- `transaction/` - Distributed transaction support: `falcon_distributed_transaction.h`
- `distributed_backend/` - Remote communication: `distributed_backend_falcon.h`, `remote_comm_falcon.h`
- `brpc_comm_adapter/` - BRPC-based communication layer

**Key Design**: Replicated directory namespace (all metadata servers have full directory tree for local path resolution) + sharded file metadata (hashed by filename across shards).

### File Store (`falcon_store/`)

C++ library (`FalconStore`) with:
- `storage/` - Storage backends: `storage.h`, `obs_storage.h` (Huawei OBS)
- `disk_cache/` - Caching layer: `disk_cache.h`
- `brpc/` - BRPC server: `brpc_server.h`
- `connection/` - I/O client: `falcon_io_client.h`

### Client (`falcon_client/`)

- `fuse_main.cpp` - Main FUSE entry point
- `src/include/falcon_meta.h` - Metadata client interface
- `src/include/router.h` - Shard mapping and request routing
- `src/include/connection.h` - Connection management

### Protocol Definitions (`remote_connection_def/proto/`)

- `falcon_meta_rpc.proto` - Metadata service RPC definitions
- `brpc_io.proto` - I/O service RPC definitions

**Important**: Proto changes require regeneration via `protoc` (handled by `build.sh`).

## Critical Build Details

1. **POSTGRES_SRC_DIR**: Must be set (CMake requirement). `build.sh` copies `falcon/` to `third_party/postgres/contrib/falcon`. **Never edit files in `third_party/postgres` directly** - they will be overwritten.

2. **Proto generation**: `build.sh` runs `protoc` to generate `.pb.cc/.h` files in `build/`. If you modify `.proto` files, the build system handles regeneration.

3. **FlatBuffers**: Schema changes require FlatBuffers generation step. See `cmake/Flatbuffers.cmake` and generated headers like `falcon_meta_param_generated.h`.

4. **OBS SDK**: Expects dependencies at `/usr/local/obs/include` and `/usr/local/obs/lib`.

5. **Environment variables**: `POSTGRES_SRC_DIR`, `CONFIG_FILE` (set by `build.sh`), `FALCONFS_INSTALL_DIR` (default: `/usr/local/falconfs`).

## Code Conventions

- **Error handling**: Internal error codes mapped to POSIX errno via `ErrorCodeToErrno()`. Pattern: `return ret > 0 ? -ErrorCodeToErrno(ret) : ret;` (see `fuse_main.cpp`)
- **Statistics**: Use `FalconStats::GetInstance()` for metrics collection
- **Latency measurement**: Use `StatFuseTimer`/`META_LAT` macros
- **FUSE operations**: Implement standard FUSE ops (`DoRead`, `DoWrite`, `DoOpen`, etc.) that call Falcon frontend interfaces

## External Dependencies

**Third-party**: BRPC, Protobuf, GFlags, LevelDB, OpenSSL, glog, jsoncpp, zookeeper_mt, FlatBuffers, OBS SDK

**System**: fuse, libpq (PostgreSQL client), pthread, dl, zlib

## Testing

Tests located in `tests/`, built to `build/tests/`. Run via `./build.sh test`.

**Test categories**:
- `tests/falcon_store/` - Unit tests for file store functionality (disk cache, file lock, I/O streaming, etc.)
- `tests/private-directory-test/` - High-concurrency metadata performance tests (LibFS interface)
- `tests/common/` - Common test utilities

**Integration testing**: `.github/workflows/smoke_test.sh` runs FIO-based I/O tests (BIO/DIO, sequential/random, large/small files) for mounted FalconFS.

## Development Notes

- **C++ Standard**: C++23 (set in top-level CMakeLists.txt)
- **PostgreSQL integration**: When debugging PostgreSQL plugin issues, work from `third_party/postgres` source tree
- **BRPC plugin**: The `brpc_comm_adapter` is compiled as a separate plugin (`libbrpcplugin.so`) via `MakefilePlugin.brpc`
- **NUMA optimization**: For metadata performance, multiple DN instances can be bound to NUMA nodes using `numactl --cpunodebind=${i} --localalloc pg_ctl start`
