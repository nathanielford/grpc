# Copyright 2026 The gRPC Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG BASE_IMAGE=base
FROM rust:1.88-bookworm AS base

RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    git \
    pkg-config \
  && apt-get clean

RUN git config --global --add safe.directory '*'
RUN git config --global protocol.file.allow always

# Define the default command.
CMD ["bash"]

#=== INTEROP_BUILD_STAGE ===
FROM ${BASE_IMAGE} AS interop
ARG RELEASE_LABEL=""
LABEL release="${RELEASE_LABEL}"
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
COPY --from=grpc_rust . /var/local/git/grpc-rust
RUN cd /var/local/git/grpc-rust \
  && CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback \
     cargo build --release -p interop --bins --features protoc-gen-grpc-rust \
  && mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-rust\nexec /var/local/git/grpc-rust/target/release/client --codec=protobuf "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-rust\nexec /var/local/git/grpc-rust/target/release/server --codec=protobuf "$@"\n' > /interop/server \
  && chmod +x /interop/client /interop/server
