# syntax=docker/dockerfile:1.4
# Copyright 2024 gRPC authors.
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
#
# Declarative BuildKit Dockerfile for gRPC interop test images.
# Replaces the legacy `docker run ... build_interop.sh && docker commit`
# workflow by mounting local git repositories as read-only named contexts.

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG BASE_NAME
ARG GRPC_BUILD_SCRIPT=/var/local/jenkins/grpc/tools/dockerfile/interoptest/${BASE_NAME}/build_interop.sh
ARG RELEASE="master"

# Propagated CI/build environment variables
ARG KOKORO_BUILD_ID=""
ARG KOKORO_BUILD_NUMBER=""
ARG KOKORO_JOB_NAME=""
ARG GRPC_BUILD_ACTIVATE_KV=""

RUN --mount=type=bind,from=grpc,target=/var/local/jenkins/grpc,ro \
    --mount=type=bind,from=grpc_java,target=/var/local/jenkins/grpc-java,ro \
    --mount=type=bind,from=grpc_go,target=/var/local/jenkins/grpc-go,ro \
    --mount=type=bind,from=grpc_dart,target=/var/local/jenkins/grpc-dart,ro \
    --mount=type=bind,from=grpc_node,target=/var/local/jenkins/grpc-node,ro \
    --mount=type=bind,from=grpc_dotnet,target=/var/local/jenkins/grpc-dotnet,ro \
    --mount=type=bind,from=service_account,target=/var/local/jenkins/service_account,ro \
    bash -l "${GRPC_BUILD_SCRIPT}"

LABEL release="${RELEASE}"

RUN --mount=type=bind,from=build_info,target=/tmp/grpc_build_info,ro \
    if [ -f /tmp/grpc_build_info/commit_log ]; then \
      mkdir -p /var/local/build_info && \
      cp /tmp/grpc_build_info/commit_log /var/local/build_info/commit_log; \
    fi
