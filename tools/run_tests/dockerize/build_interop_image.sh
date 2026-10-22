#!/bin/bash
# Copyright 2015 gRPC authors.
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
# This script is invoked by run_interop_tests.py and create_matrix_images.py
# to build the docker image for interop testing directly via BuildKit
# multi-stage Dockerfiles (without docker run + docker commit).

set -ex

# Params:
#  INTEROP_IMAGE - name of tag of the final interop image
#  BASE_NAME - base name used to locate the base Dockerfile
#  BUILD_INTEROP_DOCKER_EXTRA_ARGS - optional extra args passed to docker build
#  GRPC_ROOT - grpc base directory, default to top of this tree.
#  GRPC_GO_ROOT - grpc-go base directory, default to '$GRPC_ROOT/../grpc-go'
#  GRPC_JAVA_ROOT - grpc-java base directory, default to '$GRPC_ROOT/../grpc-java'
#  GRPC_DART_ROOT - grpc-dart base directory, default to '$GRPC_ROOT/../grpc-dart'
#  GRPC_NODE_ROOT - grpc-node base directory, default to '$GRPC_ROOT/../grpc-node'
#  GRPC_DOTNET_ROOT - grpc-dotnet base directory, default to '$GRPC_ROOT/../grpc-dotnet'
#  RELEASE_LABEL - optional release version label (e.g. 'v1.76.0')
#  BUILD_INFO_FILE - optional path to commit_log file to copy into /var/local/build_info/
#  INTEROP_DOCKERFILE_ROOT - optional root directory for locating tools/dockerfile/interoptest/$BASE_NAME
#  INTEROP_BUILD_DRY_RUN - if non-empty, print the docker build command without executing it

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$REPO_ROOT"

echo "GRPC_ROOT: ${GRPC_ROOT:=$REPO_ROOT}"
echo "GRPC_JAVA_ROOT: ${GRPC_JAVA_ROOT:=$(cd ../grpc-java 2>/dev/null && pwd || true)}"
echo "GRPC_GO_ROOT: ${GRPC_GO_ROOT:=$(cd ../grpc-go 2>/dev/null && pwd || true)}"
echo "GRPC_DART_ROOT: ${GRPC_DART_ROOT:=$(cd ../grpc-dart 2>/dev/null && pwd || true)}"
echo "GRPC_NODE_ROOT: ${GRPC_NODE_ROOT:=$(cd ../grpc-node 2>/dev/null && pwd || true)}"
echo "GRPC_DOTNET_ROOT: ${GRPC_DOTNET_ROOT:=$(cd ../grpc-dotnet 2>/dev/null && pwd || true)}"

DOCKERFILE_ROOT="${INTEROP_DOCKERFILE_ROOT:-$REPO_ROOT}"
BASE_IMAGE_DIR="${DOCKERFILE_ROOT}/tools/dockerfile/interoptest/${BASE_NAME}"
if [ ! -d "$BASE_IMAGE_DIR" ]; then
  BASE_IMAGE_DIR="${REPO_ROOT}/tools/dockerfile/interoptest/${BASE_NAME}"
fi

if [ -f "${BASE_IMAGE_DIR}.current_version" ]; then
  BASE_IMAGE="$(cat "${BASE_IMAGE_DIR}.current_version")"
elif [ -f "${REPO_ROOT}/tools/dockerfile/interoptest/${BASE_NAME}.current_version" ]; then
  BASE_IMAGE="$(cat "${REPO_ROOT}/tools/dockerfile/interoptest/${BASE_NAME}.current_version")"
else
  BASE_IMAGE="base"
fi

TEMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

EMPTY_CTX="${TEMP_DIR}/empty"
BUILD_INFO_CTX="${TEMP_DIR}/build_info"
mkdir -p "$EMPTY_CTX" "$BUILD_INFO_CTX"

if [ -n "${BUILD_INFO_FILE:-}" ] && [ -f "$BUILD_INFO_FILE" ]; then
  cp "$BUILD_INFO_FILE" "${BUILD_INFO_CTX}/"
fi

if [ -d "${HOME}/service_account" ]; then
  SERVICE_ACCOUNT_CTX="${HOME}/service_account"
else
  SERVICE_ACCOUNT_CTX="$EMPTY_CTX"
fi

BUILD_ARGS=(
  --target interop
  --tag "$INTEROP_IMAGE"
  --build-arg "BASE_IMAGE=${BASE_IMAGE}"
  --build-arg "RELEASE_LABEL=${RELEASE_LABEL:-}"
  --build-context "grpc=${GRPC_ROOT}"
  --build-context "grpc_java=${GRPC_JAVA_ROOT:-$EMPTY_CTX}"
  --build-context "grpc_go=${GRPC_GO_ROOT:-$EMPTY_CTX}"
  --build-context "grpc_dart=${GRPC_DART_ROOT:-$EMPTY_CTX}"
  --build-context "grpc_node=${GRPC_NODE_ROOT:-$EMPTY_CTX}"
  --build-context "grpc_dotnet=${GRPC_DOTNET_ROOT:-$EMPTY_CTX}"
  --build-context "service_account=${SERVICE_ACCOUNT_CTX}"
  --build-context "build_info=${BUILD_INFO_CTX}"
)

if [ -n "${RELEASE_LABEL:-}" ]; then
  BUILD_ARGS+=(--label "release=${RELEASE_LABEL}")
fi

# Propagate non-empty environment variables from docker_propagate_env.list as build-args.
ENV_LIST_FILE="${REPO_ROOT}/tools/run_tests/dockerize/docker_propagate_env.list"
if [ -f "$ENV_LIST_FILE" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%%#*}"
    line="$(echo "$line" | xargs)"
    [ -z "$line" ] && continue
    if [[ "$line" == *=* ]]; then
      BUILD_ARGS+=(--build-arg "$line")
    elif [ -n "${!line:-}" ]; then
      BUILD_ARGS+=(--build-arg "${line}=${!line}")
    fi
  done < "$ENV_LIST_FILE"
fi

if [ -n "${BUILD_INTEROP_DOCKER_EXTRA_ARGS:-}" ]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS_ARRAY=($BUILD_INTEROP_DOCKER_EXTRA_ARGS)
  BUILD_ARGS+=("${EXTRA_ARGS_ARRAY[@]}")
fi

TARGET_DOCKERFILE_DIR="$BASE_IMAGE_DIR"
if ! grep -q '^#=== INTEROP_BUILD_STAGE ===$' "${BASE_IMAGE_DIR}/Dockerfile" 2>/dev/null; then
  # Fallback for historical release tags checked out by create_matrix_images.py
  # whose legacy Dockerfile does not contain the multi-stage interop target.
  LEGACY_WRAPPER_DIR="${TEMP_DIR}/legacy_dockerfile"
  mkdir -p "$LEGACY_WRAPPER_DIR"
  cat > "${LEGACY_WRAPPER_DIR}/Dockerfile" << 'EOF'
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS interop
ARG RELEASE_LABEL=""
ARG BASE_NAME=""
ARG GRPC_BUILD_ENABLE_CCACHE=""
ARG CCACHE_SECONDARY_STORAGE=""
LABEL release="${RELEASE_LABEL}"
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
RUN --mount=type=bind,from=grpc,target=/var/local/jenkins/grpc \
    --mount=type=bind,from=grpc_java,target=/var/local/jenkins/grpc-java \
    --mount=type=bind,from=grpc_go,target=/var/local/jenkins/grpc-go \
    --mount=type=bind,from=grpc_dart,target=/var/local/jenkins/grpc-dart \
    --mount=type=bind,from=grpc_node,target=/var/local/jenkins/grpc-node \
    --mount=type=bind,from=grpc_dotnet,target=/var/local/jenkins/grpc-dotnet \
    --mount=type=bind,from=service_account,target=/var/local/jenkins/service_account \
    bash -l "/var/local/jenkins/grpc/tools/dockerfile/interoptest/${BASE_NAME}/build_interop.sh"
EOF
  BUILD_ARGS+=(--build-arg "BASE_NAME=${BASE_NAME}")
  TARGET_DOCKERFILE_DIR="$LEGACY_WRAPPER_DIR"
fi

if [ -n "${INTEROP_BUILD_DRY_RUN:-}" ]; then
  echo "DRY_RUN: docker buildx build --load ${BUILD_ARGS[*]} ${TARGET_DOCKERFILE_DIR}"
  exit 0
fi

if docker buildx version >/dev/null 2>&1; then
  BUILDER_ARGS=()
  if docker buildx inspect default >/dev/null 2>&1; then
    BUILDER_ARGS=(--builder default)
  fi
  docker buildx build --load "${BUILDER_ARGS[@]}" "${BUILD_ARGS[@]}" "$TARGET_DOCKERFILE_DIR"
else
  DOCKER_BUILDKIT=1 docker build "${BUILD_ARGS[@]}" "$TARGET_DOCKERFILE_DIR"
fi

echo "Successfully built image $INTEROP_IMAGE"
