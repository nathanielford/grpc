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
# to build the docker image for interop testing in a single docker build pass
# with structured OCI metadata labels.

set -ex

# Params:
#  INTEROP_IMAGE - name of tag of the final interop image
#  BASE_NAME - base name used to locate the base Dockerfile and build script
#  INTEROP_RELEASE - release tag to embed in OCI labels (default: master)
#  COMMIT_LOG_FILE - optional path to commit_log file for /var/local/build_info
#  BUILD_INTEROP_DOCKER_EXTRA_ARGS - optional args to be passed to the
#    docker build command
#  GRPC_ROOT - grpc base directory, default to top of this tree.
#  GRPC_GO_ROOT - grpc-go base directory, default to '$GRPC_ROOT/../grpc-go'
#  GRPC_JAVA_ROOT - grpc-java base directory, default to '$GRPC_ROOT/../grpc-java'

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$REPO_ROOT"
echo "GRPC_ROOT: ${GRPC_ROOT:=$(pwd)}"

BUILD_CONTEXT_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$BUILD_CONTEXT_DIR"
}
trap cleanup EXIT

stage_tree() {
  local src="$1"
  local dst="$2"
  if [ -n "$src" ] && [ -e "$src" ]; then
    mkdir -p "$(dirname "$dst")"
    cp -al "$src" "$dst" 2>/dev/null || cp -a "$src" "$dst"
  fi
}

mkdir -p "$BUILD_CONTEXT_DIR/jenkins"

case "$BASE_NAME" in
  grpc_interop_java*)
    echo "GRPC_JAVA_ROOT: ${GRPC_JAVA_ROOT:=$(cd ../grpc-java 2>/dev/null && pwd || true)}"
    stage_tree "$GRPC_JAVA_ROOT" "$BUILD_CONTEXT_DIR/jenkins/grpc-java"
    ;;
  grpc_interop_go*)
    echo "GRPC_GO_ROOT: ${GRPC_GO_ROOT:=$(cd ../grpc-go 2>/dev/null && pwd || true)}"
    stage_tree "$GRPC_GO_ROOT" "$BUILD_CONTEXT_DIR/jenkins/grpc-go"
    ;;
  grpc_interop_dart*)
    echo "GRPC_DART_ROOT: ${GRPC_DART_ROOT:=$(cd ../grpc-dart 2>/dev/null && pwd || true)}"
    stage_tree "$GRPC_DART_ROOT" "$BUILD_CONTEXT_DIR/jenkins/grpc-dart"
    ;;
  grpc_interop_node*)
    echo "GRPC_NODE_ROOT: ${GRPC_NODE_ROOT:=$(cd ../grpc-node 2>/dev/null && pwd || true)}"
    stage_tree "$GRPC_NODE_ROOT" "$BUILD_CONTEXT_DIR/jenkins/grpc-node"
    ;;
  grpc_interop_aspnetcore*)
    echo "GRPC_DOTNET_ROOT: ${GRPC_DOTNET_ROOT:=$(cd ../grpc-dotnet 2>/dev/null && pwd || true)}"
    stage_tree "$GRPC_DOTNET_ROOT" "$BUILD_CONTEXT_DIR/jenkins/grpc-dotnet"
    ;;
  *)
    stage_tree "$GRPC_ROOT" "$BUILD_CONTEXT_DIR/jenkins/grpc"
    ;;
esac

# Always ensure the build_interop.sh script is staged under jenkins/grpc
if [ ! -e "$BUILD_CONTEXT_DIR/jenkins/grpc/tools/dockerfile/interoptest/$BASE_NAME/build_interop.sh" ]; then
  if [ -e "$GRPC_ROOT/tools/dockerfile/interoptest/$BASE_NAME" ]; then
    stage_tree "$GRPC_ROOT/tools/dockerfile/interoptest/$BASE_NAME" \
      "$BUILD_CONTEXT_DIR/jenkins/grpc/tools/dockerfile/interoptest/$BASE_NAME"
  else
    stage_tree "$REPO_ROOT/tools/dockerfile/interoptest/$BASE_NAME" \
      "$BUILD_CONTEXT_DIR/jenkins/grpc/tools/dockerfile/interoptest/$BASE_NAME"
  fi
fi

# Stage service account directory if available.
if [ -e "$HOME/service_account" ]; then
  stage_tree "$HOME/service_account" "$BUILD_CONTEXT_DIR/jenkins/service_account"
fi

# Locate base image version from .current_version file.
if [ -f "$GRPC_ROOT/tools/dockerfile/interoptest/${BASE_NAME}.current_version" ]; then
  BASE_IMAGE="$(cat "$GRPC_ROOT/tools/dockerfile/interoptest/${BASE_NAME}.current_version")"
else
  BASE_IMAGE="$(cat "$REPO_ROOT/tools/dockerfile/interoptest/${BASE_NAME}.current_version")"
fi

COMMIT_LOG_FLAG=()
if [ -n "${COMMIT_LOG_FILE:-}" ] && [ -f "$COMMIT_LOG_FILE" ]; then
  cp "$COMMIT_LOG_FILE" "$BUILD_CONTEXT_DIR/commit_log"
  COMMIT_LOG_FLAG=("--include-commit-log")
elif [ -f "$GRPC_ROOT/commit_log" ]; then
  cp "$GRPC_ROOT/commit_log" "$BUILD_CONTEXT_DIR/commit_log"
  COMMIT_LOG_FLAG=("--include-commit-log")
fi

ENV_PROPAGATE_FILE="$REPO_ROOT/tools/run_tests/dockerize/docker_propagate_env.list"
DOCKER_BUILD_ARGS=()
if [ -f "$ENV_PROPAGATE_FILE" ]; then
  while IFS= read -r env_var || [ -n "$env_var" ]; do
    env_var="${env_var%%#*}"
    env_var="${env_var// /}"
    if [ -n "$env_var" ] && [ -n "${!env_var:-}" ]; then
      DOCKER_BUILD_ARGS+=("--build-arg" "${env_var}=${!env_var}")
    fi
  done < "$ENV_PROPAGATE_FILE"
fi

python3 "$REPO_ROOT/tools/run_tests/python_utils/interop_capabilities.py" \
  --emit-dockerfile \
  --base-name="$BASE_NAME" \
  --base-image="$BASE_IMAGE" \
  --release="${INTEROP_RELEASE:-master}" \
  --propagate-env-file="$ENV_PROPAGATE_FILE" \
  "${COMMIT_LOG_FLAG[@]}" > "$BUILD_CONTEXT_DIR/Dockerfile"

# Single-pass docker build baking the build step and OCI metadata labels.
# shellcheck disable=SC2086
docker build \
  --rm \
  --tag "$INTEROP_IMAGE" \
  "${DOCKER_BUILD_ARGS[@]}" \
  ${BUILD_INTEROP_DOCKER_EXTRA_ARGS:-} \
  "$BUILD_CONTEXT_DIR"

echo "Successfully built image $INTEROP_IMAGE"
