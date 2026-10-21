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
# This script is invoked by run_interop_tests.py to build the docker image
# for interop testing. You should never need to call this script on your own.

set -ex

# Params:
#  INTEROP_IMAGE - name of tag of the final interop image
#  BASE_NAME - base name used to locate the base Dockerfile and build script
#  BUILD_INTEROP_DOCKER_EXTRA_ARGS - optional args to be passed to the
#    docker run command
#  GRPC_ROOT - grpc base directory, default to top of this tree.
#  GRPC_GO_ROOT - grpc-go base directory, default to '$GRPC_ROOT/../grpc-go'
#  GRPC_JAVA_ROOT - grpc-java base directory, default to '$GRPC_ROOT/../grpc-java'
#  INTEROP_IMAGE_LABEL - optional label to attach to the final image (e.g. release=v1.76.0)
#  BUILD_INFO_COMMIT_LOG - optional commit_log file to copy to /var/local/build_info/commit_log
#  USE_DOCKER_BUILD - when set to 1, use 'docker build' directly instead of 'docker run' + 'docker commit'
#  STAGE_ENTRYPOINTS_ONLY - when set to a directory path, write /interop/* scripts for BASE_NAME and exit

stage_interop_entrypoints() {
  local base_name="$1"
  local dest_dir="$2"
  mkdir -p "$dest_dir"

  case "$base_name" in
    grpc_interop_cxx)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc
if [ -x cmake/build/interop_client ]; then
  exec cmake/build/interop_client "$@"
fi
exec bins/opt/interop_client "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc
if [ -x cmake/build/interop_server ]; then
  exec cmake/build/interop_server "$@"
fi
exec bins/opt/interop_server "$@"
EOF
      cat << 'EOF' > "$dest_dir/http2_client"
#!/bin/bash
set -e
cd /var/local/git/grpc
if [ -x cmake/build/http2_client ]; then
  exec cmake/build/http2_client "$@"
fi
exec bins/opt/http2_client "$@"
EOF
      ;;
    grpc_interop_aspnetcore)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc-dotnet/output/InteropTestsClient
exec dotnet exec InteropTestsClient.dll "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc-dotnet/output/InteropTestsWebsite
exec dotnet exec InteropTestsWebsite.dll "$@"
EOF
      ;;
    grpc_interop_dart)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc-dart/interop
exec dart bin/client.dart "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc-dart/interop
exec dart bin/server.dart "$@"
EOF
      ;;
    grpc_interop_go1.8)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /go/src/google.golang.org/grpc/interop/client
if [ -x /go/bin/client ]; then
  exec /go/bin/client "$@"
fi
exec go run client.go "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /go/src/google.golang.org/grpc/interop/server
if [ -x /go/bin/server ]; then
  exec /go/bin/server "$@"
fi
exec go run server.go "$@"
EOF
      cat << 'EOF' > "$dest_dir/http2_client"
#!/bin/bash
set -e
cd /go/src/google.golang.org/grpc/interop/http2
if [ -x /go/bin/negative_http2_client ]; then
  exec /go/bin/negative_http2_client "$@"
fi
exec go run negative_http2_client.go "$@"
EOF
      ;;
    grpc_interop_go|grpc_interop_go1.11|grpc_interop_go1.16|grpc_interop_go1.19|grpc_interop_go1.x)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
export GO111MODULE=on
cd /go/src/google.golang.org/grpc/interop/client
if [ -x /go/bin/client ]; then
  exec /go/bin/client "$@"
fi
exec go run client.go "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
export GO111MODULE=on
cd /go/src/google.golang.org/grpc/interop/server
if [ -x /go/bin/server ]; then
  exec /go/bin/server "$@"
fi
exec go run server.go "$@"
EOF
      cat << 'EOF' > "$dest_dir/http2_client"
#!/bin/bash
set -e
export GO111MODULE=on
cd /go/src/google.golang.org/grpc/interop/http2
if [ -x /go/bin/negative_http2_client ]; then
  exec /go/bin/negative_http2_client "$@"
fi
exec go run negative_http2_client.go "$@"
EOF
      ;;
    grpc_interop_java|grpc_interop_java_oracle8)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc-java
exec ./run-test-client.sh "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc-java
exec ./run-test-server.sh "$@"
EOF
      cat << 'EOF' > "$dest_dir/http2_client"
#!/bin/bash
set -e
cd /var/local/git/grpc-java
exec ./interop-testing/build/install/grpc-interop-testing/bin/http2-client "$@"
EOF
      ;;
    grpc_interop_node)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /home/appuser/grpc-node
exec packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh \
  node --require ./test/fixtures/native_native test/interop/interop_client.js "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /home/appuser/grpc-node
exec packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh \
  node --require ./test/fixtures/native_native test/interop/interop_server.js "$@"
EOF
      ;;
    grpc_interop_nodepurejs)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /home/appuser/grpc-node
exec packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh \
  node --require ./test/fixtures/js_js test/interop/interop_client.js "$@"
EOF
      ;;
    grpc_interop_php7)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc
exec src/php/bin/interop_client.sh "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc
exec src/php/bin/interop_server.sh "$@"
EOF
      ;;
    grpc_interop_python)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc
export LD_LIBRARY_PATH="/var/local/git/grpc/libs/opt"
export PYTHONPATH="/var/local/git/grpc/src/python/gens"
exec py39/bin/python src/python/grpcio_tests/setup.py run_interop --client --args="$*"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc
export LD_LIBRARY_PATH="/var/local/git/grpc/libs/opt"
export PYTHONPATH="/var/local/git/grpc/src/python/gens"
exec py39/bin/python src/python/grpcio_tests/setup.py run_interop --server --args="$*"
EOF
      cat << 'EOF' > "$dest_dir/http2_client"
#!/bin/bash
set -e
cd /var/local/git/grpc
export LD_LIBRARY_PATH="/var/local/git/grpc/libs/opt"
export PYTHONPATH="/var/local/git/grpc/src/python/gens"
exec py39/bin/python src/python/grpcio_tests/tests/http2/negative_http2_client.py "$@"
EOF
      ;;
    grpc_interop_pythonasyncio)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc
export LD_LIBRARY_PATH="/var/local/git/grpc/libs/opt"
export PYTHONPATH="/var/local/git/grpc/src/python/gens"
exec py39/bin/python src/python/grpcio_tests/setup.py run_interop --use-asyncio --client --args="$*"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc
export LD_LIBRARY_PATH="/var/local/git/grpc/libs/opt"
export PYTHONPATH="/var/local/git/grpc/src/python/gens"
exec py39/bin/python src/python/grpcio_tests/setup.py run_interop --use-asyncio --server --args="$*"
EOF
      cat << 'EOF' > "$dest_dir/http2_client"
#!/bin/bash
set -e
cd /var/local/git/grpc
export LD_LIBRARY_PATH="/var/local/git/grpc/libs/opt"
export PYTHONPATH="/var/local/git/grpc/src/python/gens"
exec py39/bin/python src/python/grpcio_tests/tests/http2/negative_http2_client.py "$@"
EOF
      ;;
    grpc_interop_ruby)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc
exec tools/run_tests/interop/with_rvm.sh ruby src/ruby/pb/test/client.rb "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc
exec tools/run_tests/interop/with_rvm.sh ruby src/ruby/pb/test/server.rb "$@"
EOF
      ;;
    grpc_interop_http2)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc
exec tools/http2_interop/http2_interop.test -test.v "$@"
EOF
      cat << 'EOF' > "$dest_dir/server"
#!/bin/bash
set -e
cd /var/local/git/grpc
exec python test/http2_test/http2_test_server.py "$@"
EOF
      ;;
    grpc_interop_csharp)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc/src/csharp/Grpc.IntegrationTesting.Client/bin/Debug/net45
exec mono Grpc.IntegrationTesting.Client.exe "$@"
EOF
      ;;
    grpc_interop_csharpcoreclr)
      cat << 'EOF' > "$dest_dir/client"
#!/bin/bash
set -e
cd /var/local/git/grpc/src/csharp/Grpc.IntegrationTesting.Client/bin/Debug/netcoreapp3.1
exec dotnet exec Grpc.IntegrationTesting.Client.dll "$@"
EOF
      ;;
  esac
  chmod +x "$dest_dir"/* 2>/dev/null || true
}

if [ -n "$STAGE_ENTRYPOINTS_ONLY" ]; then
  stage_interop_entrypoints "$BASE_NAME" "$STAGE_ENTRYPOINTS_ONLY"
  exit 0
fi

cd "$(dirname "$0")/../../.."
echo "GRPC_ROOT: ${GRPC_ROOT:=$(pwd)}"
MOUNT_ARGS="-v $GRPC_ROOT:/var/local/jenkins/grpc:ro"

echo "GRPC_JAVA_ROOT: ${GRPC_JAVA_ROOT:=$(cd ../grpc-java 2>/dev/null && pwd || true)}"
if [ -n "$GRPC_JAVA_ROOT" ]
then
  MOUNT_ARGS+=" -v $GRPC_JAVA_ROOT:/var/local/jenkins/grpc-java:ro"
else
  echo "WARNING: grpc-java not found, it won't be mounted to the docker container."
fi

echo "GRPC_GO_ROOT: ${GRPC_GO_ROOT:=$(cd ../grpc-go 2>/dev/null && pwd || true)}"
if [ -n "$GRPC_GO_ROOT" ]
then
  MOUNT_ARGS+=" -v $GRPC_GO_ROOT:/var/local/jenkins/grpc-go:ro"
else
  echo "WARNING: grpc-go not found, it won't be mounted to the docker container."
fi

echo "GRPC_DART_ROOT: ${GRPC_DART_ROOT:=$(cd ../grpc-dart 2>/dev/null && pwd || true)}"
if [ -n "$GRPC_DART_ROOT" ]
then
  MOUNT_ARGS+=" -v $GRPC_DART_ROOT:/var/local/jenkins/grpc-dart:ro"
else
  echo "WARNING: grpc-dart not found, it won't be mounted to the docker container."
fi

echo "GRPC_NODE_ROOT: ${GRPC_NODE_ROOT:=$(cd ../grpc-node 2>/dev/null && pwd || true)}"
if [ -n "$GRPC_NODE_ROOT" ]
then
  MOUNT_ARGS+=" -v $GRPC_NODE_ROOT:/var/local/jenkins/grpc-node:ro"
else
  echo "WARNING: grpc-node not found, it won't be mounted to the docker container."
fi

echo "GRPC_DOTNET_ROOT: ${GRPC_DOTNET_ROOT:=$(cd ../grpc-dotnet 2>/dev/null && pwd || true)}"
if [ -n "$GRPC_DOTNET_ROOT" ]
then
  MOUNT_ARGS+=" -v $GRPC_DOTNET_ROOT:/var/local/jenkins/grpc-dotnet:ro"
else
  echo "WARNING: grpc-dotnet not found, it won't be mounted to the docker container."
fi

# Mount service account dir if available.
# If service_directory does not contain the service account JSON file,
# some of the tests will fail.
if [ -e "$HOME/service_account" ]
then
  MOUNT_ARGS+=" -v $HOME/service_account:/var/local/jenkins/service_account:ro"
fi

BASE_IMAGE_DIR="tools/dockerfile/interoptest/$BASE_NAME"
# The exact base docker image to use and its version is determined by the corresponding .current_version file
BASE_IMAGE="$(cat "${BASE_IMAGE_DIR}.current_version")"

STAGE_ROOT="$(mktemp -d)"
trap 'rm -rf "$STAGE_ROOT"' EXIT
stage_interop_entrypoints "$BASE_NAME" "$STAGE_ROOT/interop"

if [ -n "$BUILD_INFO_COMMIT_LOG" ] && [ -f "$BUILD_INFO_COMMIT_LOG" ]; then
  mkdir -p "$STAGE_ROOT/var/local/build_info"
  cp "$BUILD_INFO_COMMIT_LOG" "$STAGE_ROOT/var/local/build_info/commit_log"
fi

if [ "$USE_DOCKER_BUILD" = "1" ]; then
  cp "tools/dockerfile/interoptest/$BASE_NAME/build_interop.sh" "$STAGE_ROOT/build_interop.sh"
  chmod +x "$STAGE_ROOT/build_interop.sh"
  {
    echo "FROM $BASE_IMAGE"
    if [ -n "$INTEROP_IMAGE_LABEL" ]; then
      echo "LABEL $INTEROP_IMAGE_LABEL"
    fi
    echo "COPY interop/ /interop/"
    if [ -d "$STAGE_ROOT/var/local/build_info" ]; then
      echo "COPY var/local/build_info/ /var/local/build_info/"
    fi
    echo "COPY build_interop.sh /tmp/build_interop.sh"
    echo "RUN bash -l /tmp/build_interop.sh && rm -f /tmp/build_interop.sh"
  } > "$STAGE_ROOT/Dockerfile"
  docker build --rm --tag "$INTEROP_IMAGE" "$STAGE_ROOT"
  exit $?
fi

# If TTY is available, the running container can be conveniently terminated with Ctrl+C.
if [[ -t 0 ]]; then
  DOCKER_TTY_ARGS=("-it")
else
  # The input device on kokoro is not a TTY, so -it does not work.
  DOCKER_TTY_ARGS=()
fi

CONTAINER_NAME="build_${BASE_NAME}_$(uuidgen)"
COMMIT_ARGS=()
if [ -n "$INTEROP_IMAGE_LABEL" ]; then
  COMMIT_ARGS+=("--change" "LABEL $INTEROP_IMAGE_LABEL")
fi

# Prepare image for interop tests, install /interop/* entrypoints and optional build_info, and commit on success.
# shellcheck disable=SC2086
(docker run \
  --cap-add SYS_PTRACE \
  --env-file "tools/run_tests/dockerize/docker_propagate_env.list" \
  "${DOCKER_TTY_ARGS[@]}" \
  $MOUNT_ARGS \
  $BUILD_INTEROP_DOCKER_EXTRA_ARGS \
  --name="$CONTAINER_NAME" \
  "$BASE_IMAGE" \
  bash -l "/var/local/jenkins/grpc/tools/dockerfile/interoptest/$BASE_NAME/build_interop.sh" \
  && docker cp "$STAGE_ROOT/interop" "$CONTAINER_NAME:/" \
  && ( [ ! -d "$STAGE_ROOT/var" ] || docker cp "$STAGE_ROOT/var" "$CONTAINER_NAME:/" ) \
  && docker commit "${COMMIT_ARGS[@]}" "$CONTAINER_NAME" "$INTEROP_IMAGE" \
  && echo "Successfully built image $INTEROP_IMAGE")
EXITCODE=$?

# remove intermediate container, possibly killing it first
docker rm -f "$CONTAINER_NAME"

exit $EXITCODE
