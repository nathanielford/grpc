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
# Builds C++ interop server and client in a base image.
set -e

mkdir -p /var/local/git
git clone /var/local/jenkins/grpc /var/local/git/grpc
# clone gRPC submodules, use data from locally cloned submodules where possible
(cd /var/local/jenkins/grpc/ && git submodule foreach 'cd /var/local/git/grpc \
&& git submodule update --init --reference /var/local/jenkins/grpc/${name} \
${name}')

# copy service account keys if available
cp -r /var/local/jenkins/service_account $HOME || true

cd /var/local/git/grpc

# Install the roots.pem
mkdir -p /usr/local/share/grpc
cp etc/roots.pem /usr/local/share/grpc/roots.pem

# build C++ interop client, interop server and http2 interop client
mkdir -p cmake/build
cd cmake/build
cmake -DgRPC_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 ../..
make interop_client interop_server -j4
make http2_client -j4

# Install standardized container entrypoints
mkdir -p /interop
cat << 'EOF' > /interop/client
#!/bin/bash
set -e
cd /var/local/git/grpc
if [ -x cmake/build/interop_client ]; then
  exec cmake/build/interop_client "$@"
fi
exec bins/opt/interop_client "$@"
EOF
cat << 'EOF' > /interop/server
#!/bin/bash
set -e
cd /var/local/git/grpc
if [ -x cmake/build/interop_server ]; then
  exec cmake/build/interop_server "$@"
fi
exec bins/opt/interop_server "$@"
EOF
cat << 'EOF' > /interop/http2_client
#!/bin/bash
set -e
cd /var/local/git/grpc
if [ -x cmake/build/http2_client ]; then
  exec cmake/build/http2_client "$@"
fi
exec bins/opt/http2_client "$@"
EOF
chmod +x /interop/client /interop/server /interop/http2_client
