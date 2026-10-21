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
# Builds http2 interop client in a base image.
set -e

mkdir -p /var/local/git
git clone /var/local/jenkins/grpc /var/local/git/grpc
# clone gRPC submodules, use data from locally cloned submodules where possible
(cd /var/local/jenkins/grpc/ && git submodule foreach 'cd /var/local/git/grpc \
&& git submodule update --init --reference /var/local/jenkins/grpc/${name} \
${name}')

# copy service account keys if available
cp -r /var/local/jenkins/service_account $HOME || true

# compile the tests
(cd /var/local/git/grpc/tools/http2_interop && go test -c)

# Install standardized container entrypoints
mkdir -p /interop
cat << 'EOF' > /interop/client
#!/bin/bash
set -e
cd /var/local/git/grpc
exec tools/http2_interop/http2_interop.test -test.v "$@"
EOF
cat << 'EOF' > /interop/server
#!/bin/bash
set -e
cd /var/local/git/grpc
exec python test/http2_test/http2_test_server.py "$@"
EOF
chmod +x /interop/client /interop/server

