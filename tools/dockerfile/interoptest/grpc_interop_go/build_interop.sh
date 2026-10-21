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
# Builds Go interop server and client in a base image.
set -e

# Turn on support for Go modules.
export GO111MODULE=on

# Clone just the grpc-go source code without any dependencies.
# We are cloning from a local git repo that contains the right revision
# to test instead of using "go get" to download from Github directly.
git clone --recursive /var/local/jenkins/grpc-go src/google.golang.org/grpc

# copy service account keys if available
cp -r /var/local/jenkins/service_account $HOME || true

# Build the interop client and server
(cd src/google.golang.org/grpc/interop/client && go install)
(cd src/google.golang.org/grpc/interop/server && go install)
if [ -d src/google.golang.org/grpc/interop/http2 ]; then
  (cd src/google.golang.org/grpc/interop/http2 && go build -o /go/bin/negative_http2_client negative_http2_client.go) || true
fi

# Install standardized container entrypoints
mkdir -p /interop
cat << 'EOF' > /interop/client
#!/bin/bash
set -e
export GO111MODULE=on
cd /go/src/google.golang.org/grpc/interop/client
if [ -x /go/bin/client ]; then
  exec /go/bin/client "$@"
fi
exec go run client.go "$@"
EOF
cat << 'EOF' > /interop/server
#!/bin/bash
set -e
export GO111MODULE=on
cd /go/src/google.golang.org/grpc/interop/server
if [ -x /go/bin/server ]; then
  exec /go/bin/server "$@"
fi
exec go run server.go "$@"
EOF
cat << 'EOF' > /interop/http2_client
#!/bin/bash
set -e
export GO111MODULE=on
cd /go/src/google.golang.org/grpc/interop/http2
if [ -x /go/bin/negative_http2_client ]; then
  exec /go/bin/negative_http2_client "$@"
fi
exec go run negative_http2_client.go "$@"
EOF
chmod +x /interop/client /interop/server /interop/http2_client

