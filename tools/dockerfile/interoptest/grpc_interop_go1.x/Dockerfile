# Copyright 2024 The gRPC Authors
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
FROM golang:latest AS base

# Using login shell removes Go from path, so we add it.
RUN ln -s /usr/local/go/bin/go /usr/local/bin

#=================
# Setup git to access working directory across docker boundary.
# This avoids the "fatal: detected dubious ownership in repository XYZ"
# git error.

RUN git config --global --add safe.directory '*'
RUN git config --global protocol.file.allow always

# Define the default command.
CMD ["bash"]

#=== INTEROP_BUILD_STAGE ===
FROM ${BASE_IMAGE} AS interop
ARG RELEASE_LABEL=""
LABEL release="${RELEASE_LABEL}"
LABEL io.grpc.interop.release="${RELEASE_LABEL}"
LABEL io.grpc.interop.client_cmd="[\"go\", \"run\", \"client.go\"]"
LABEL io.grpc.interop.server_cmd="[\"go\", \"run\", \"server.go\"]"
LABEL io.grpc.interop.http2_client_cmd="[\"go\", \"run\", \"negative_http2_client.go\"]"
LABEL io.grpc.interop.workdir="/go/src/google.golang.org/grpc/interop/client"
LABEL io.grpc.interop.env="{\"GO111MODULE\": \"on\"}"
LABEL io.grpc.interop.capabilities="core_interop,timeout_on_sleeping_server,tls"
ENV GO111MODULE=on
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
COPY --from=grpc_go . /go/src/google.golang.org/grpc
RUN (cd /go/src/google.golang.org/grpc/interop/client && go install) \
  && (cd /go/src/google.golang.org/grpc/interop/server && go install)
  && if [ -d /go/src/google.golang.org/grpc/interop/http2 ]; then (cd /go/src/google.golang.org/grpc/interop/http2 && go build -o /go/bin/negative_http2_client negative_http2_client.go) || true; fi
RUN mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\nexport GO111MODULE=on\ncd /go/src/google.golang.org/grpc/interop/client\nif [ -x /go/bin/client ]; then\n  exec /go/bin/client "$@"\nfi\nexec go run client.go "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\nexport GO111MODULE=on\ncd /go/src/google.golang.org/grpc/interop/server\nif [ -x /go/bin/server ]; then\n  exec /go/bin/server "$@"\nfi\nexec go run server.go "$@"\n' > /interop/server \
  && printf '#!/bin/bash\nset -e\nexport GO111MODULE=on\ncd /go/src/google.golang.org/grpc/interop/http2\nif [ -x /go/bin/negative_http2_client ]; then\n  exec /go/bin/negative_http2_client "$@"\nfi\nexec go run negative_http2_client.go "$@"\n' > /interop/http2_client \
  && chmod +x /interop/client /interop/server /interop/http2_client
