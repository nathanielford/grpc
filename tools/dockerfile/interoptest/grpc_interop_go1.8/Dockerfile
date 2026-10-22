# Copyright 2017 gRPC authors.
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
FROM golang:1.8 AS base

# Using login shell removes Go from path, so we add it.
RUN ln -s /usr/local/go/bin/go /usr/local/bin

#====================
# run_tests.py python dependencies

# Basic python dependencies to be able to run tools/run_tests python scripts
# These dependencies are not sufficient to build gRPC Python, gRPC Python
# deps are defined elsewhere (e.g. python_deps.include)
RUN apt-get update && apt-get install -y \
  python3 \
  python3-pip \
  python3-setuptools \
  python3-yaml \
  && apt-get clean

# use pinned version of pip to avoid sudden breakages
RUN python3 -m pip install --upgrade pip==19.3.1

# TODO(jtattermusch): currently six is needed for tools/run_tests scripts
# but since our python2 usage is deprecated, we should get rid of it.
RUN python3 -m pip install six==1.16.0

# Google Cloud Platform API libraries
# These are needed for uploading test results to BigQuery (e.g. by tools/run_tests scripts)
RUN python3 -m pip install --upgrade google-auth==1.23.0 google-api-python-client==1.12.8 oauth2client==4.1.0


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
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
COPY --from=grpc_go . /go/src/google.golang.org/grpc
RUN (cd /go/src/google.golang.org/grpc && make deps && make testdeps) \
  && (cd /go/src/google.golang.org/grpc/interop/client && go install) \
  && (cd /go/src/google.golang.org/grpc/interop/server && go install)
  && if [ -d /go/src/google.golang.org/grpc/interop/http2 ]; then (cd /go/src/google.golang.org/grpc/interop/http2 && go build -o /go/bin/negative_http2_client negative_http2_client.go) || true; fi
RUN mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\ncd /go/src/google.golang.org/grpc/interop/client\nif [ -x /go/bin/client ]; then\n  exec /go/bin/client "$@"\nfi\nexec go run client.go "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\ncd /go/src/google.golang.org/grpc/interop/server\nif [ -x /go/bin/server ]; then\n  exec /go/bin/server "$@"\nfi\nexec go run server.go "$@"\n' > /interop/server \
  && printf '#!/bin/bash\nset -e\ncd /go/src/google.golang.org/grpc/interop/http2\nif [ -x /go/bin/negative_http2_client ]; then\n  exec /go/bin/negative_http2_client "$@"\nfi\nexec go run negative_http2_client.go "$@"\n' > /interop/http2_client \
  && chmod +x /interop/client /interop/server /interop/http2_client
