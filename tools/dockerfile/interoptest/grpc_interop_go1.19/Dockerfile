# Copyright 2021 The gRPC Authors
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
FROM golang:1.19 AS base

# Using login shell removes Go from path, so we add it.
RUN ln -s /usr/local/go/bin/go /usr/local/bin

#=================
# Setup git to access working directory across docker boundary.
# This avoids the "fatal: detected dubious ownership in repository XYZ"
# git error.

RUN git config --global --add safe.directory '*'
RUN git config --global protocol.file.allow always

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
# Some newer distros that have already implemented PEP 668. Use of
# --break-system-packages is to workaround that. We should look into using
# virtualenv in Dockerfile though.
RUN python3 -m pip install --break-system-packages --upgrade pip==19.3.1

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
ENV GO111MODULE=on
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
COPY --from=grpc_go . /go/src/google.golang.org/grpc
RUN (cd /go/src/google.golang.org/grpc/interop/client && go install) \
  && (cd /go/src/google.golang.org/grpc/interop/server && go install) \
  && if [ -d /go/src/google.golang.org/grpc/interop/http2 ]; then (cd /go/src/google.golang.org/grpc/interop/http2 && go build -o /go/bin/negative_http2_client negative_http2_client.go) || true; fi \
  && mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\ncd /go/src/google.golang.org/grpc/interop/client\nif [ -x /go/bin/client ]; then exec /go/bin/client "$@"; fi\nexec go run client.go "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\ncd /go/src/google.golang.org/grpc/interop/server\nif [ -x /go/bin/server ]; then exec /go/bin/server "$@"; fi\nexec go run server.go "$@"\n' > /interop/server \
  && printf '#!/bin/bash\nset -e\ncd /go/src/google.golang.org/grpc/interop/http2\nif [ -x /go/bin/negative_http2_client ]; then exec /go/bin/negative_http2_client "$@"; fi\nexec go run negative_http2_client.go "$@"\n' > /interop/http2_client \
  && chmod +x /interop/client /interop/server /interop/http2_client
