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

ARG BASE_IMAGE=base
FROM debian:11 AS base

#=================
# Basic C core dependencies

# C/C++ dependencies according to https://github.com/grpc/grpc/blob/master/BUILDING.md
RUN apt-get update && apt-get install -y \
  build-essential \
  autoconf \
  libtool \
  pkg-config \
  && apt-get clean

# GCC
RUN apt-get update && apt-get install -y \
  gcc \
  g++ \
  && apt-get clean

# libc6
RUN apt-get update && apt-get install -y \
  libc6 \
  libc6-dbg \
  libc6-dev \
  && apt-get clean

# Tools
RUN apt-get update && apt-get install -y \
  bzip2 \
  curl \
  dnsutils \
  git \
  lcov \
  make \
  strace \
  time \
  unzip \
  wget \
  zip \
  && apt-get clean

#=================
# Setup git to access working directory across docker boundary.
# This avoids the "fatal: detected dubious ownership in repository XYZ"
# git error.

RUN git config --global --add safe.directory '*'
RUN git config --global protocol.file.allow always



RUN mkdir /var/local/jenkins

#==================
# Node dependencies

# Install nvm
RUN groupadd -g 999 appuser && useradd -r -u 999 -g appuser appuser
RUN mkdir -p /home/appuser && chown appuser /home/appuser
RUN chmod 777 /root
USER appuser
RUN touch ~/.profile
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
# Install all versions of node that we want to test
RUN /bin/bash -l -c "nvm install 16 && npm config set cache /tmp/npm-cache"
RUN /bin/bash -l -c "nvm alias default 16"

# Include the git workaround also for the user "appuser" we just created.
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
COPY --from=service_account --chown=appuser:appuser . /root/service_account/
COPY --from=build_info --chown=appuser:appuser . /var/local/build_info/
COPY --from=grpc_node --chown=appuser:appuser . /home/appuser/grpc-node
RUN /bin/bash -l -c "cd /home/appuser/grpc-node && (if [ -d .git ]; then git submodule update --init --recursive || true; fi) && ./setup_interop_purejs.sh"
USER root
RUN mkdir -p /interop \
  && printf '#!/bin/bash -l\nset -e\ncd /home/appuser/grpc-node\nexec packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh node --require ./test/fixtures/js_js test/interop/interop_client.js "$@"\n' > /interop/client \
  && chmod +x /interop/client \
  && chown -R appuser:appuser /interop
USER appuser
