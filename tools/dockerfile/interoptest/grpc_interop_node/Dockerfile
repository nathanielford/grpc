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
RUN cd /home/appuser/grpc-node && ./setup_interop.sh
USER root
RUN mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\ncd /home/appuser/grpc-node\nexec packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh node --require ./test/fixtures/native_native test/interop/interop_client.js "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\ncd /home/appuser/grpc-node\nexec packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh node --require ./test/fixtures/native_native test/interop/interop_server.js "$@"\n' > /interop/server \
  && chmod +x /interop/client /interop/server \
  && chown -R appuser:appuser /interop
USER appuser
