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
FROM debian:bullseye AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless && \
    apt-get clean

WORKDIR /var/local/git/grpc-java/

# Define the default command.
CMD ["bash"]

#=== INTEROP_BUILD_STAGE ===
FROM ${BASE_IMAGE} AS interop
ARG RELEASE_LABEL=""
LABEL release="${RELEASE_LABEL}"
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
RUN --mount=type=bind,from=grpc_java,target=/var/local/jenkins/grpc-java \
  cp -r /var/local/jenkins/grpc-java /tmp/grpc-java \
  && export GRADLE_OPTS="-Dorg.gradle.jvmargs='-Xmx1g'" \
  && cd /tmp/grpc-java \
  && (./gradlew --no-daemon :grpc-interop-testing:installDist -PskipCodegen=true -PskipAndroid=true || \
      ./gradlew --no-daemon :grpc-interop-testing:installDist -PskipCodegen=true -PskipAndroid=true) \
  && mkdir -p /var/local/git/grpc-java/ \
  && cp -r --parents -t /var/local/git/grpc-java/ \
      interop-testing/build/install/ \
      run-test-client.sh \
      run-test-server.sh \
  && rm -rf /tmp/grpc-java "$HOME/.gradle" \
  && mkdir -p /var/local/grpc_java_logging \
  && printf "handlers = java.util.logging.ConsoleHandler\njava.util.logging.ConsoleHandler.level = ALL\n.level = FINE\nio.grpc.netty.NettyClientHandler = ALL\nio.grpc.netty.NettyServerHandler = ALL\n" > /var/local/grpc_java_logging/logconf.txt

RUN mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-java\nexec ./run-test-client.sh "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-java\nexec ./run-test-server.sh "$@"\n' > /interop/server \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-java\nexec ./interop-testing/build/install/grpc-interop-testing/bin/http2-client "$@"\n' > /interop/http2_client \
  && chmod +x /interop/*
