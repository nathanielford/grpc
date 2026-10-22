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

# Updated: 2025-11-18

ARG BASE_IMAGE=base
FROM dart:3.10.0-sdk AS base

# Define the default command.
CMD ["bash"]
#=== INTEROP_BUILD_STAGE ===
FROM ${BASE_IMAGE} AS interop
ARG RELEASE_LABEL=""
LABEL release="${RELEASE_LABEL}"
LABEL io.grpc.interop.release="${RELEASE_LABEL}"
LABEL io.grpc.interop.client_cmd="[\"dart\", \"bin/client.dart\"]"
LABEL io.grpc.interop.server_cmd="[\"dart\", \"bin/server.dart\"]"
LABEL io.grpc.interop.http2_client_cmd="[]"
LABEL io.grpc.interop.workdir="/var/local/git/grpc/../grpc-dart/interop"
LABEL io.grpc.interop.env="{}"
LABEL io.grpc.interop.capabilities="core_interop,timeout_on_sleeping_server,tls"
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
COPY --from=grpc_dart . /var/local/git/grpc-dart
RUN cd /var/local/git/grpc-dart/interop \
  && (/usr/lib/dart/bin/dart pub get --verbose || /usr/lib/dart/bin/dart pub get --verbose)
RUN mkdir -p /interop \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-dart/interop\nexec dart bin/client.dart "$@"\n' > /interop/client \
  && printf '#!/bin/bash\nset -e\ncd /var/local/git/grpc-dart/interop\nexec dart bin/server.dart "$@"\n' > /interop/server \
  && chmod +x /interop/client /interop/server
