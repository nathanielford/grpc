# Copyright 2019 The gRPC Authors
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
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base

# needed by get-dotnet.sh script
RUN apt-get update && apt-get install -y curl git jq && apt-get clean

RUN git config --global --add safe.directory '*'
RUN git config --global protocol.file.allow always

# Define the default command.
CMD ["bash"]
#=== INTEROP_BUILD_STAGE ===
FROM ${BASE_IMAGE} AS interop
ARG RELEASE_LABEL=""
LABEL release="${RELEASE_LABEL}"
COPY --from=service_account . /root/service_account/
COPY --from=build_info . /var/local/build_info/
COPY --from=grpc_dotnet . /var/local/git/grpc-dotnet
RUN cd /var/local/git/grpc-dotnet \
  && ./build/get-dotnet.sh \
  && if [ -f "$(pwd)/.dotnet/dotnet" ]; then ln -sf "$(pwd)/.dotnet/dotnet" /usr/local/bin/dotnet; fi \
  && dotnet build --configuration Debug --output ./output/InteropTestsWebsite testassets/InteropTestsWebsite/InteropTestsWebsite.csproj -p:RepositoryUrl=https://github.com/grpc/grpc-dotnet.git -p:LatestFramework=true \
  && dotnet build --configuration Debug --output ./output/InteropTestsClient testassets/InteropTestsClient/InteropTestsClient.csproj -p:RepositoryUrl=https://github.com/grpc/grpc-dotnet.git -p:LatestFramework=true
USER root
RUN mkdir -p /interop \
  && printf '#!/bin/bash\ncd /var/local/git/grpc-dotnet/output/InteropTestsClient && exec dotnet exec InteropTestsClient.dll "$@"\n' > /interop/client \
  && printf '#!/bin/bash\ncd /var/local/git/grpc-dotnet/output/InteropTestsWebsite && exec dotnet exec InteropTestsWebsite.dll "$@"\n' > /interop/server \
  && chmod +x /interop/client /interop/server
