// Copyright 2024 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Declarative Docker Buildx Bake specification for all gRPC interop images.
// Usage:
//   docker buildx bake -f tools/interop_bake/docker-bake.hcl [target...]

variable "GRPC_ROOT" {
  default = "."
}

variable "GRPC_JAVA_ROOT" {
  default = "../grpc-java"
}

variable "GRPC_GO_ROOT" {
  default = "../grpc-go"
}

variable "GRPC_DART_ROOT" {
  default = "../grpc-dart"
}

variable "GRPC_NODE_ROOT" {
  default = "../grpc-node"
}

variable "GRPC_DOTNET_ROOT" {
  default = "../grpc-dotnet"
}

variable "SERVICE_ACCOUNT_DIR" {
  default = "."
}

variable "BUILD_INFO_DIR" {
  default = "."
}

variable "IMAGE_PREFIX" {
  default = ""
}

variable "TAG_SUFFIX" {
  default = "latest"
}

variable "RELEASE" {
  default = "master"
}

variable "BASE_IMAGE_ASPNETCORE" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_aspnetcore:348fd005a7e7e5f76598ca7e4457faef98df178a@sha256:aedf6ba703f9e73bb072fedb6805c96c4db255e6917d22353383d287455f4e14"
}

variable "BASE_IMAGE_CXX" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_cxx:5ac4662c6625d1869fa29c987b528acf06eed273@sha256:3e6016519cc5d5b5fcd67eb570d6d56da739164dbc7604f55d914ef7546fdad5"
}

variable "BASE_IMAGE_DART" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_dart:513d0c3303b1ed00c5ac541d69200779f71c1ea9@sha256:0500c2bc2d7f4241b92089df75e82608f22b3ff4274382307c837c8828a730ef"
}

variable "BASE_IMAGE_GO" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go:9ce7edfe9ec05b0153e36a66b6936670d87a30cd@sha256:61a86fb789e99b9eec2782638124fd6ed40de3f1d4b7b243cc92a437a23f2f7f"
}

variable "BASE_IMAGE_GO1_8" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.8:2774616b814793b38eb3314efa6285faeef07034@sha256:7830a301b37539252c592b9cd7fa30a6142d0afc717a05fc8d2b82c74fb45efe"
}

variable "BASE_IMAGE_GO1_11" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.11:0c99197e434df9faf7e57c5e3bd9cde30fbe9108@sha256:597b7577008299de519b3e33dd31480b210fb5d6372f5c91a67b416e1e87ebee"
}

variable "BASE_IMAGE_GO1_16" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.16:6f6e1f042e13697deb9d08ef14ca377899415f8b@sha256:eade2ade2831197fe2e894d537ca4ddbdcd6ae5eb7b6efb00ae7f4b8b92efb48"
}

variable "BASE_IMAGE_GO1_19" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.19:47292427457f8e7c7c0143485e355a8f09cc9bd3@sha256:fb4c87752e6b1eafa6fb57b5a8d7f7de813d3207ebe378c2d5685208739a9f7f"
}

variable "BASE_IMAGE_GO1_X" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.x:a79705ce331cfc3628b35356bfc71208b2097760@sha256:61a86fb789e99b9eec2782638124fd6ed40de3f1d4b7b243cc92a437a23f2f7f"
}

variable "BASE_IMAGE_HTTP2" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_http2:688989f016e16178b61ce842aa599c731688b09d@sha256:9105057ebfd4902c0b85ed7fe302d877222afdb2c4994d831affc5f76aa10fcd"
}

variable "BASE_IMAGE_JAVA" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_java:84573d964578d9d9db8a30ecf41a9f7cedaa5ff6@sha256:91ff5d28c2de63eaf6ee121429e7a7b269c83110833a5e2a9a46a2954dcbfb4a"
}

variable "BASE_IMAGE_NODE" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_node:e9d71413f38cdbada670d38963775a85beb0d9fd@sha256:c30e8af219aae32f5c682c1027dfceaa489ea412613b151cc351e4c6daa3bb69"
}

variable "BASE_IMAGE_NODEPUREJS" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_nodepurejs:70bfdfddedfd6ba366c9e2b4c80255d23a6e694f@sha256:b990c14434ed85caede949170c2f917085166874de34410be2452b9447e42371"
}

variable "BASE_IMAGE_PHP7" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_php7:003b6f3b00799b8296a71f9c7df89d48830d8ad0@sha256:3377c55c0a697f20e8391c49bffea5396cc6f7cffa86671ea1efbc32191f6934"
}

variable "BASE_IMAGE_PYTHON" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_python:9d1379f623e15c84d49a039b481efee71db96091@sha256:ca2dfcb62ae1d4011b0c5398a5ffaac89fd7e694d6accd240e0d1399eb93786e"
}

variable "BASE_IMAGE_PYTHONASYNCIO" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_pythonasyncio:bf6ebd12729cf6e0371eb615b02f3d4f1b735cbd@sha256:e3fcb433d1600597109bff57a353c2c0935143788b011a75a7bf2e6b69c70872"
}

variable "BASE_IMAGE_RUBY" {
  default = "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_ruby:8d144da45535336dea5f8e6c98fc184105a312ab@sha256:1b4160c0bf66ba79bc423e4bb75e2e8945162287ed73895b51d5cd0c0c522616"
}

group "default" {
  targets = [
    "cxx",
    "aspnetcore",
    "dart",
    "go",
    "java",
    "node",
    "nodepurejs",
    "php7",
    "python",
    "pythonasyncio",
    "ruby",
    "http2",
  ]
}

group "matrix" {
  targets = [
    "cxx",
    "go1_8",
    "go1_11",
    "go1_16",
    "go1_19",
    "go1_x",
    "java",
    "node",
    "php7",
    "python",
    "pythonasyncio",
    "ruby",
  ]
}

group "alts" {
  targets = [
    "cxx",
    "go",
    "java",
    "python",
  ]
}

target "_common" {
  context = "${GRPC_ROOT}"
  dockerfile = "tools/interop_bake/Interop.Dockerfile"
  contexts = {
    grpc            = "dir:${GRPC_ROOT}"
    grpc_java       = "dir:${GRPC_JAVA_ROOT}"
    grpc_go         = "dir:${GRPC_GO_ROOT}"
    grpc_dart       = "dir:${GRPC_DART_ROOT}"
    grpc_node       = "dir:${GRPC_NODE_ROOT}"
    grpc_dotnet     = "dir:${GRPC_DOTNET_ROOT}"
    service_account = "dir:${SERVICE_ACCOUNT_DIR}"
    build_info      = "dir:${BUILD_INFO_DIR}"
  }
  output = ["type=docker"]
}

target "cxx" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_CXX}"
    BASE_NAME  = "grpc_interop_cxx"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_cxx:${TAG_SUFFIX}"]
}

target "aspnetcore" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_ASPNETCORE}"
    BASE_NAME  = "grpc_interop_aspnetcore"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_aspnetcore:${TAG_SUFFIX}"]
}

target "dart" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_DART}"
    BASE_NAME  = "grpc_interop_dart"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_dart:${TAG_SUFFIX}"]
}

target "go" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_GO}"
    BASE_NAME  = "grpc_interop_go"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_go:${TAG_SUFFIX}"]
}

target "go1_8" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_GO1_8}"
    BASE_NAME  = "grpc_interop_go1.8"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_go1.8:${TAG_SUFFIX}"]
}

target "go1_11" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_GO1_11}"
    BASE_NAME  = "grpc_interop_go1.11"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_go1.11:${TAG_SUFFIX}"]
}

target "go1_16" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_GO1_16}"
    BASE_NAME  = "grpc_interop_go1.16"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_go1.16:${TAG_SUFFIX}"]
}

target "go1_19" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_GO1_19}"
    BASE_NAME  = "grpc_interop_go1.19"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_go1.19:${TAG_SUFFIX}"]
}

target "go1_x" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_GO1_X}"
    BASE_NAME  = "grpc_interop_go1.x"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_go1.x:${TAG_SUFFIX}"]
}

target "http2" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_HTTP2}"
    BASE_NAME  = "grpc_interop_http2"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_http2:${TAG_SUFFIX}"]
}

target "java" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_JAVA}"
    BASE_NAME  = "grpc_interop_java"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_java:${TAG_SUFFIX}"]
}

target "node" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_NODE}"
    BASE_NAME  = "grpc_interop_node"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_node:${TAG_SUFFIX}"]
}

target "nodepurejs" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_NODEPUREJS}"
    BASE_NAME  = "grpc_interop_nodepurejs"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_nodepurejs:${TAG_SUFFIX}"]
}

target "php7" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_PHP7}"
    BASE_NAME  = "grpc_interop_php7"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_php7:${TAG_SUFFIX}"]
}

target "python" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_PYTHON}"
    BASE_NAME  = "grpc_interop_python"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_python:${TAG_SUFFIX}"]
}

target "pythonasyncio" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_PYTHONASYNCIO}"
    BASE_NAME  = "grpc_interop_pythonasyncio"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_pythonasyncio:${TAG_SUFFIX}"]
}

target "ruby" {
  inherits = ["_common"]
  args = {
    BASE_IMAGE = "${BASE_IMAGE_RUBY}"
    BASE_NAME  = "grpc_interop_ruby"
    RELEASE    = "${RELEASE}"
  }
  tags = ["${IMAGE_PREFIX}grpc_interop_ruby:${TAG_SUFFIX}"]
}
