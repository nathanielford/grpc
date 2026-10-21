# Copyright 2024 gRPC authors.
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
"""Declarative Docker Buildx Bake manifest generator and image deduplicator."""

from collections import OrderedDict
import json
import os
import subprocess
import tempfile
import uuid

DEFAULT_BASE_IMAGES = {
    "grpc_interop_aspnetcore": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_aspnetcore:348fd005a7e7e5f76598ca7e4457faef98df178a"
        "@sha256:aedf6ba703f9e73bb072fedb6805c96c4db255e6917d22353383d287455f4e14"
    ),
    "grpc_interop_cxx": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_cxx:5ac4662c6625d1869fa29c987b528acf06eed273"
        "@sha256:3e6016519cc5d5b5fcd67eb570d6d56da739164dbc7604f55d914ef7546fdad5"
    ),
    "grpc_interop_dart": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_dart:513d0c3303b1ed00c5ac541d69200779f71c1ea9"
        "@sha256:0500c2bc2d7f4241b92089df75e82608f22b3ff4274382307c837c8828a730ef"
    ),
    "grpc_interop_go": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_go:9ce7edfe9ec05b0153e36a66b6936670d87a30cd"
        "@sha256:61a86fb789e99b9eec2782638124fd6ed40de3f1d4b7b243cc92a437a23f2f7f"
    ),
    "grpc_interop_go1.8": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_go1.8:2774616b814793b38eb3314efa6285faeef07034"
        "@sha256:7830a301b37539252c592b9cd7fa30a6142d0afc717a05fc8d2b82c74fb45efe"
    ),
    "grpc_interop_go1.11": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_go1.11:0c99197e434df9faf7e57c5e3bd9cde30fbe9108"
        "@sha256:597b7577008299de519b3e33dd31480b210fb5d6372f5c91a67b416e1e87ebee"
    ),
    "grpc_interop_go1.16": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_go1.16:6f6e1f042e13697deb9d08ef14ca377899415f8b"
        "@sha256:eade2ade2831197fe2e894d537ca4ddbdcd6ae5eb7b6efb00ae7f4b8b92efb48"
    ),
    "grpc_interop_go1.19": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_go1.19:47292427457f8e7c7c0143485e355a8f09cc9bd3"
        "@sha256:fb4c87752e6b1eafa6fb57b5a8d7f7de813d3207ebe378c2d5685208739a9f7f"
    ),
    "grpc_interop_go1.x": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_go1.x:a79705ce331cfc3628b35356bfc71208b2097760"
        "@sha256:61a86fb789e99b9eec2782638124fd6ed40de3f1d4b7b243cc92a437a23f2f7f"
    ),
    "grpc_interop_http2": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_http2:688989f016e16178b61ce842aa599c731688b09d"
        "@sha256:9105057ebfd4902c0b85ed7fe302d877222afdb2c4994d831affc5f76aa10fcd"
    ),
    "grpc_interop_java": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_java:84573d964578d9d9db8a30ecf41a9f7cedaa5ff6"
        "@sha256:91ff5d28c2de63eaf6ee121429e7a7b269c83110833a5e2a9a46a2954dcbfb4a"
    ),
    "grpc_interop_node": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_node:e9d71413f38cdbada670d38963775a85beb0d9fd"
        "@sha256:c30e8af219aae32f5c682c1027dfceaa489ea412613b151cc351e4c6daa3bb69"
    ),
    "grpc_interop_nodepurejs": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_nodepurejs:70bfdfddedfd6ba366c9e2b4c80255d23a6e694f"
        "@sha256:b990c14434ed85caede949170c2f917085166874de34410be2452b9447e42371"
    ),
    "grpc_interop_php7": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_php7:003b6f3b00799b8296a71f9c7df89d48830d8ad0"
        "@sha256:3377c55c0a697f20e8391c49bffea5396cc6f7cffa86671ea1efbc32191f6934"
    ),
    "grpc_interop_python": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_python:9d1379f623e15c84d49a039b481efee71db96091"
        "@sha256:ca2dfcb62ae1d4011b0c5398a5ffaac89fd7e694d6accd240e0d1399eb93786e"
    ),
    "grpc_interop_pythonasyncio": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_pythonasyncio:bf6ebd12729cf6e0371eb615b02f3d4f1b735cbd"
        "@sha256:e3fcb433d1600597109bff57a353c2c0935143788b011a75a7bf2e6b69c70872"
    ),
    "grpc_interop_ruby": (
        "us-docker.pkg.dev/grpc-testing/testing-images-public/"
        "grpc_interop_ruby:8d144da45535336dea5f8e6c98fc184105a312ab"
        "@sha256:1b4160c0bf66ba79bc423e4bb75e2e8945162287ed73895b51d5cd0c0c522616"
    ),
}

LANGUAGE_TO_SAFENAME = {
    "c++": "cxx",
    "cxx": "cxx",
    "aspnetcore": "aspnetcore",
    "dart": "dart",
    "go": "go",
    "go1.8": "go1.8",
    "go1.11": "go1.11",
    "go1.16": "go1.16",
    "go1.19": "go1.19",
    "go1.x": "go1.x",
    "java": "java",
    "javaokhttp": "java",
    "java_oracle8": "java_oracle8",
    "node": "node",
    "nodepurejs": "nodepurejs",
    "php7": "php7",
    "php": "php7",
    "python": "python",
    "pythonasyncio": "pythonasyncio",
    "ruby": "ruby",
    "http2": "http2",
    "objc": "objc",
    "csharp": "csharp",
    "csharpcoreclr": "csharpcoreclr",
}


def safename_for_language(lang_or_obj):
    """Returns canonical safename for a language object or string."""
    if hasattr(lang_or_obj, "safename"):
        return lang_or_obj.safename
    name = str(lang_or_obj)
    return LANGUAGE_TO_SAFENAME.get(name, name)


def safename_to_bake_target(safename):
    """Converts a safename (e.g. 'go1.8') to a valid HCL target name ('go1_8')."""
    return safename.replace(".", "_").replace("+", "x").replace("-", "_")


def read_current_version(grpc_root, base_name):
    """Reads pinned base image reference from tools/dockerfile/interoptest/<base_name>.current_version."""
    version_file = os.path.join(
        grpc_root,
        "tools",
        "dockerfile",
        "interoptest",
        "%s.current_version" % base_name,
    )
    if os.path.exists(version_file):
        with open(version_file, "r") as f:
            content = f.read().strip()
            if content:
                return content
    return DEFAULT_BASE_IMAGES.get(base_name, "%s:latest" % base_name)


def deduplicate_language_builds(languages_to_build, tag_generator=None):
    """Deduplicates requested languages by safename so shared images build once.

    For example:
    - `java` and `javaokhttp` both resolve to `safename='java'` (`grpc_interop_java:<tag>`).
    - `Http2Client` and `Http2Server` both resolve to `safename='http2'` (`grpc_interop_http2:<tag>`).
    - `objc` is host-only on macOS and is excluded from Docker image builds.

    Returns:
      (safename_to_tag, language_to_tag, safename_to_languages)
    """
    if tag_generator is None:
        tag_generator = lambda safename: "grpc_interop_%s:%s" % (
            safename,
            uuid.uuid4(),
        )

    safename_to_languages = OrderedDict()
    sorted_items = sorted(
        languages_to_build,
        key=lambda x: (safename_for_language(x), str(x)),
    )
    for item in sorted_items:
        lang_name = str(item)
        safename = safename_for_language(item)
        if safename == "objc" or lang_name == "objc":
            continue
        if safename not in safename_to_languages:
            safename_to_languages[safename] = []
        if lang_name not in safename_to_languages[safename]:
            safename_to_languages[safename].append(lang_name)

    safename_to_tag = OrderedDict()
    language_to_tag = {}
    for safename, lang_names in safename_to_languages.items():
        tag = tag_generator(safename)
        safename_to_tag[safename] = tag
        for lang_name in lang_names:
            language_to_tag[lang_name] = tag

    return safename_to_tag, language_to_tag, safename_to_languages


def resolve_repo_contexts(grpc_root, env=None, fallback_empty_dir=None):
    """Resolves local repository context directories for Buildx Bake."""
    env = dict(os.environ if env is None else env)
    grpc_root = os.path.abspath(env.get("GRPC_ROOT", grpc_root))
    parent_dir = os.path.dirname(grpc_root)

    if fallback_empty_dir is None:
        fallback_empty_dir = os.path.join(
            tempfile.gettempdir(), "grpc_interop_empty_context"
        )
        os.makedirs(fallback_empty_dir, exist_ok=True)

    def _resolve_dir(env_key, default_path):
        candidate = env.get(env_key) or default_path
        if candidate and os.path.isdir(candidate):
            return os.path.abspath(candidate)
        return os.path.abspath(fallback_empty_dir)

    home_dir = env.get("HOME", os.path.expanduser("~"))
    return OrderedDict(
        [
            ("grpc", grpc_root),
            (
                "grpc_java",
                _resolve_dir(
                    "GRPC_JAVA_ROOT", os.path.join(parent_dir, "grpc-java")
                ),
            ),
            (
                "grpc_go",
                _resolve_dir(
                    "GRPC_GO_ROOT", os.path.join(parent_dir, "grpc-go")
                ),
            ),
            (
                "grpc_dart",
                _resolve_dir(
                    "GRPC_DART_ROOT", os.path.join(parent_dir, "grpc-dart")
                ),
            ),
            (
                "grpc_node",
                _resolve_dir(
                    "GRPC_NODE_ROOT", os.path.join(parent_dir, "grpc-node")
                ),
            ),
            (
                "grpc_dotnet",
                _resolve_dir(
                    "GRPC_DOTNET_ROOT", os.path.join(parent_dir, "grpc-dotnet")
                ),
            ),
            (
                "service_account",
                _resolve_dir(
                    "SERVICE_ACCOUNT_DIR",
                    os.path.join(home_dir, "service_account"),
                ),
            ),
            (
                "build_info",
                _resolve_dir("BUILD_INFO_DIR", fallback_empty_dir),
            ),
        ]
    )


def generate_bake_hcl(
    safename_to_tag,
    grpc_root,
    env=None,
    release="master",
    extra_tags_by_safename=None,
    dockerfile_relpath="tools/interop_bake/Interop.Dockerfile",
):
    """Generates a complete, self-contained docker-bake.hcl configuration."""
    contexts = resolve_repo_contexts(grpc_root, env=env)
    extra_tags_by_safename = extra_tags_by_safename or {}
    targets = [safename_to_bake_target(s) for s in safename_to_tag.keys()]

    lines = [
        "// Generated by tools/interop_bake/bake_manifest.py",
        "// Declarative Docker Buildx Bake file for gRPC interop images",
        "",
        'variable "GRPC_ROOT" {',
        '  default = %s' % json.dumps(contexts["grpc"]),
        "}",
        "",
        'variable "GRPC_JAVA_ROOT" {',
        '  default = %s' % json.dumps(contexts["grpc_java"]),
        "}",
        "",
        'variable "GRPC_GO_ROOT" {',
        '  default = %s' % json.dumps(contexts["grpc_go"]),
        "}",
        "",
        'variable "GRPC_DART_ROOT" {',
        '  default = %s' % json.dumps(contexts["grpc_dart"]),
        "}",
        "",
        'variable "GRPC_NODE_ROOT" {',
        '  default = %s' % json.dumps(contexts["grpc_node"]),
        "}",
        "",
        'variable "GRPC_DOTNET_ROOT" {',
        '  default = %s' % json.dumps(contexts["grpc_dotnet"]),
        "}",
        "",
        'variable "SERVICE_ACCOUNT_DIR" {',
        '  default = %s' % json.dumps(contexts["service_account"]),
        "}",
        "",
        'variable "BUILD_INFO_DIR" {',
        '  default = %s' % json.dumps(contexts["build_info"]),
        "}",
        "",
        'variable "RELEASE" {',
        '  default = %s' % json.dumps(release),
        "}",
        "",
        'group "default" {',
        "  targets = [%s]" % ", ".join(json.dumps(t) for t in targets),
        "}",
        "",
        'target "_common" {',
        '  context = "${GRPC_ROOT}"',
        "  dockerfile = %s" % json.dumps(dockerfile_relpath),
        "  contexts = {",
        '    grpc            = "dir:${GRPC_ROOT}"',
        '    grpc_java       = "dir:${GRPC_JAVA_ROOT}"',
        '    grpc_go         = "dir:${GRPC_GO_ROOT}"',
        '    grpc_dart       = "dir:${GRPC_DART_ROOT}"',
        '    grpc_node       = "dir:${GRPC_NODE_ROOT}"',
        '    grpc_dotnet     = "dir:${GRPC_DOTNET_ROOT}"',
        '    service_account = "dir:${SERVICE_ACCOUNT_DIR}"',
        '    build_info      = "dir:${BUILD_INFO_DIR}"',
        "  }",
        '  output = ["type=docker"]',
        "}",
    ]

    for safename, primary_tag in safename_to_tag.items():
        target_name = safename_to_bake_target(safename)
        base_name = "grpc_interop_%s" % safename
        base_image = read_current_version(grpc_root, base_name)
        all_tags = [primary_tag] + list(extra_tags_by_safename.get(safename, []))
        lines.extend(
            [
                "",
                'target %s {' % json.dumps(target_name),
                '  inherits = ["_common"]',
                "  args = {",
                "    BASE_IMAGE = %s" % json.dumps(base_image),
                "    BASE_NAME  = %s" % json.dumps(base_name),
                '    RELEASE    = "${RELEASE}"',
                "  }",
                "  tags = [%s]" % ", ".join(json.dumps(t) for t in all_tags),
                "}",
            ]
        )

    lines.append("")
    return "\n".join(lines)


def is_buildx_bake_available(env=None):
    """Returns True if `docker buildx bake` is available and not disabled."""
    check_env = os.environ if env is None else env
    if check_env.get("GRPC_INTEROP_DISABLE_BAKE", "") in ("1", "true", "TRUE"):
        return False
    if check_env.get("GRPC_INTEROP_FORCE_BAKE", "") in ("1", "true", "TRUE"):
        return True
    try:
        with open(os.devnull, "w") as devnull:
            return (
                subprocess.call(
                    ["docker", "buildx", "bake", "--help"],
                    stdout=devnull,
                    stderr=devnull,
                )
                == 0
            )
    except OSError:
        return False
