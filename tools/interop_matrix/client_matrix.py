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

# Defines languages, runtimes and releases for backward compatibility testing

from collections import OrderedDict
from collections import namedtuple

INTEROP_MATRIX_TEST_CASES = [
    "large_unary",
    "empty_unary",
    "ping_pong",
    "empty_stream",
    "client_streaming",
    "server_streaming",
    "cancel_after_begin",
    "cancel_after_first_response",
    "timeout_on_sleeping_server",
]

INTEROP_MATRIX_SERVERS = [
    ("grpc-test", "grpc-test.sandbox.googleapis.com"),
    ("grpc-test4", "grpc-test4.sandbox.googleapis.com"),
]

MatrixClientSpec = namedtuple(
    "MatrixClientSpec",
    [
        "workdir",
        "env",
        "shell",
        "client_prefix",
        "flag_order",
        "args_wrapper",
    ],
)

_LEGACY_FLAG_ORDER = (
    "server_host",
    "server_host_override",
    "server_port",
    "use_tls",
    "test_case",
)
_MODERN_FLAG_ORDER = (
    "server_host",
    "server_host_override",
    "server_port",
    "test_case",
    "use_tls",
)


def parse_release_version(release):
    """Parse a release tag (e.g. 'v1.32.0', 'v1.0.x', 'master') into a comparable tuple."""
    if not release or release == "master":
        return (999, 999, 999)
    ver = release.lstrip("v")
    parts = []
    for part in ver.split("."):
        if part == "x":
            parts.append(0)
        else:
            parts.append(int(part))
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])


def resolve_matrix_client_spec(lang, runtime, release="master"):
    """Resolve the legacy container invocation spec for (lang, runtime, release)."""
    ver = parse_release_version(release)

    if lang == "cxx":
        binary = (
            "bins/opt/interop_client"
            if ver <= (1, 31, 1)
            else "cmake/build/interop_client"
        )
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 19, 0) else _MODERN_FLAG_ORDER
        )
        return MatrixClientSpec(
            workdir="/var/local/git/grpc",
            env=(),
            shell="bash -c",
            client_prefix=binary,
            flag_order=flag_order,
            args_wrapper="direct",
        )

    if lang == "go":
        env = (("GO111MODULE", "on"),) if ver >= (1, 34, 0) else ()
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 19, 0) else _MODERN_FLAG_ORDER
        )
        return MatrixClientSpec(
            workdir="/go/src/google.golang.org/grpc/interop/client",
            env=env,
            shell="bash -c",
            client_prefix="go run client.go",
            flag_order=flag_order,
            args_wrapper="direct",
        )

    if lang == "java":
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 19, 0) else _MODERN_FLAG_ORDER
        )
        return MatrixClientSpec(
            workdir="/var/local/git/grpc/../grpc-java",
            env=(),
            shell="bash -c",
            client_prefix="./run-test-client.sh",
            flag_order=flag_order,
            args_wrapper="direct",
        )

    if lang == "python":
        if ver <= (1, 10, 1):
            py_bin = "py27/bin/python"
        elif ver <= (1, 17, 1):
            py_bin = "py27_native/bin/python"
        elif ver <= (1, 39, 0):
            py_bin = "py37_native/bin/python"
        elif ver <= (1, 43, 2):
            py_bin = "py39_native/bin/python"
        else:
            py_bin = "py39/bin/python"
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 17, 1) else _MODERN_FLAG_ORDER
        )
        asyncio_flag = "--use-asyncio " if runtime == "pythonasyncio" else ""
        prefix = (
            f"{py_bin} src/python/grpcio_tests/setup.py run_interop "
            f"{asyncio_flag}--client"
        )
        return MatrixClientSpec(
            workdir="/var/local/git/grpc",
            env=(
                ("PYTHONPATH", "/var/local/git/grpc/src/python/gens"),
                ("LD_LIBRARY_PATH", "/var/local/git/grpc/libs/opt"),
            ),
            shell="bash -c",
            client_prefix=prefix,
            flag_order=flag_order,
            args_wrapper="python_setup",
        )

    if lang == "node":
        if ver <= (1, 0, 1):
            return MatrixClientSpec(
                workdir="/var/local/git/grpc",
                env=(),
                shell="bash -l -c",
                client_prefix="node src/node/interop/interop_client.js",
                flag_order=_LEGACY_FLAG_ORDER,
                args_wrapper="direct",
            )
        if ver <= (1, 6, 6):
            return MatrixClientSpec(
                workdir="/var/local/git/grpc",
                env=(),
                shell="bash -c",
                client_prefix=(
                    "tools/run_tests/interop/with_nvm.sh node"
                    " src/node/interop/interop_client.js"
                ),
                flag_order=_LEGACY_FLAG_ORDER,
                args_wrapper="direct",
            )
        return MatrixClientSpec(
            workdir="/var/local/git/grpc-node",
            env=(),
            shell="bash -c",
            client_prefix=(
                "packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh"
                " node --require ./test/fixtures/native_native"
                " test/interop/interop_client.js"
            ),
            flag_order=_LEGACY_FLAG_ORDER,
            args_wrapper="direct",
        )

    if lang == "ruby":
        if ver <= (1, 0, 1):
            prefix = (
                "source /usr/local/rvm/scripts/rvm && ruby"
                " src/ruby/pb/test/client.rb"
            )
        else:
            prefix = (
                "tools/run_tests/interop/with_rvm.sh ruby"
                " src/ruby/pb/test/client.rb"
            )
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 17, 1) else _MODERN_FLAG_ORDER
        )
        return MatrixClientSpec(
            workdir="/var/local/git/grpc",
            env=(),
            shell="bash -c",
            client_prefix=prefix,
            flag_order=flag_order,
            args_wrapper="direct",
        )

    if lang == "php":
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 17, 1) else _MODERN_FLAG_ORDER
        )
        return MatrixClientSpec(
            workdir="/var/local/git/grpc",
            env=(),
            shell="bash -c",
            client_prefix="src/php/bin/interop_client.sh",
            flag_order=flag_order,
            args_wrapper="direct",
        )

    if lang == "csharp":
        flag_order = (
            _LEGACY_FLAG_ORDER if ver <= (1, 17, 1) else _MODERN_FLAG_ORDER
        )
        if runtime == "csharpcoreclr":
            if ver <= (1, 17, 1):
                tfm = "netcoreapp1.0"
            elif ver <= (1, 19, 0):
                tfm = "netcoreapp1.1"
            elif ver <= (1, 42, 0):
                tfm = "netcoreapp2.1"
            else:
                tfm = "netcoreapp3.1"
            return MatrixClientSpec(
                workdir=(
                    "/var/local/git/grpc/src/csharp/"
                    f"Grpc.IntegrationTesting.Client/bin/Debug/{tfm}"
                ),
                env=(),
                shell="bash -c",
                client_prefix="dotnet exec Grpc.IntegrationTesting.Client.dll",
                flag_order=flag_order,
                args_wrapper="direct",
            )
        subdir = "bin/Debug" if ver <= (1, 2, 5) else "bin/Debug/net45"
        return MatrixClientSpec(
            workdir=(
                "/var/local/git/grpc/src/csharp/"
                f"Grpc.IntegrationTesting.Client/{subdir}"
            ),
            env=(),
            shell="bash -c",
            client_prefix="mono Grpc.IntegrationTesting.Client.exe",
            flag_order=flag_order,
            args_wrapper="direct",
        )

    raise ValueError(f"Unsupported matrix language: {lang}")


def build_matrix_docker_cmdline(
    lang,
    runtime,
    release,
    server_host,
    server_host_override,
    test_case,
    docker_image="$docker_image",
    server_port=443,
    use_tls=True,
    contract_entrypoints=False,
):
    """Build the docker run command string for a matrix client test case."""
    if contract_entrypoints:
        flag_values = {
            "server_host": f"--server_host={server_host}",
            "server_host_override": (
                f"--server_host_override={server_host_override}"
                if server_host_override
                else None
            ),
            "server_port": f"--server_port={server_port}",
            "use_tls": f"--use_tls={'true' if use_tls else 'false'}",
            "test_case": f"--test_case={test_case}",
        }
        flags = [
            flag_values[k]
            for k in _MODERN_FLAG_ORDER
            if flag_values.get(k) is not None
        ]
        return (
            f"docker run -i --rm=true --net=host {docker_image}"
            f" /interop/client {' '.join(flags)}"
        )

    spec = resolve_matrix_client_spec(lang, runtime, release)
    include_override = bool(
        server_host_override
        and (
            server_host != server_host_override
            or spec.flag_order == _LEGACY_FLAG_ORDER
        )
    )
    flag_values = {
        "server_host": f"--server_host={server_host}",
        "server_host_override": (
            f"--server_host_override={server_host_override}"
            if include_override
            else None
        ),
        "server_port": f"--server_port={server_port}",
        "use_tls": f"--use_tls={'true' if use_tls else 'false'}",
        "test_case": f"--test_case={test_case}",
    }
    flags = [
        flag_values[k]
        for k in spec.flag_order
        if flag_values.get(k) is not None
    ]
    flags_str = " ".join(flags)

    env_parts = [f"-e {k}={v}" for k, v in spec.env]
    env_str = (" " + " ".join(env_parts)) if env_parts else ""

    if spec.args_wrapper == "python_setup":
        inner_cmd = f'{spec.client_prefix} --args=\\"{flags_str}\\"'
    else:
        inner_cmd = f"{spec.client_prefix} {flags_str}"

    return (
        f"docker run -i --rm=true{env_str} -w {spec.workdir} --net=host"
        f' {docker_image} {spec.shell} "{inner_cmd}"'
    )


def get_github_repo(lang):
    return {
        "dart": "https://github.com/grpc/grpc-dart.git",
        "go": "https://github.com/grpc/grpc-go.git",
        "java": "https://github.com/grpc/grpc-java.git",
        "node": "https://github.com/grpc/grpc-node.git",
        # all other languages use the grpc.git repo.
    }.get(lang, "https://github.com/grpc/grpc.git")


def get_release_tags(lang):
    """Returns list of known releases for given language."""
    return list(LANG_RELEASE_MATRIX[lang].keys())


def get_runtimes_for_lang_release(lang, release):
    """Get list of valid runtimes for given release of lang."""
    runtimes = list(LANG_RUNTIME_MATRIX[lang])
    release_info = LANG_RELEASE_MATRIX[lang].get(release)
    if release_info and release_info.runtimes:
        runtimes = list(release_info.runtimes)
    return runtimes


def should_build_docker_interop_image_from_release_tag(lang):
    # All dockerfile definitions live in grpc/grpc repository.
    # For language that have a separate repo, we need to use
    # dockerfile definitions from head of grpc/grpc.
    if lang in ["go", "java", "node"]:
        return False
    return True


# Dictionary of default runtimes per language
LANG_RUNTIME_MATRIX = {
    "cxx": ["cxx"],  # This is actually debian8.
    "go": ["go1.x"],
    "java": ["java"],
    "python": ["python", "pythonasyncio"],
    "node": ["node"],
    "ruby": ["ruby"],
    "php": ["php7"],
    "csharp": ["csharp", "csharpcoreclr"],
}


class ReleaseInfo:
    """Info about a single release of a language"""

    def __init__(self, patch=[], runtimes=[], testcases_file=None):
        self.patch = patch
        self.runtimes = runtimes
        self.testcases_file = testcases_file


# Dictionary of known releases for given language.
LANG_RELEASE_MATRIX = {
    "cxx": OrderedDict(
        [
            ("v1.0.1", ReleaseInfo()),
            ("v1.1.4", ReleaseInfo()),
            ("v1.2.5", ReleaseInfo()),
            ("v1.3.9", ReleaseInfo()),
            ("v1.4.2", ReleaseInfo()),
            ("v1.6.6", ReleaseInfo()),
            ("v1.7.2", ReleaseInfo()),
            ("v1.8.0", ReleaseInfo()),
            ("v1.9.1", ReleaseInfo()),
            ("v1.10.1", ReleaseInfo()),
            ("v1.11.1", ReleaseInfo()),
            ("v1.12.0", ReleaseInfo()),
            ("v1.13.0", ReleaseInfo()),
            ("v1.14.1", ReleaseInfo()),
            ("v1.15.0", ReleaseInfo()),
            ("v1.16.0", ReleaseInfo()),
            ("v1.17.1", ReleaseInfo()),
            ("v1.18.0", ReleaseInfo()),
            ("v1.19.0", ReleaseInfo()),
            ("v1.20.0", ReleaseInfo()),
            ("v1.21.4", ReleaseInfo()),
            ("v1.22.0", ReleaseInfo()),
            ("v1.22.1", ReleaseInfo()),
            ("v1.23.0", ReleaseInfo()),
            ("v1.24.0", ReleaseInfo()),
            ("v1.25.0", ReleaseInfo()),
            ("v1.26.0", ReleaseInfo()),
            ("v1.27.3", ReleaseInfo()),
            ("v1.30.0", ReleaseInfo()),
            ("v1.31.1", ReleaseInfo()),
            ("v1.32.0", ReleaseInfo()),
            ("v1.33.2", ReleaseInfo()),
            ("v1.34.0", ReleaseInfo()),
            ("v1.35.0", ReleaseInfo()),
            ("v1.36.3", ReleaseInfo()),
            ("v1.37.0", ReleaseInfo()),
            ("v1.38.0", ReleaseInfo()),
            ("v1.39.0", ReleaseInfo()),
            ("v1.41.1", ReleaseInfo()),
            ("v1.42.0", ReleaseInfo()),
            ("v1.43.0", ReleaseInfo()),
            ("v1.44.0", ReleaseInfo()),
            ("v1.46.2", ReleaseInfo()),
            ("v1.47.1", ReleaseInfo()),
            ("v1.48.3", ReleaseInfo()),
            ("v1.49.1", ReleaseInfo()),
            ("v1.52.0", ReleaseInfo()),
            ("v1.53.0", ReleaseInfo()),
            ("v1.54.0", ReleaseInfo()),
            ("v1.55.0", ReleaseInfo()),
            ("v1.56.0", ReleaseInfo()),
            ("v1.57.0", ReleaseInfo()),
            ("v1.58.0", ReleaseInfo()),
            ("v1.60.0", ReleaseInfo()),
            ("v1.61.0", ReleaseInfo()),
            ("v1.62.0", ReleaseInfo()),
            ("v1.63.1", ReleaseInfo()),
            ("v1.64.1", ReleaseInfo()),
            ("v1.65.0", ReleaseInfo()),
            ("v1.66.0", ReleaseInfo()),
            ("v1.67.0", ReleaseInfo()),
            ("v1.69.0", ReleaseInfo()),
            ("v1.70.0", ReleaseInfo()),
            ("v1.71.0", ReleaseInfo()),
            ("v1.72.0", ReleaseInfo()),
            ("v1.73.0", ReleaseInfo()),
            ("v1.74.0", ReleaseInfo()),
            ("v1.75.1", ReleaseInfo()),
            ("v1.76.0", ReleaseInfo()),
        ]
    ),
    "go": OrderedDict(
        [
            ("v1.0.5", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.2.1", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.3.0", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.4.2", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.5.2", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.6.0", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.7.4", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.8.2", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.9.2", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.10.1", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.11.3", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.12.2", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.13.0", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.14.0", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.15.0", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.16.0", ReleaseInfo(runtimes=["go1.8"])),
            ("v1.17.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.18.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.19.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.20.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.21.3", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.22.3", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.23.1", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.24.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.25.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.26.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.27.1", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.28.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.29.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.30.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.31.1", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.32.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.33.1", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.34.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.35.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.36.0", ReleaseInfo(runtimes=["go1.11"])),
            ("v1.37.0", ReleaseInfo(runtimes=["go1.11"])),
            # NOTE: starting from release v1.38.0, use runtimes=['go1.16']
            ("v1.38.1", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.39.1", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.40.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.41.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.42.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.43.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.44.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.45.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.46.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.47.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.48.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.49.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.50.1", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.51.0", ReleaseInfo(runtimes=["go1.16"])),
            ("v1.52.3", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.53.0", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.54.1", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.55.0", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.56.3", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.57.2", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.58.3", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.59.0", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.60.1", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.61.2", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.62.2", ReleaseInfo(runtimes=["go1.19"])),
            ("v1.63.3", ReleaseInfo()),
            ("v1.64.1", ReleaseInfo()),
            ("v1.65.1", ReleaseInfo()),
            ("v1.66.3", ReleaseInfo()),
            ("v1.67.3", ReleaseInfo()),
            ("v1.68.2", ReleaseInfo()),
            ("v1.69.4", ReleaseInfo()),
            ("v1.70.0", ReleaseInfo()),
            ("v1.71.3", ReleaseInfo()),
            ("v1.72.3", ReleaseInfo()),
            ("v1.73.1", ReleaseInfo()),
            ("v1.74.3", ReleaseInfo()),
            ("v1.75.1", ReleaseInfo()),
            ("v1.76.0", ReleaseInfo()),
        ]
    ),
    "java": OrderedDict(
        [
            ("v1.0.3", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.1.2", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.2.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.3.1", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.4.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.5.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.6.1", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.7.1", ReleaseInfo()),
            ("v1.8.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.9.1", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.10.1", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.11.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.12.1", ReleaseInfo()),
            ("v1.13.2", ReleaseInfo()),
            ("v1.14.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.15.1", ReleaseInfo()),
            ("v1.16.1", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.17.2", ReleaseInfo()),
            ("v1.18.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.19.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.20.0", ReleaseInfo(runtimes=["java_oracle8"])),
            ("v1.21.1", ReleaseInfo()),
            ("v1.22.2", ReleaseInfo()),
            ("v1.23.0", ReleaseInfo()),
            ("v1.24.0", ReleaseInfo()),
            ("v1.25.0", ReleaseInfo()),
            ("v1.26.1", ReleaseInfo()),
            ("v1.27.2", ReleaseInfo()),
            ("v1.28.1", ReleaseInfo()),
            ("v1.29.0", ReleaseInfo()),
            ("v1.30.2", ReleaseInfo()),
            ("v1.31.2", ReleaseInfo()),
            ("v1.32.3", ReleaseInfo()),
            ("v1.33.1", ReleaseInfo()),
            ("v1.34.1", ReleaseInfo()),
            ("v1.35.1", ReleaseInfo()),
            ("v1.36.3", ReleaseInfo()),
            ("v1.37.1", ReleaseInfo()),
            ("v1.38.1", ReleaseInfo()),
            ("v1.39.0", ReleaseInfo()),
            ("v1.40.2", ReleaseInfo()),
            ("v1.41.3", ReleaseInfo()),
            ("v1.42.3", ReleaseInfo()),
            ("v1.43.3", ReleaseInfo()),
            ("v1.44.2", ReleaseInfo()),
            ("v1.45.4", ReleaseInfo()),
            ("v1.46.1", ReleaseInfo()),
            ("v1.47.1", ReleaseInfo()),
            ("v1.48.2", ReleaseInfo()),
            ("v1.49.2", ReleaseInfo()),
            ("v1.50.3", ReleaseInfo()),
            ("v1.51.3", ReleaseInfo()),
            ("v1.52.1", ReleaseInfo()),
            ("v1.53.0", ReleaseInfo()),
            ("v1.54.0", ReleaseInfo()),
            ("v1.55.1", ReleaseInfo()),
            ("v1.56.0", ReleaseInfo()),
            ("v1.57.2", ReleaseInfo()),
            ("v1.58.0", ReleaseInfo()),
            ("v1.59.1", ReleaseInfo()),
            ("v1.60.1", ReleaseInfo()),
            ("v1.61.0", ReleaseInfo()),
            ("v1.63.2", ReleaseInfo()),
            ("v1.64.2", ReleaseInfo()),
            ("v1.65.1", ReleaseInfo()),
            ("v1.66.0", ReleaseInfo()),
            ("v1.67.1", ReleaseInfo()),
            ("v1.68.3", ReleaseInfo()),
            ("v1.69.1", ReleaseInfo()),
            ("v1.70.0", ReleaseInfo()),
            ("v1.71.0", ReleaseInfo()),
            ("v1.72.0", ReleaseInfo()),
            ("v1.73.0", ReleaseInfo()),
            ("v1.74.0", ReleaseInfo()),
            ("v1.75.0", ReleaseInfo()),
            ("v1.76.0", ReleaseInfo()),
        ]
    ),
    "python": OrderedDict(
        [
            ("v1.0.x", ReleaseInfo(runtimes=["python"])),
            ("v1.1.4", ReleaseInfo(runtimes=["python"])),
            ("v1.2.5", ReleaseInfo(runtimes=["python"])),
            ("v1.3.9", ReleaseInfo(runtimes=["python"])),
            ("v1.4.2", ReleaseInfo(runtimes=["python"])),
            ("v1.6.6", ReleaseInfo(runtimes=["python"])),
            ("v1.7.2", ReleaseInfo(runtimes=["python"])),
            ("v1.8.1", ReleaseInfo(runtimes=["python"])),
            ("v1.9.1", ReleaseInfo(runtimes=["python"])),
            ("v1.10.1", ReleaseInfo(runtimes=["python"])),
            ("v1.11.1", ReleaseInfo(runtimes=["python"])),
            ("v1.12.0", ReleaseInfo(runtimes=["python"])),
            ("v1.13.0", ReleaseInfo(runtimes=["python"])),
            ("v1.14.1", ReleaseInfo(runtimes=["python"])),
            ("v1.15.0", ReleaseInfo(runtimes=["python"])),
            ("v1.16.0", ReleaseInfo(runtimes=["python"])),
            ("v1.17.1", ReleaseInfo(runtimes=["python"])),
            ("v1.18.0", ReleaseInfo(runtimes=["python"])),
            ("v1.19.0", ReleaseInfo(runtimes=["python"])),
            ("v1.20.0", ReleaseInfo(runtimes=["python"])),
            ("v1.21.4", ReleaseInfo(runtimes=["python"])),
            ("v1.22.0", ReleaseInfo(runtimes=["python"])),
            ("v1.22.1", ReleaseInfo(runtimes=["python"])),
            ("v1.23.0", ReleaseInfo(runtimes=["python"])),
            ("v1.24.0", ReleaseInfo(runtimes=["python"])),
            ("v1.25.0", ReleaseInfo(runtimes=["python"])),
            ("v1.26.0", ReleaseInfo(runtimes=["python"])),
            ("v1.27.3", ReleaseInfo(runtimes=["python"])),
            ("v1.30.0", ReleaseInfo(runtimes=["python"])),
            ("v1.31.1", ReleaseInfo(runtimes=["python"])),
            ("v1.32.0", ReleaseInfo(runtimes=["python"])),
            ("v1.33.2", ReleaseInfo(runtimes=["python"])),
            ("v1.34.0", ReleaseInfo(runtimes=["python"])),
            ("v1.35.0", ReleaseInfo(runtimes=["python"])),
            ("v1.36.3", ReleaseInfo(runtimes=["python"])),
            ("v1.37.0", ReleaseInfo(runtimes=["python"])),
            ("v1.38.0", ReleaseInfo(runtimes=["python"])),
            ("v1.39.0", ReleaseInfo(runtimes=["python"])),
            ("v1.41.1", ReleaseInfo(runtimes=["python"])),
            ("v1.42.0", ReleaseInfo(runtimes=["python"])),
            ("v1.43.2", ReleaseInfo(runtimes=["python"])),
            ("v1.44.0", ReleaseInfo(runtimes=["python"])),
            ("v1.46.2", ReleaseInfo(runtimes=["python"])),
            ("v1.47.1", ReleaseInfo(runtimes=["python"])),
            ("v1.48.3", ReleaseInfo(runtimes=["python"])),
            ("v1.49.1", ReleaseInfo(runtimes=["python"])),
            ("v1.52.0", ReleaseInfo(runtimes=["python"])),
            ("v1.53.0", ReleaseInfo(runtimes=["python"])),
            ("v1.54.0", ReleaseInfo(runtimes=["python"])),
            ("v1.55.0", ReleaseInfo(runtimes=["python"])),
            ("v1.56.0", ReleaseInfo(runtimes=["python"])),
            ("v1.57.0", ReleaseInfo(runtimes=["python"])),
            ("v1.58.0", ReleaseInfo(runtimes=["python"])),
            ("v1.60.0", ReleaseInfo(runtimes=["python"])),
            ("v1.61.0", ReleaseInfo(runtimes=["python"])),
            ("v1.62.0", ReleaseInfo(runtimes=["python"])),
            ("v1.63.0", ReleaseInfo(runtimes=["python"])),
            ("v1.64.1", ReleaseInfo(runtimes=["python"])),
            ("v1.65.0", ReleaseInfo(runtimes=["python"])),
            ("v1.66.0", ReleaseInfo(runtimes=["python"])),
            ("v1.67.0", ReleaseInfo(runtimes=["python"])),
            ("v1.69.0", ReleaseInfo(runtimes=["python"])),
            ("v1.70.0", ReleaseInfo(runtimes=["python"])),
            ("v1.71.0", ReleaseInfo(runtimes=["python"])),
            ("v1.72.0", ReleaseInfo(runtimes=["python"])),
            ("v1.73.0", ReleaseInfo(runtimes=["python"])),
            ("v1.74.0", ReleaseInfo(runtimes=["python"])),
            ("v1.75.1", ReleaseInfo(runtimes=["python"])),
            # Python images can't be anymore built without #40959 temporary fix,
            # or #40833 proper pyproject migration.
            # TODO(sreenithi): update this once v1.76.1 is out.
            # (
            #     "v1.76.0",
            #     ReleaseInfo(runtimes=["python"]),
            # ),
        ]
    ),
    "node": OrderedDict(
        [
            ("v1.0.1", ReleaseInfo()),
            ("v1.1.4", ReleaseInfo()),
            ("v1.2.5", ReleaseInfo()),
            ("v1.3.9", ReleaseInfo()),
            ("v1.4.2", ReleaseInfo()),
            ("v1.6.6", ReleaseInfo()),
            # TODO: https://github.com/grpc/grpc-node/issues/235.
            # ('v1.7.2', ReleaseInfo()),
            ("v1.8.4", ReleaseInfo()),
            ("v1.9.1", ReleaseInfo()),
            ("v1.10.0", ReleaseInfo()),
            ("v1.11.3", ReleaseInfo()),
            ("v1.12.4", ReleaseInfo()),
        ]
    ),
    "ruby": OrderedDict(
        [
            (
                "v1.0.1",
                ReleaseInfo(
                    patch=[
                        "tools/dockerfile/interoptest/grpc_interop_ruby/Dockerfile",
                        "tools/dockerfile/interoptest/grpc_interop_ruby/build_interop.sh",
                    ],
                ),
            ),
            ("v1.1.4", ReleaseInfo()),
            ("v1.2.5", ReleaseInfo()),
            ("v1.3.9", ReleaseInfo()),
            ("v1.4.2", ReleaseInfo()),
            ("v1.6.6", ReleaseInfo()),
            ("v1.7.2", ReleaseInfo()),
            ("v1.8.0", ReleaseInfo()),
            ("v1.9.1", ReleaseInfo()),
            ("v1.10.1", ReleaseInfo()),
            ("v1.11.1", ReleaseInfo()),
            ("v1.12.0", ReleaseInfo()),
            ("v1.13.0", ReleaseInfo()),
            ("v1.14.1", ReleaseInfo()),
            ("v1.15.0", ReleaseInfo()),
            ("v1.16.0", ReleaseInfo()),
            ("v1.17.1", ReleaseInfo()),
            (
                "v1.18.0",
                ReleaseInfo(
                    patch=[
                        "tools/dockerfile/interoptest/grpc_interop_ruby/build_interop.sh",
                    ]
                ),
            ),
            ("v1.19.0", ReleaseInfo()),
            ("v1.20.0", ReleaseInfo()),
            ("v1.21.4", ReleaseInfo()),
            ("v1.22.0", ReleaseInfo()),
            ("v1.22.1", ReleaseInfo()),
            ("v1.23.0", ReleaseInfo()),
            ("v1.24.0", ReleaseInfo()),
            ("v1.25.0", ReleaseInfo()),
            # TODO: https://github.com/grpc/grpc/issues/18262.
            # If you are not encountering the error in above issue
            # go ahead and upload the docker image for new releases.
            ("v1.26.0", ReleaseInfo()),
            ("v1.27.3", ReleaseInfo()),
            ("v1.30.0", ReleaseInfo()),
            ("v1.31.1", ReleaseInfo()),
            ("v1.32.0", ReleaseInfo()),
            ("v1.33.2", ReleaseInfo()),
            ("v1.34.0", ReleaseInfo()),
            ("v1.35.0", ReleaseInfo()),
            ("v1.36.3", ReleaseInfo()),
            ("v1.37.0", ReleaseInfo()),
            ("v1.38.0", ReleaseInfo()),
            ("v1.39.0", ReleaseInfo()),
            ("v1.41.1", ReleaseInfo()),
            ("v1.42.0", ReleaseInfo()),
            ("v1.43.0", ReleaseInfo()),
            ("v1.44.0", ReleaseInfo()),
            ("v1.46.2", ReleaseInfo()),
            ("v1.47.1", ReleaseInfo()),
            ("v1.48.3", ReleaseInfo()),
            ("v1.49.1", ReleaseInfo()),
            ("v1.52.0", ReleaseInfo()),
            ("v1.53.0", ReleaseInfo()),
            ("v1.54.0", ReleaseInfo()),
            ("v1.55.0", ReleaseInfo()),
            ("v1.56.0", ReleaseInfo()),
            ("v1.57.0", ReleaseInfo()),
            ("v1.58.0", ReleaseInfo()),
            ("v1.60.0", ReleaseInfo()),
            ("v1.61.0", ReleaseInfo()),
            ("v1.62.0", ReleaseInfo()),
            ("v1.63.0", ReleaseInfo()),
            ("v1.64.0", ReleaseInfo()),
            ("v1.65.0", ReleaseInfo()),
            ("v1.66.0", ReleaseInfo()),
            ("v1.67.0", ReleaseInfo()),
            ("v1.69.0", ReleaseInfo()),
            ("v1.70.0", ReleaseInfo()),
            ("v1.71.0", ReleaseInfo()),
            ("v1.72.0", ReleaseInfo()),
            ("v1.73.0", ReleaseInfo()),
            ("v1.74.0", ReleaseInfo()),
            ("v1.75.1", ReleaseInfo()),
            ("v1.76.0", ReleaseInfo()),
        ]
    ),
    "php": OrderedDict(
        [
            ("v1.0.1", ReleaseInfo()),
            ("v1.1.4", ReleaseInfo()),
            ("v1.2.5", ReleaseInfo()),
            ("v1.3.9", ReleaseInfo()),
            ("v1.4.2", ReleaseInfo()),
            ("v1.6.6", ReleaseInfo()),
            ("v1.7.2", ReleaseInfo()),
            ("v1.8.0", ReleaseInfo()),
            ("v1.9.1", ReleaseInfo()),
            ("v1.10.1", ReleaseInfo()),
            ("v1.11.1", ReleaseInfo()),
            ("v1.12.0", ReleaseInfo()),
            ("v1.13.0", ReleaseInfo()),
            ("v1.14.1", ReleaseInfo()),
            ("v1.15.0", ReleaseInfo()),
            ("v1.16.0", ReleaseInfo()),
            ("v1.17.1", ReleaseInfo()),
            ("v1.18.0", ReleaseInfo()),
            # v1.19 and v1.20 were deliberately omitted here because of an issue.
            # See https://github.com/grpc/grpc/issues/18264
            ("v1.21.4", ReleaseInfo()),
            ("v1.22.0", ReleaseInfo()),
            ("v1.22.1", ReleaseInfo()),
            ("v1.23.0", ReleaseInfo()),
            ("v1.24.0", ReleaseInfo()),
            ("v1.25.0", ReleaseInfo()),
            ("v1.26.0", ReleaseInfo()),
            ("v1.27.3", ReleaseInfo()),
            ("v1.30.0", ReleaseInfo()),
            ("v1.31.1", ReleaseInfo()),
            ("v1.32.0", ReleaseInfo()),
            ("v1.33.2", ReleaseInfo()),
            ("v1.34.0", ReleaseInfo()),
            ("v1.35.0", ReleaseInfo()),
            ("v1.36.3", ReleaseInfo()),
            ("v1.37.0", ReleaseInfo()),
            ("v1.38.0", ReleaseInfo()),
            ("v1.39.0", ReleaseInfo()),
            ("v1.41.1", ReleaseInfo()),
            ("v1.42.0", ReleaseInfo()),
            ("v1.43.0", ReleaseInfo()),
            ("v1.44.0", ReleaseInfo()),
            ("v1.46.2", ReleaseInfo()),
            ("v1.47.1", ReleaseInfo()),
            ("v1.48.3", ReleaseInfo()),
            ("v1.49.1", ReleaseInfo()),
            ("v1.52.0", ReleaseInfo()),
            ("v1.53.0", ReleaseInfo()),
            ("v1.54.0", ReleaseInfo()),
            ("v1.55.0", ReleaseInfo()),
            ("v1.56.0", ReleaseInfo()),
            ("v1.57.0", ReleaseInfo()),
            ("v1.58.0", ReleaseInfo()),
            ("v1.60.0", ReleaseInfo()),
            ("v1.61.0", ReleaseInfo()),
            ("v1.62.0", ReleaseInfo()),
            ("v1.63.0", ReleaseInfo()),
            ("v1.64.0", ReleaseInfo()),
            ("v1.65.0", ReleaseInfo()),
            ("v1.66.0", ReleaseInfo()),
            ("v1.67.0", ReleaseInfo()),
            ("v1.70.0", ReleaseInfo()),
            ("v1.71.0", ReleaseInfo()),
            ("v1.72.0", ReleaseInfo()),
            ("v1.73.0", ReleaseInfo()),
            ("v1.74.0", ReleaseInfo()),
            ("v1.75.1", ReleaseInfo()),
            ("v1.76.0", ReleaseInfo()),
        ]
    ),
    "csharp": OrderedDict(
        [
            (
                "v1.0.1",
                ReleaseInfo(
                    patch=[
                        "tools/dockerfile/interoptest/grpc_interop_csharp/Dockerfile",
                        "tools/dockerfile/interoptest/grpc_interop_csharpcoreclr/Dockerfile",
                    ],
                ),
            ),
            ("v1.1.4", ReleaseInfo()),
            ("v1.2.5", ReleaseInfo()),
            ("v1.3.9", ReleaseInfo()),
            ("v1.4.2", ReleaseInfo()),
            ("v1.6.6", ReleaseInfo()),
            ("v1.7.2", ReleaseInfo()),
            ("v1.8.0", ReleaseInfo()),
            ("v1.9.1", ReleaseInfo()),
            ("v1.10.1", ReleaseInfo()),
            ("v1.11.1", ReleaseInfo()),
            ("v1.12.0", ReleaseInfo()),
            ("v1.13.0", ReleaseInfo()),
            ("v1.14.1", ReleaseInfo()),
            ("v1.15.0", ReleaseInfo()),
            ("v1.16.0", ReleaseInfo()),
            ("v1.17.1", ReleaseInfo()),
            ("v1.18.0", ReleaseInfo()),
            ("v1.19.0", ReleaseInfo()),
            ("v1.20.0", ReleaseInfo()),
            ("v1.21.4", ReleaseInfo()),
            ("v1.22.0", ReleaseInfo()),
            ("v1.22.1", ReleaseInfo()),
            ("v1.23.0", ReleaseInfo()),
            ("v1.24.0", ReleaseInfo()),
            ("v1.25.0", ReleaseInfo()),
            ("v1.26.0", ReleaseInfo()),
            ("v1.27.3", ReleaseInfo()),
            ("v1.30.0", ReleaseInfo()),
            ("v1.31.1", ReleaseInfo()),
            ("v1.32.0", ReleaseInfo()),
            ("v1.33.2", ReleaseInfo()),
            ("v1.34.0", ReleaseInfo()),
            ("v1.35.0", ReleaseInfo()),
            ("v1.36.3", ReleaseInfo()),
            ("v1.37.0", ReleaseInfo()),
            ("v1.38.1", ReleaseInfo()),
            ("v1.39.1", ReleaseInfo()),
            ("v1.41.1", ReleaseInfo()),
            ("v1.42.0", ReleaseInfo()),
            ("v1.43.0", ReleaseInfo()),
            ("v1.44.0", ReleaseInfo()),
            ("v1.46.2", ReleaseInfo()),
        ]
    ),
}
