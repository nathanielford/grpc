#!/usr/bin/env python3
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
"""Run interop (cross-language) tests in parallel."""

from __future__ import print_function

import argparse
import atexit
from dataclasses import dataclass
import itertools
import multiprocessing
import os
import re
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple
import uuid

import six

_THIS_DIR = os.path.abspath(os.path.dirname(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

import python_utils.dockerjob as dockerjob
import python_utils.jobset as jobset
import python_utils.report_utils as report_utils

# It's ok to not import because this is only necessary to upload results to BQ.
try:
    from python_utils.upload_test_results import upload_interop_results_to_bq
except ImportError as e:
    print(e)

ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
os.chdir(ROOT)

_DEFAULT_SERVER_PORT = 8080

_SKIP_CLIENT_COMPRESSION = [
    "client_compressed_unary",
    "client_compressed_streaming",
]

_SKIP_SERVER_COMPRESSION = [
    "server_compressed_unary",
    "server_compressed_streaming",
]

_SKIP_COMPRESSION = _SKIP_CLIENT_COMPRESSION + _SKIP_SERVER_COMPRESSION

_SKIP_ADVANCED = [
    "status_code_and_message",
    "custom_metadata",
    "unimplemented_method",
    "unimplemented_service",
]

_SKIP_SPECIAL_STATUS_MESSAGE = ["special_status_message"]

_ORCA_TEST_CASES = ["orca_per_rpc", "orca_oob"]

_GOOGLE_DEFAULT_CREDS_TEST_CASE = "google_default_credentials"

_SKIP_GOOGLE_DEFAULT_CREDS = [
    _GOOGLE_DEFAULT_CREDS_TEST_CASE,
]

_COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE = "compute_engine_channel_credentials"

_SKIP_COMPUTE_ENGINE_CHANNEL_CREDS = [
    _COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE,
]

_TEST_TIMEOUT = 3 * 60

# disable this test on core-based languages,
# see https://github.com/grpc/grpc/issues/9779
_SKIP_DATA_FRAME_PADDING = ["data_frame_padding"]

# report suffix "sponge_log.xml" is important for reports to get picked up by internal CI
_DOCKER_BUILD_XML_REPORT = "interop_docker_build/sponge_log.xml"
_TESTS_XML_REPORT = "interop_test/sponge_log.xml"

_TEST_CASES = [
    "large_unary",
    "empty_unary",
    "ping_pong",
    "empty_stream",
    "client_streaming",
    "server_streaming",
    "cancel_after_begin",
    "cancel_after_first_response",
    "timeout_on_sleeping_server",
    "custom_metadata",
    "status_code_and_message",
    "unimplemented_method",
    "client_compressed_unary",
    "server_compressed_unary",
    "client_compressed_streaming",
    "server_compressed_streaming",
    "unimplemented_service",
    "special_status_message",
    "orca_per_rpc",
    "orca_oob",
]

_AUTH_TEST_CASES = [
    "compute_engine_creds",
    "jwt_token_creds",
    "oauth2_auth_token",
    "per_rpc_creds",
    _GOOGLE_DEFAULT_CREDS_TEST_CASE,
    _COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE,
]

_HTTP2_TEST_CASES = ["tls", "framing"]

_HTTP2_SERVER_TEST_CASES = [
    "rst_after_header",
    "rst_after_data",
    "rst_during_data",
    "goaway",
    "ping",
    "max_streams",
    "data_frame_padding",
    "no_df_padding_sanity_test",
]

_GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES = {
    "data_frame_padding": "large_unary",
    "no_df_padding_sanity_test": "large_unary",
}

_HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS = list(
    _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES.keys()
)

_LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES = [
    "java",
    "go",
    "python",
    "c++",
]

_LANGUAGES_FOR_ALTS_TEST_CASES = ["java", "go", "c++", "python"]

_SERVERS_FOR_ALTS_TEST_CASES = ["java", "go", "c++", "python"]

_TRANSPORT_SECURITY_OPTIONS = ["tls", "alts", "insecure"]

_CUSTOM_CREDENTIALS_TYPE_OPTIONS = [
    "tls",
    "google_default_credentials",
    "compute_engine_channel_creds",
]

DOCKER_WORKDIR_ROOT = "/var/local/git/grpc"

_PYTHON_BINARY = "py39/bin/python"
_NODE_WITH_NVM = (
    "packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh"
)
_PYTHON_ENV = (
    ("LD_LIBRARY_PATH", "{}/libs/opt".format(DOCKER_WORKDIR_ROOT)),
    ("PYTHONPATH", "{}/src/python/gens".format(DOCKER_WORKDIR_ROOT)),
)


@dataclass(frozen=True)
class LanguageConfig:
    """Declarative configuration for a gRPC interop language or HTTP/2 endpoint."""

    name: str
    safename: str
    client_cwd: Optional[str] = None
    server_cwd: Optional[str] = None
    http2_cwd: Optional[str] = None
    client_cmd_prefix: Tuple[str, ...] = ()
    server_cmd_prefix: Tuple[str, ...] = ()
    http2_cmd_prefix: Tuple[str, ...] = ()
    args_as_single_flag: bool = False
    server_ignores_args: bool = False
    env: Tuple[Tuple[str, str], ...] = ()
    unimplemented_client_cases: Tuple[str, ...] = ()
    unimplemented_server_cases: Tuple[str, ...] = ()

    def client_cmd(self, args: List[str]) -> Optional[List[str]]:
        if self.name == "objc":
            for arg in args:
                port = re.search(r"--server_port=(\d+)", arg)
                if port:
                    portnum = port.group(1)
                    cmdline = (
                        "pod install && xcodebuild -workspace Tests.xcworkspace"
                        ' -scheme InteropTestsLocalSSL -destination name="iPhone 6"'
                        " HOST_PORT_LOCALSSL=localhost:%s test" % portnum
                    )
                    return [cmdline]
            return None
        if self.args_as_single_flag:
            return list(self.client_cmd_prefix) + [
                '--args="{}"'.format(" ".join(args))
            ]
        return list(self.client_cmd_prefix) + list(args)

    def server_cmd(self, args: List[str]) -> List[str]:
        if self.server_ignores_args:
            return list(self.server_cmd_prefix)
        if self.args_as_single_flag:
            return list(self.server_cmd_prefix) + [
                '--args="{}"'.format(" ".join(args))
            ]
        return list(self.server_cmd_prefix) + list(args)

    def client_cmd_http2interop(self, args: List[str]) -> List[str]:
        return list(self.http2_cmd_prefix) + list(args)

    def cloud_to_prod_env(self) -> Dict[str, str]:
        return {}

    def global_env(self) -> Dict[str, str]:
        return dict(self.env)

    def unimplemented_test_cases(self) -> List[str]:
        return list(self.unimplemented_client_cases)

    def unimplemented_test_cases_server(self) -> List[str]:
        return list(self.unimplemented_server_cases)

    def __str__(self) -> str:
        return self.name


_LANGUAGES: Dict[str, LanguageConfig] = {
    "c++": LanguageConfig(
        name="c++",
        safename="cxx",
        client_cmd_prefix=("cmake/build/interop_client",),
        server_cmd_prefix=("cmake/build/interop_server",),
        http2_cmd_prefix=("cmake/build/http2_client",),
        unimplemented_client_cases=tuple(
            _SKIP_DATA_FRAME_PADDING
            + _SKIP_SPECIAL_STATUS_MESSAGE
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        ),
        unimplemented_server_cases=(),
    ),
    "aspnetcore": LanguageConfig(
        name="aspnetcore",
        safename="aspnetcore",
        client_cwd="../grpc-dotnet/output/InteropTestsClient",
        server_cwd="../grpc-dotnet/output/InteropTestsWebsite",
        client_cmd_prefix=("dotnet", "exec", "InteropTestsClient.dll"),
        server_cmd_prefix=("dotnet", "exec", "InteropTestsWebsite.dll"),
        unimplemented_client_cases=tuple(
            _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_ORCA_TEST_CASES),
    ),
    "dart": LanguageConfig(
        name="dart",
        safename="dart",
        client_cwd="../grpc-dart/interop",
        server_cwd="../grpc-dart/interop",
        http2_cwd="../grpc-dart/interop",
        client_cmd_prefix=("dart", "bin/client.dart"),
        server_cmd_prefix=("dart", "bin/server.dart"),
        unimplemented_client_cases=tuple(
            _SKIP_COMPRESSION
            + _SKIP_SPECIAL_STATUS_MESSAGE
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(
            _SKIP_COMPRESSION + _SKIP_SPECIAL_STATUS_MESSAGE + _ORCA_TEST_CASES
        ),
    ),
    "go": LanguageConfig(
        name="go",
        safename="go",
        client_cwd="/go/src/google.golang.org/grpc/interop/client",
        server_cwd="/go/src/google.golang.org/grpc/interop/server",
        http2_cwd="/go/src/google.golang.org/grpc/interop/http2",
        client_cmd_prefix=("go", "run", "client.go"),
        server_cmd_prefix=("go", "run", "server.go"),
        http2_cmd_prefix=("go", "run", "negative_http2_client.go"),
        env=(("GO111MODULE", "on"),),
        unimplemented_client_cases=tuple(_SKIP_COMPRESSION),
        unimplemented_server_cases=tuple(_SKIP_COMPRESSION),
    ),
    "java": LanguageConfig(
        name="java",
        safename="java",
        client_cwd="../grpc-java",
        server_cwd="../grpc-java",
        http2_cwd="../grpc-java",
        client_cmd_prefix=("./run-test-client.sh",),
        server_cmd_prefix=("./run-test-server.sh",),
        http2_cmd_prefix=(
            "./interop-testing/build/install/grpc-interop-testing/bin/http2-client",
        ),
        unimplemented_client_cases=(),
        # Does not support CompressedRequest feature.
        # Only supports CompressedResponse feature for unary.
        unimplemented_server_cases=tuple(
            _SKIP_CLIENT_COMPRESSION + ["server_compressed_streaming"]
        ),
    ),
    "javaokhttp": LanguageConfig(
        name="javaokhttp",
        safename="java",
        client_cwd="../grpc-java",
        client_cmd_prefix=("./run-test-client.sh", "--use_okhttp=true"),
        unimplemented_client_cases=tuple(_SKIP_DATA_FRAME_PADDING),
    ),
    "node": LanguageConfig(
        name="node",
        safename="node",
        client_cwd="../../../../home/appuser/grpc-node",
        server_cwd="../../../../home/appuser/grpc-node",
        client_cmd_prefix=(
            _NODE_WITH_NVM,
            "node",
            "--require",
            "./test/fixtures/native_native",
            "test/interop/interop_client.js",
        ),
        server_cmd_prefix=(
            _NODE_WITH_NVM,
            "node",
            "--require",
            "./test/fixtures/native_native",
            "test/interop/interop_server.js",
        ),
        unimplemented_client_cases=tuple(
            _SKIP_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_SKIP_COMPRESSION + _ORCA_TEST_CASES),
    ),
    "nodepurejs": LanguageConfig(
        name="nodepurejs",
        safename="nodepurejs",
        client_cwd="../../../../home/appuser/grpc-node",
        server_cwd="../../../../home/appuser/grpc-node",
        client_cmd_prefix=(
            _NODE_WITH_NVM,
            "node",
            "--require",
            "./test/fixtures/js_js",
            "test/interop/interop_client.js",
        ),
        unimplemented_client_cases=tuple(
            _SKIP_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_ORCA_TEST_CASES),
    ),
    "php7": LanguageConfig(
        name="php7",
        safename="php7",
        client_cmd_prefix=("src/php/bin/interop_client.sh",),
        server_cmd_prefix=("src/php/bin/interop_server.sh",),
        unimplemented_client_cases=tuple(
            _SKIP_SERVER_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_SKIP_COMPRESSION + _ORCA_TEST_CASES),
    ),
    "objc": LanguageConfig(
        name="objc",
        safename="objc",
        client_cwd="src/objective-c/tests",
        # ObjC test runs all cases with the same command. It ignores the testcase
        # cmdline argument. Here we return all but one test cases as unimplemented,
        # and depend upon ObjC test's behavior that it runs all cases even when
        # we tell it to run just one.
        unimplemented_client_cases=tuple(
            _TEST_CASES[1:]
            + _SKIP_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _SKIP_SPECIAL_STATUS_MESSAGE
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_SKIP_COMPRESSION + _ORCA_TEST_CASES),
    ),
    "ruby": LanguageConfig(
        name="ruby",
        safename="ruby",
        client_cmd_prefix=(
            "tools/run_tests/interop/with_rvm.sh",
            "ruby",
            "src/ruby/pb/test/client.rb",
        ),
        server_cmd_prefix=(
            "tools/run_tests/interop/with_rvm.sh",
            "ruby",
            "src/ruby/pb/test/server.rb",
        ),
        unimplemented_client_cases=tuple(
            _SKIP_SERVER_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_SKIP_COMPRESSION + _ORCA_TEST_CASES),
    ),
    "python": LanguageConfig(
        name="python",
        safename="python",
        client_cmd_prefix=(
            _PYTHON_BINARY,
            "src/python/grpcio_tests/setup.py",
            "run_interop",
            "--client",
        ),
        server_cmd_prefix=(
            _PYTHON_BINARY,
            "src/python/grpcio_tests/setup.py",
            "run_interop",
            "--server",
        ),
        http2_cmd_prefix=(
            _PYTHON_BINARY,
            "src/python/grpcio_tests/tests/http2/negative_http2_client.py",
        ),
        args_as_single_flag=True,
        env=_PYTHON_ENV,
        unimplemented_client_cases=tuple(
            _SKIP_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _SKIP_GOOGLE_DEFAULT_CREDS
            + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
            + _ORCA_TEST_CASES
        ),
        unimplemented_server_cases=tuple(_SKIP_COMPRESSION + _ORCA_TEST_CASES),
    ),
    "pythonasyncio": LanguageConfig(
        name="pythonasyncio",
        safename="pythonasyncio",
        client_cmd_prefix=(
            _PYTHON_BINARY,
            "src/python/grpcio_tests/setup.py",
            "run_interop",
            "--use-asyncio",
            "--client",
        ),
        server_cmd_prefix=(
            _PYTHON_BINARY,
            "src/python/grpcio_tests/setup.py",
            "run_interop",
            "--use-asyncio",
            "--server",
        ),
        http2_cmd_prefix=(
            _PYTHON_BINARY,
            "src/python/grpcio_tests/tests/http2/negative_http2_client.py",
        ),
        args_as_single_flag=True,
        env=_PYTHON_ENV,
        # TODO(https://github.com/grpc/grpc/issues/21707)
        unimplemented_client_cases=tuple(
            _SKIP_COMPRESSION
            + _SKIP_DATA_FRAME_PADDING
            + _AUTH_TEST_CASES
            + ["timeout_on_sleeping_server"]
            + _ORCA_TEST_CASES
        ),
        # TODO(https://github.com/grpc/grpc/issues/21749)
        unimplemented_server_cases=tuple(
            _TEST_CASES
            + _AUTH_TEST_CASES
            + _HTTP2_TEST_CASES
            + _HTTP2_SERVER_TEST_CASES
        ),
    ),
}

_HTTP2_SERVER = LanguageConfig(
    name="http2",
    safename="http2",
    server_cmd_prefix=("python test/http2_test/http2_test_server.py",),
    server_ignores_args=True,
    unimplemented_client_cases=tuple(
        _TEST_CASES
        + _SKIP_DATA_FRAME_PADDING
        + _SKIP_SPECIAL_STATUS_MESSAGE
        + _SKIP_GOOGLE_DEFAULT_CREDS
        + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
    ),
    unimplemented_server_cases=tuple(_TEST_CASES),
)

_HTTP2_CLIENT = LanguageConfig(
    name="http2",
    safename="http2",
    client_cmd_prefix=("tools/http2_interop/http2_interop.test", "-test.v"),
    unimplemented_client_cases=tuple(
        _TEST_CASES
        + _SKIP_SPECIAL_STATUS_MESSAGE
        + _SKIP_GOOGLE_DEFAULT_CREDS
        + _SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
    ),
    unimplemented_server_cases=tuple(_TEST_CASES),
)


def Http2Server() -> LanguageConfig:
    return _HTTP2_SERVER


def Http2Client() -> LanguageConfig:
    return _HTTP2_CLIENT


# languages supported as cloud_to_cloud servers
# NOTE: pythonasyncio has all server test cases unimplemented and is excluded
# so we do not start an idle server container.
_SERVERS = [
    "c++",
    "node",
    "aspnetcore",
    "java",
    "go",
    "ruby",
    "python",
    "dart",
    "php7",
]


def _server_has_implemented_test_cases(
    server_lang_name: str, http2_interop: bool = False
) -> bool:
    """Returns True if the server language implements at least one active test case."""
    lang = _LANGUAGES.get(server_lang_name)
    if not lang:
        return True
    unimplemented = set(lang.unimplemented_test_cases_server())
    if any(tc not in unimplemented for tc in _TEST_CASES):
        return True
    if http2_interop and any(
        tc not in unimplemented for tc in _HTTP2_TEST_CASES
    ):
        return True
    return False


def docker_run_cmdline(cmdline, image, docker_args=[], cwd=None, environ=None):
    """Wraps given cmdline array to create 'docker run' cmdline from it."""

    # don't use '-t' even when TTY is available, since that would break
    # the testcases generated by tools/interop_matrix/create_testcases.sh
    docker_cmdline = ["docker", "run", "-i", "--rm=true"]

    # turn environ into -e docker args
    if environ:
        for k, v in list(environ.items()):
            docker_cmdline += ["-e", "%s=%s" % (k, v)]

    # set working directory
    workdir = DOCKER_WORKDIR_ROOT
    if cwd:
        workdir = os.path.join(workdir, cwd)
    docker_cmdline += ["-w", workdir]

    docker_cmdline += docker_args + [image] + cmdline
    return docker_cmdline


def manual_cmdline(docker_cmdline, docker_image):
    """Returns docker cmdline adjusted for manual invocation."""
    print_cmdline = []
    for item in docker_cmdline:
        if item.startswith("--name="):
            continue
        if item == docker_image:
            item = "$docker_image"
        item = item.replace('"', '\\"')
        # add quotes when necessary
        if any(character.isspace() for character in item):
            item = '"%s"' % item
        print_cmdline.append(item)
    return " ".join(print_cmdline)


def write_cmdlog_maybe(cmdlog, filename):
    """Returns docker cmdline adjusted for manual invocation."""
    if cmdlog:
        with open(filename, "w") as logfile:
            logfile.write("#!/bin/bash\n")
            logfile.write("# DO NOT MODIFY\n")
            logfile.write(
                "# This file is generated by"
                " run_interop_tests.py/create_testcases.sh\n"
            )
            logfile.writelines("%s\n" % line for line in cmdlog)
        print("Command log written to file %s" % filename)


def bash_cmdline(cmdline):
    """Creates bash -c cmdline from args list."""
    # Use login shell:
    # * makes error messages clearer if executables are missing
    return ["bash", "-c", " ".join(cmdline)]


def compute_engine_creds_required(language, test_case):
    """Returns True if given test requires access to compute engine creds."""
    language = str(language)
    if test_case == "compute_engine_creds":
        return True
    if test_case == "oauth2_auth_token" and language == "c++":
        # C++ oauth2 test uses GCE creds because C++ only supports JWT
        return True
    return False


def auth_options(
    language,
    test_case,
    google_default_creds_use_key_file,
    service_account_key_file,
    default_service_account,
):
    """Returns (cmdline, env) tuple with cloud_to_prod_auth test options."""

    language = str(language)
    cmdargs = []
    env = {}

    oauth_scope_arg = "--oauth_scope=https://www.googleapis.com/auth/xapi.zoo"
    key_file_arg = "--service_account_key_file=%s" % service_account_key_file
    default_account_arg = (
        "--default_service_account=%s" % default_service_account
    )

    if test_case in ["jwt_token_creds", "per_rpc_creds", "oauth2_auth_token"]:
        if language in [
            "aspnetcore",
            "node",
            "php7",
            "python",
            "ruby",
            "nodepurejs",
        ]:
            env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
        else:
            cmdargs += [key_file_arg]

    if test_case in ["per_rpc_creds", "oauth2_auth_token"]:
        cmdargs += [oauth_scope_arg]

    if test_case == "oauth2_auth_token" and language == "c++":
        # C++ oauth2 test uses GCE creds and thus needs to know the default account
        cmdargs += [default_account_arg]

    if test_case == "compute_engine_creds":
        cmdargs += [oauth_scope_arg, default_account_arg]

    if test_case == _GOOGLE_DEFAULT_CREDS_TEST_CASE:
        if google_default_creds_use_key_file:
            env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
        cmdargs += [default_account_arg]

    if test_case == _COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE:
        cmdargs += [default_account_arg]

    return (cmdargs, env)


def _job_kill_handler(job):
    if job._spec.container_name:
        dockerjob.docker_kill(job._spec.container_name)
        # When the job times out and we decide to kill it,
        # we need to wait a before restarting the job
        # to prevent "container name already in use" error.
        # TODO(jtattermusch): figure out a cleaner way to this.
        time.sleep(2)


def cloud_to_prod_jobspec(
    language,
    test_case,
    server_host_nickname,
    server_host,
    google_default_creds_use_key_file,
    docker_image=None,
    auth=False,
    manual_cmd_log=None,
    service_account_key_file=None,
    default_service_account=None,
    transport_security="tls",
):
    """Creates jobspec for cloud-to-prod interop test"""
    container_name = None
    cmdargs = [
        "--server_host=%s" % server_host,
        "--server_port=443",
        "--test_case=%s" % test_case,
    ]
    if transport_security == "tls":
        transport_security_options = ["--use_tls=true"]
    elif transport_security == "google_default_credentials" and str(
        language
    ) in ["c++", "go", "java", "javaokhttp"]:
        transport_security_options = [
            "--custom_credentials_type=google_default_credentials"
        ]
    elif transport_security == "compute_engine_channel_creds" and str(
        language
    ) in ["go", "java", "javaokhttp"]:
        transport_security_options = [
            "--custom_credentials_type=compute_engine_channel_creds"
        ]
    else:
        print(
            "Invalid transport security option %s in cloud_to_prod_jobspec."
            " Lang: %s" % (str(language), transport_security)
        )
        sys.exit(1)
    cmdargs = cmdargs + transport_security_options
    environ = dict(language.cloud_to_prod_env(), **language.global_env())
    if auth:
        auth_cmdargs, auth_env = auth_options(
            language,
            test_case,
            google_default_creds_use_key_file,
            service_account_key_file,
            default_service_account,
        )
        cmdargs += auth_cmdargs
        environ.update(auth_env)
    cmdline = bash_cmdline(language.client_cmd(cmdargs))
    cwd = language.client_cwd

    if docker_image:
        container_name = dockerjob.random_name(
            "interop_client_%s" % language.safename
        )
        cmdline = docker_run_cmdline(
            cmdline,
            image=docker_image,
            cwd=cwd,
            environ=environ,
            docker_args=["--net=host", "--name=%s" % container_name],
        )
        if manual_cmd_log is not None:
            if manual_cmd_log == []:
                manual_cmd_log.append(
                    'echo "Testing ${docker_image:=%s}"' % docker_image
                )
            manual_cmd_log.append(manual_cmdline(cmdline, docker_image))
        cwd = None
        environ = None

    suite_name = "cloud_to_prod_auth" if auth else "cloud_to_prod"
    allow_flakes = bool(args and getattr(args, "allow_flakes", False))
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="%s:%s:%s:%s:%s"
        % (
            suite_name,
            language,
            server_host_nickname,
            test_case,
            transport_security,
        ),
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=4 if allow_flakes else 0,
        timeout_retries=2 if allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
        test_job.container_name = container_name
    return test_job


def cloud_to_cloud_jobspec(
    language,
    test_case,
    server_name,
    server_host,
    server_port,
    docker_image=None,
    transport_security="tls",
    manual_cmd_log=None,
):
    """Creates jobspec for cloud-to-cloud interop test"""
    interop_only_options = [
        "--server_host_override=foo.test.google.fr",
        "--use_test_ca=true",
    ]
    if transport_security == "tls":
        interop_only_options += ["--use_tls=true"]
    elif transport_security == "alts":
        interop_only_options += ["--use_tls=false", "--use_alts=true"]
    elif transport_security == "insecure":
        interop_only_options += ["--use_tls=false"]
    else:
        print(
            "Invalid transport security option %s in cloud_to_cloud_jobspec."
            % transport_security
        )
        sys.exit(1)

    client_test_case = test_case
    if test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
        client_test_case = _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES[
            test_case
        ]
    if client_test_case in language.unimplemented_test_cases():
        print(
            "asking client %s to run unimplemented test case %s"
            % (repr(language), client_test_case)
        )
        sys.exit(1)

    if test_case in _ORCA_TEST_CASES:
        interop_only_options += [
            '--service_config_json=\'{"loadBalancingConfig":[{"test_backend_metrics_load_balancer":{}}]}\''
        ]

    common_options = [
        "--test_case=%s" % client_test_case,
        "--server_host=%s" % server_host,
        "--server_port=%s" % server_port,
    ]

    if test_case in _HTTP2_SERVER_TEST_CASES:
        if test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
            client_options = interop_only_options + common_options
            cmdline = bash_cmdline(language.client_cmd(client_options))
            cwd = language.client_cwd
        else:
            cmdline = bash_cmdline(
                language.client_cmd_http2interop(common_options)
            )
            cwd = language.http2_cwd
    else:
        cmdline = bash_cmdline(
            language.client_cmd(common_options + interop_only_options)
        )
        cwd = language.client_cwd

    environ = language.global_env()
    if docker_image and language.safename != "objc":
        # we can't run client in docker for objc.
        container_name = dockerjob.random_name(
            "interop_client_%s" % language.safename
        )
        cmdline = docker_run_cmdline(
            cmdline,
            image=docker_image,
            environ=environ,
            cwd=cwd,
            docker_args=["--net=host", "--name=%s" % container_name],
        )
        if manual_cmd_log is not None:
            if manual_cmd_log == []:
                manual_cmd_log.append(
                    'echo "Testing ${docker_image:=%s}"' % docker_image
                )
            manual_cmd_log.append(manual_cmdline(cmdline, docker_image))
        cwd = None

    allow_flakes = bool(args and getattr(args, "allow_flakes", False))
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="cloud_to_cloud:%s:%s_server:%s:%s"
        % (language, server_name, test_case, transport_security),
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=4 if allow_flakes else 0,
        timeout_retries=2 if allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
        test_job.container_name = container_name
    return test_job


def server_jobspec(
    language, docker_image, transport_security="tls", manual_cmd_log=None
):
    """Create jobspec for running a server"""
    container_name = dockerjob.random_name(
        "interop_server_%s" % language.safename
    )
    server_cmd = ["--port=%s" % _DEFAULT_SERVER_PORT]
    if transport_security == "tls":
        server_cmd += ["--use_tls=true"]
    elif transport_security == "alts":
        server_cmd += ["--use_tls=false", "--use_alts=true"]
    elif transport_security == "insecure":
        server_cmd += ["--use_tls=false"]
    else:
        print(
            "Invalid transport security option %s in server_jobspec."
            % transport_security
        )
        sys.exit(1)
    cmdline = bash_cmdline(language.server_cmd(server_cmd))
    environ = language.global_env()
    docker_args = ["--name=%s" % container_name]
    if language.safename == "http2":
        # we are running the http2 interop server. Open next N ports beginning
        # with the server port. These ports are used for http2 interop test
        # (one test case per port).
        docker_args += list(
            itertools.chain.from_iterable(
                ("-p", str(_DEFAULT_SERVER_PORT + i))
                for i in range(len(_HTTP2_SERVER_TEST_CASES))
            )
        )
        # Enable docker's healthcheck mechanism.
        # This runs a Python script inside the container every second. The script
        # pings the http2 server to verify it is ready. The 'health-retries' flag
        # specifies the number of consecutive failures before docker will report
        # the container's status as 'unhealthy'. Prior to the first 'health_retries'
        # failures or the first success, the status will be 'starting'. 'docker ps'
        # or 'docker inspect' can be used to see the health of the container on the
        # command line.
        docker_args += [
            "--health-cmd=python test/http2_test/http2_server_health_check.py "
            "--server_host=%s --server_port=%d"
            % ("localhost", _DEFAULT_SERVER_PORT),
            "--health-interval=1s",
            "--health-retries=5",
            "--health-timeout=10s",
        ]

    else:
        docker_args += ["-p", str(_DEFAULT_SERVER_PORT)]

    docker_cmdline = docker_run_cmdline(
        cmdline,
        image=docker_image,
        cwd=language.server_cwd,
        environ=environ,
        docker_args=docker_args,
    )
    if manual_cmd_log is not None:
        if manual_cmd_log == []:
            manual_cmd_log.append(
                'echo "Testing ${docker_image:=%s}"' % docker_image
            )
        manual_cmd_log.append(manual_cmdline(docker_cmdline, docker_image))
    server_job = jobset.JobSpec(
        cmdline=docker_cmdline,
        environ=environ,
        shortname="interop_server_%s" % language,
        timeout_seconds=30 * 60,
    )
    server_job.container_name = container_name
    return server_job


def build_interop_image_jobspec(language, tag=None):
    """Creates jobspec for building interop docker image for a language"""
    if not tag:
        tag = "grpc_interop_%s:%s" % (language.safename, uuid.uuid4())
    env = {
        "INTEROP_IMAGE": tag,
        "BASE_NAME": "grpc_interop_%s" % language.safename,
    }
    build_job = jobset.JobSpec(
        cmdline=["tools/run_tests/dockerize/build_interop_image.sh"],
        environ=env,
        shortname="build_docker_%s" % (language),
        timeout_seconds=45 * 60,
    )
    build_job.tag = tag
    return build_job


def compute_docker_build_jobs(
    languages,
    servers,
    http2_interop=False,
    http2_server_interop=False,
    languages_http2_clients_for_http2_server_interop=(),
):
    """Computes deduplicated Docker image build jobs keyed by language safename.

    Returns (build_jobs, docker_images) where:
    - build_jobs: list of JobSpec (at most one per unique safename)
    - docker_images: dict mapping str(language) -> image tag
    """
    languages_to_build = set(
        _LANGUAGES[k]
        for k in set([str(l) for l in languages] + [s for s in servers])
    )
    languages_to_build |= set(languages_http2_clients_for_http2_server_interop)

    if http2_interop:
        languages_to_build.add(_HTTP2_CLIENT)

    if http2_server_interop:
        languages_to_build.add(_HTTP2_SERVER)

    # Group by safename so shared images (e.g. java & javaokhttp -> 'java',
    # Http2Client & Http2Server -> 'http2') are built only once.
    safename_to_langs: Dict[str, List[LanguageConfig]] = {}
    for l in sorted(languages_to_build, key=lambda x: str(x)):
        if str(l) == "objc":
            # we don't need to build a docker image for objc
            continue
        safename_to_langs.setdefault(l.safename, []).append(l)

    docker_images: Dict[str, str] = {}
    build_jobs = []
    for safename in sorted(safename_to_langs.keys()):
        langs_for_image = safename_to_langs[safename]
        canonical_lang = langs_for_image[0]
        job = build_interop_image_jobspec(canonical_lang)
        for l in langs_for_image:
            docker_images[str(l)] = job.tag
        build_jobs.append(job)

    return build_jobs, docker_images


# A dictionary of prod servers to test against.
# See go/grpc-interop-tests (internal-only) for details.
prod_servers = {
    "default": "grpc-test.sandbox.googleapis.com",
    "gateway_v4": "grpc-test4.sandbox.googleapis.com",
}

argp = argparse.ArgumentParser(description="Run interop tests.")
argp.add_argument(
    "-l",
    "--language",
    choices=["all"] + sorted(_LANGUAGES),
    nargs="+",
    default=["all"],
    help="Clients to run. Objc client can be only run on OSX.",
)
argp.add_argument("-j", "--jobs", default=multiprocessing.cpu_count(), type=int)
argp.add_argument(
    "--cloud_to_prod",
    default=False,
    action="store_const",
    const=True,
    help="Run cloud_to_prod tests.",
)
argp.add_argument(
    "--cloud_to_prod_auth",
    default=False,
    action="store_const",
    const=True,
    help="Run cloud_to_prod_auth tests.",
)
argp.add_argument(
    "--google_default_creds_use_key_file",
    default=False,
    action="store_const",
    const=True,
    help=(
        "Whether or not we should use a key file for the "
        "google_default_credentials test case, e.g. by "
        "setting env var GOOGLE_APPLICATION_CREDENTIALS."
    ),
)
argp.add_argument(
    "--prod_servers",
    choices=list(prod_servers.keys()),
    default=["default"],
    nargs="+",
    help=(
        "The servers to run cloud_to_prod and cloud_to_prod_auth tests against."
    ),
)
argp.add_argument(
    "-s",
    "--server",
    choices=["all"] + sorted(_SERVERS) + ["pythonasyncio"],
    nargs="+",
    help="Run cloud_to_cloud servers in a separate docker "
    + "image. Servers can only be started automatically if "
    + "--use_docker option is enabled.",
    default=[],
)
argp.add_argument(
    "--override_server",
    action="append",
    type=lambda kv: kv.split("="),
    help=(
        "Use servername=HOST:PORT to explicitly specify a server. E.g."
        " csharp=localhost:50000"
    ),
    default=[],
)
# TODO(jtattermusch): the default service_account_key_file only works when --use_docker is used.
argp.add_argument(
    "--service_account_key_file",
    type=str,
    help="The service account key file to use for some auth interop tests.",
    default="/root/service_account/grpc-testing-ebe7c1ac7381.json",
)
argp.add_argument(
    "--default_service_account",
    type=str,
    help=(
        "Default GCE service account email to use for some auth interop tests."
    ),
    default="830293263384-compute@developer.gserviceaccount.com",
)
argp.add_argument(
    "-t",
    "--travis",
    default=False,
    action="store_const",
    const=True,
    help=(
        "When set, indicates that the script is running on CI (= not locally)."
    ),
)
argp.add_argument(
    "-v", "--verbose", default=False, action="store_const", const=True
)
argp.add_argument(
    "--use_docker",
    default=False,
    action="store_const",
    const=True,
    help="Run all the interop tests under docker. That provides "
    + "additional isolation and prevents the need to install "
    + "language specific prerequisites. Only available on Linux.",
)
argp.add_argument(
    "--allow_flakes",
    default=False,
    action="store_const",
    const=True,
    help=(
        "Allow flaky tests to show as passing (re-runs failed tests up to five"
        " times)"
    ),
)
argp.add_argument(
    "--manual_run",
    default=False,
    action="store_const",
    const=True,
    help="Prepare things for running interop tests manually. "
    + "Preserve docker images after building them and skip "
    "actually running the tests. Only print commands to run by " + "hand.",
)
argp.add_argument(
    "--http2_interop",
    default=False,
    action="store_const",
    const=True,
    help="Enable HTTP/2 client edge case testing. (Bad client, good server)",
)
argp.add_argument(
    "--http2_server_interop",
    default=False,
    action="store_const",
    const=True,
    help=(
        "Enable HTTP/2 server edge case testing. (Includes positive and"
        " negative tests"
    ),
)
argp.add_argument(
    "--transport_security",
    choices=_TRANSPORT_SECURITY_OPTIONS,
    default="tls",
    type=str,
    nargs="?",
    const=True,
    help="Which transport security mechanism to use.",
)
argp.add_argument(
    "--custom_credentials_type",
    choices=_CUSTOM_CREDENTIALS_TYPE_OPTIONS,
    default=_CUSTOM_CREDENTIALS_TYPE_OPTIONS,
    nargs="+",
    help=(
        "Credential types to test in the cloud_to_prod setup. Default is to"
        " test with all creds types possible."
    ),
)
argp.add_argument(
    "--skip_compute_engine_creds",
    default=False,
    action="store_const",
    const=True,
    help="Skip auth tests requiring access to compute engine credentials.",
)
argp.add_argument(
    "--bq_result_table",
    default="",
    type=str,
    nargs="?",
    help="Upload test results to a specified BQ table.",
)

args = argp.parse_args([])


def main():
    global args
    # Docker doesn't clean up after itself, so we do it on exit.
    atexit.register(lambda: subprocess.call(["stty", "echo"]))
    args = argp.parse_args()

    servers = set(
        s
        for s in itertools.chain.from_iterable(
            _SERVERS if x == "all" else [x] for x in args.server
        )
        if _server_has_implemented_test_cases(s, args.http2_interop)
    )
    # ALTS servers are only available for certain languages.
    if args.transport_security == "alts":
        servers = servers.intersection(_SERVERS_FOR_ALTS_TEST_CASES)

    if args.use_docker:
        if not args.travis:
            print("Seen --use_docker flag, will run interop tests under docker.")

    if args.manual_run and not args.use_docker:
        print(
            "--manual_run is only supported with --use_docker option enabled."
        )
        sys.exit(1)

    if not args.use_docker and servers:
        print(
            "Running interop servers is only supported with --use_docker option"
            " enabled."
        )
        sys.exit(1)

    # we want to include everything but objc in 'all'
    # because objc won't run on non-mac platforms
    all_but_objc = set(six.iterkeys(_LANGUAGES)) - set(["objc"])
    languages = set(
        _LANGUAGES[l]
        for l in itertools.chain.from_iterable(
            all_but_objc if x == "all" else [x] for x in args.language
        )
    )
    # ALTS interop clients are only available for certain languages.
    if args.transport_security == "alts":
        alts_languages = set(
            _LANGUAGES[l] for l in _LANGUAGES_FOR_ALTS_TEST_CASES
        )
        languages = languages.intersection(alts_languages)

    languages_http2_clients_for_http2_server_interop = set()
    if args.http2_server_interop:
        languages_http2_clients_for_http2_server_interop = set(
            _LANGUAGES[l]
            for l in _LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES
            if "all" in args.language or l in args.language
        )

    http2Interop = _HTTP2_CLIENT if args.http2_interop else None
    http2InteropServer = _HTTP2_SERVER if args.http2_server_interop else None

    docker_images = {}
    if args.use_docker:
        build_jobs, docker_images = compute_docker_build_jobs(
            languages=languages,
            servers=servers,
            http2_interop=args.http2_interop,
            http2_server_interop=args.http2_server_interop,
            languages_http2_clients_for_http2_server_interop=languages_http2_clients_for_http2_server_interop,
        )

        if build_jobs:
            jobset.message(
                "START", "Building interop docker images.", do_newline=True
            )
            if args.verbose:
                print(
                    "Jobs to run: \n%s\n"
                    % "\n".join(str(j) for j in build_jobs)
                )

            num_failures, build_resultset = jobset.run(
                build_jobs, newline_on_success=True, maxjobs=args.jobs
            )

            report_utils.render_junit_xml_report(
                build_resultset, _DOCKER_BUILD_XML_REPORT
            )

            if num_failures == 0:
                jobset.message(
                    "SUCCESS",
                    "All docker images built successfully.",
                    do_newline=True,
                )
            else:
                jobset.message(
                    "FAILED",
                    "Failed to build interop docker images.",
                    do_newline=True,
                )
                for image in sorted(set(six.itervalues(docker_images))):
                    dockerjob.remove_image(image, skip_nonexistent=True)
                sys.exit(1)

    server_manual_cmd_log = [] if args.manual_run else None
    client_manual_cmd_log = [] if args.manual_run else None

    # Start interop servers.
    server_jobs = {}
    server_addresses = {}
    try:
        for s in servers:
            lang = str(s)
            spec = server_jobspec(
                _LANGUAGES[lang],
                docker_images.get(lang),
                args.transport_security,
                manual_cmd_log=server_manual_cmd_log,
            )
            if not args.manual_run:
                job = dockerjob.DockerJob(spec)
                server_jobs[lang] = job
                server_addresses[lang] = (
                    "localhost",
                    job.mapped_port(_DEFAULT_SERVER_PORT),
                )
            else:
                # don't run the server, set server port to a placeholder value
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")

        http2_server_job = None
        if args.http2_server_interop:
            # launch a HTTP2 server emulator that creates edge cases
            lang = str(http2InteropServer)
            spec = server_jobspec(
                http2InteropServer,
                docker_images.get(lang),
                manual_cmd_log=server_manual_cmd_log,
            )
            if not args.manual_run:
                http2_server_job = dockerjob.DockerJob(spec)
                server_jobs[lang] = http2_server_job
            else:
                # don't run the server, set server port to a placeholder value
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")

        jobs = []
        if args.cloud_to_prod:
            if args.transport_security not in ["tls"]:
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                for language in languages:
                    for test_case in _TEST_CASES:
                        if not test_case in language.unimplemented_test_cases():
                            if (
                                not test_case
                                in _SKIP_ADVANCED
                                + _SKIP_COMPRESSION
                                + _SKIP_SPECIAL_STATUS_MESSAGE
                                + _ORCA_TEST_CASES
                            ):
                                for (
                                    transport_security
                                ) in args.custom_credentials_type:
                                    # google_default_credentials not yet supported by all languages
                                    if (
                                        transport_security
                                        == "google_default_credentials"
                                        and str(language)
                                        not in [
                                            "c++",
                                            "go",
                                            "java",
                                            "javaokhttp",
                                        ]
                                    ):
                                        continue
                                    # compute_engine_channel_creds not yet supported by all languages
                                    if (
                                        transport_security
                                        == "compute_engine_channel_creds"
                                        and str(language)
                                        not in ["go", "java", "javaokhttp"]
                                    ):
                                        continue
                                    test_job = cloud_to_prod_jobspec(
                                        language,
                                        test_case,
                                        server_host_nickname,
                                        prod_servers[server_host_nickname],
                                        google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                        docker_image=docker_images.get(
                                            str(language)
                                        ),
                                        manual_cmd_log=client_manual_cmd_log,
                                        service_account_key_file=args.service_account_key_file,
                                        default_service_account=args.default_service_account,
                                        transport_security=transport_security,
                                    )
                                    jobs.append(test_job)
                if args.http2_interop:
                    for test_case in _HTTP2_TEST_CASES:
                        test_job = cloud_to_prod_jobspec(
                            http2Interop,
                            test_case,
                            server_host_nickname,
                            prod_servers[server_host_nickname],
                            google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                            docker_image=docker_images.get(str(http2Interop)),
                            manual_cmd_log=client_manual_cmd_log,
                            service_account_key_file=args.service_account_key_file,
                            default_service_account=args.default_service_account,
                            transport_security=args.transport_security,
                        )
                        jobs.append(test_job)

        if args.cloud_to_prod_auth:
            if args.transport_security not in ["tls"]:
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                for language in languages:
                    for test_case in _AUTH_TEST_CASES:
                        if (
                            not args.skip_compute_engine_creds
                            or not compute_engine_creds_required(
                                language, test_case
                            )
                        ):
                            if (
                                not test_case
                                in language.unimplemented_test_cases()
                            ):
                                if test_case == _GOOGLE_DEFAULT_CREDS_TEST_CASE:
                                    transport_security = (
                                        "google_default_credentials"
                                    )
                                elif (
                                    test_case
                                    == _COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE
                                ):
                                    transport_security = (
                                        "compute_engine_channel_creds"
                                    )
                                else:
                                    transport_security = "tls"
                                if (
                                    transport_security
                                    not in args.custom_credentials_type
                                ):
                                    continue
                                test_job = cloud_to_prod_jobspec(
                                    language,
                                    test_case,
                                    server_host_nickname,
                                    prod_servers[server_host_nickname],
                                    google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                    docker_image=docker_images.get(
                                        str(language)
                                    ),
                                    auth=True,
                                    manual_cmd_log=client_manual_cmd_log,
                                    service_account_key_file=args.service_account_key_file,
                                    default_service_account=args.default_service_account,
                                    transport_security=transport_security,
                                )
                                jobs.append(test_job)

        for server in args.override_server:
            server_name = server[0]
            (server_host, server_port) = server[1].split(":")
            server_addresses[server_name] = (server_host, server_port)

        for server_name, server_address in list(server_addresses.items()):
            (server_host, server_port) = server_address
            server_language = _LANGUAGES.get(server_name, None)
            skip_server = []  # test cases unimplemented by server
            if server_language:
                skip_server = server_language.unimplemented_test_cases_server()
            for language in languages:
                for test_case in _TEST_CASES:
                    if not test_case in language.unimplemented_test_cases():
                        if not test_case in skip_server:
                            test_job = cloud_to_cloud_jobspec(
                                language,
                                test_case,
                                server_name,
                                server_host,
                                server_port,
                                docker_image=docker_images.get(str(language)),
                                transport_security=args.transport_security,
                                manual_cmd_log=client_manual_cmd_log,
                            )
                            jobs.append(test_job)

            if args.http2_interop:
                for test_case in _HTTP2_TEST_CASES:
                    if server_name == "go":
                        # TODO(carl-mastrangelo): Reenable after https://github.com/grpc/grpc-go/issues/434
                        continue
                    test_job = cloud_to_cloud_jobspec(
                        http2Interop,
                        test_case,
                        server_name,
                        server_host,
                        server_port,
                        docker_image=docker_images.get(str(http2Interop)),
                        transport_security=args.transport_security,
                        manual_cmd_log=client_manual_cmd_log,
                    )
                    jobs.append(test_job)

        if args.http2_server_interop:
            if not args.manual_run:
                http2_server_job.wait_for_healthy(timeout_seconds=600)
            for language in languages_http2_clients_for_http2_server_interop:
                for test_case in set(_HTTP2_SERVER_TEST_CASES) - set(
                    _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS
                ):
                    offset = sorted(_HTTP2_SERVER_TEST_CASES).index(test_case)
                    server_port = _DEFAULT_SERVER_PORT + offset
                    if not args.manual_run:
                        server_port = http2_server_job.mapped_port(server_port)
                    test_job = cloud_to_cloud_jobspec(
                        language,
                        test_case,
                        str(http2InteropServer),
                        "localhost",
                        server_port,
                        docker_image=docker_images.get(str(language)),
                        manual_cmd_log=client_manual_cmd_log,
                    )
                    jobs.append(test_job)
            for language in languages:
                # HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS is a subset of
                # HTTP_SERVER_TEST_CASES, in which clients use their gRPC interop clients rather
                # than specialized http2 clients, reusing existing test implementations.
                # For example, in the "data_frame_padding" test, use language's gRPC
                # interop clients and make them think that they're running "large_unary"
                # test case. This avoids implementing a new test case in each language.
                for test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
                    if test_case not in language.unimplemented_test_cases():
                        offset = sorted(_HTTP2_SERVER_TEST_CASES).index(
                            test_case
                        )
                        server_port = _DEFAULT_SERVER_PORT + offset
                        if not args.manual_run:
                            server_port = http2_server_job.mapped_port(
                                server_port
                            )
                        if args.transport_security != "insecure":
                            print(
                                "Creating grpc client to http2 server test case "
                                "with insecure connection, even though "
                                "args.transport_security is not insecure. Http2 "
                                "test server only supports insecure connections."
                            )
                        test_job = cloud_to_cloud_jobspec(
                            language,
                            test_case,
                            str(http2InteropServer),
                            "localhost",
                            server_port,
                            docker_image=docker_images.get(str(language)),
                            transport_security="insecure",
                            manual_cmd_log=client_manual_cmd_log,
                        )
                        jobs.append(test_job)

        if not jobs:
            print("No jobs to run.")
            for image in sorted(set(six.itervalues(docker_images))):
                dockerjob.remove_image(image, skip_nonexistent=True)
            sys.exit(1)

        if args.manual_run:
            print("All tests will skipped --manual_run option is active.")

        if args.verbose:
            print("Jobs to run: \n%s\n" % "\n".join(str(job) for job in jobs))

        num_failures, resultset = jobset.run(
            jobs,
            newline_on_success=True,
            maxjobs=args.jobs,
            skip_jobs=args.manual_run,
        )
        if args.bq_result_table and resultset:
            upload_interop_results_to_bq(resultset, args.bq_result_table)
        if num_failures:
            jobset.message("FAILED", "Some tests failed", do_newline=True)
        else:
            jobset.message("SUCCESS", "All tests passed", do_newline=True)

        write_cmdlog_maybe(server_manual_cmd_log, "interop_server_cmds.sh")
        write_cmdlog_maybe(client_manual_cmd_log, "interop_client_cmds.sh")

        report_utils.render_junit_xml_report(resultset, _TESTS_XML_REPORT)

        if num_failures:
            sys.exit(1)
        else:
            sys.exit(0)
    finally:
        # Check if servers are still running.
        for server, job in list(server_jobs.items()):
            if not job.is_running():
                print('Server "%s" has exited prematurely.' % server)

        dockerjob.finish_jobs([j for j in six.itervalues(server_jobs)])

        for image in sorted(set(six.itervalues(docker_images))):
            if not args.manual_run:
                print("Removing docker image %s" % image)
                dockerjob.remove_image(image)
            else:
                print("Preserving docker image: %s" % image)


if __name__ == "__main__":
    main()
