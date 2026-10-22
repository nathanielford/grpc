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
"""Run interop (cross-language) tests in parallel (delegates to tools.interop)."""

from __future__ import print_function

import atexit
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.interop.engine import (  # noqa: E402
    DockerImageBuilder,
    aggregate_http2_results,
    auth_options,
    bash_cmdline,
    build_interop_argparser,
    cloud_to_cloud_jobspec as _engine_cloud_to_cloud_jobspec,
    cloud_to_prod_jobspec as _engine_cloud_to_prod_jobspec,
    compute_engine_creds_required,
    docker_run_cmdline,
    get_spec,
    manual_cmdline,
    run_interop_cli,
    server_jobspec,
    write_cmdlog_maybe,
)

_SPEC = get_spec()
_BUILDER = DockerImageBuilder(_SPEC)

_DEFAULT_SERVER_PORT = _SPEC.defaults["default_server_port"]
_SKIP_CLIENT_COMPRESSION = _SPEC.test_case_groups["skip_client_compression"]
_SKIP_SERVER_COMPRESSION = _SPEC.test_case_groups["skip_server_compression"]
_SKIP_COMPRESSION = _SPEC.test_case_groups["skip_compression"]
_SKIP_ADVANCED = _SPEC.test_case_groups["skip_advanced"]
_SKIP_SPECIAL_STATUS_MESSAGE = _SPEC.test_case_groups[
    "skip_special_status_message"
]
_ORCA_TEST_CASES = _SPEC.test_case_groups["orca_test_cases"]
_GOOGLE_DEFAULT_CREDS_TEST_CASE = "google_default_credentials"
_SKIP_GOOGLE_DEFAULT_CREDS = _SPEC.test_case_groups["skip_google_default_creds"]
_COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE = "compute_engine_channel_credentials"
_SKIP_COMPUTE_ENGINE_CHANNEL_CREDS = _SPEC.test_case_groups[
    "skip_compute_engine_channel_creds"
]
_TEST_TIMEOUT = _SPEC.defaults["test_timeout_seconds"]
_SKIP_DATA_FRAME_PADDING = _SPEC.test_case_groups["skip_data_frame_padding"]
_DOCKER_BUILD_XML_REPORT = _SPEC.defaults["reports"]["docker_build_xml"]
_TESTS_XML_REPORT = _SPEC.defaults["reports"]["tests_xml"]
DOCKER_WORKDIR_ROOT = _SPEC.defaults["docker_workdir_root"]

# Dynamically bind legacy Language callables from interop.json with zero hardcoded language names
for _lspec in _SPEC.languages.values():
    if _lspec.legacy_class:
        globals()[_lspec.legacy_class] = (lambda s=_lspec: s)


def Http2Server():
    return _SPEC.http2_server_target


def Http2Client():
    return _SPEC.http2_client_target


_LANGUAGES = dict(_SPEC.languages)
_SERVERS = list(_SPEC.servers)
_TEST_CASES = list(_SPEC.standard_test_cases)
_AUTH_TEST_CASES = list(_SPEC.auth_test_cases)
_HTTP2_TEST_CASES = list(_SPEC.http2_client_test_cases)
_HTTP2_SERVER_TEST_CASES = list(_SPEC.http2_server_test_cases)
_GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES = dict(
    _SPEC.grpc_client_test_cases_for_http2_server_test_cases
)
_HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS = list(
    _SPEC.http2_server_test_cases_that_use_grpc_clients
)
_LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES = list(
    _SPEC.languages_with_http2_clients_for_http2_server_test_cases
)
_LANGUAGES_FOR_ALTS_TEST_CASES = list(_SPEC.languages_for_alts_test_cases)
_SERVERS_FOR_ALTS_TEST_CASES = list(_SPEC.servers_for_alts_test_cases)
_TRANSPORT_SECURITY_OPTIONS = list(_SPEC.transport_security_options)
_CUSTOM_CREDENTIALS_TYPE_OPTIONS = list(_SPEC.custom_credentials_type_options)
prod_servers = dict(_SPEC.prod_servers)

argp = build_interop_argparser(_SPEC)
args = argp.parse_args([])


def cloud_to_prod_jobspec(*pos_args, **kwargs):
    if "allow_flakes" not in kwargs or kwargs["allow_flakes"] is None:
        kwargs["allow_flakes"] = bool(getattr(args, "allow_flakes", False))
    return _engine_cloud_to_prod_jobspec(*pos_args, **kwargs)


def cloud_to_cloud_jobspec(*pos_args, **kwargs):
    if "allow_flakes" not in kwargs or kwargs["allow_flakes"] is None:
        kwargs["allow_flakes"] = bool(getattr(args, "allow_flakes", False))
    return _engine_cloud_to_cloud_jobspec(*pos_args, **kwargs)


def build_interop_image_jobspec(language, tag=None):
    """Creates jobspec for building interop docker image for a language."""
    return _BUILDER.build_interop_image_jobspec(language, tag=tag)


if __name__ == "__main__":
    atexit.register(lambda: subprocess.call(["stty", "echo"]))
    os.chdir(ROOT)
    sys.exit(run_interop_cli())
