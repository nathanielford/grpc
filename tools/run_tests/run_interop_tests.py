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

import os
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import tools.interop.runner as _runner  # noqa: E402
from tools.interop.runner import *  # noqa: E402,F401,F403
from tools.interop.runner import (  # noqa: E402,F401
    DOCKER_WORKDIR_ROOT,
    _AUTH_TEST_CASES,
    _CUSTOM_CREDENTIALS_TYPE_OPTIONS,
    _DEFAULT_SERVER_PORT,
    _DOCKER_BUILD_XML_REPORT,
    _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES,
    _HTTP2_SERVER_TEST_CASES,
    _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS,
    _HTTP2_TEST_CASES,
    _LANGUAGES,
    _LANGUAGES_FOR_ALTS_TEST_CASES,
    _LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES,
    _SERVERS,
    _SERVERS_FOR_ALTS_TEST_CASES,
    _TESTS_XML_REPORT,
    _TEST_CASES,
    _TEST_TIMEOUT,
    _TRANSPORT_SECURITY_OPTIONS,
    _job_kill_handler,
)

argp = _runner.argp
args = _runner.args
_INITIAL_ARGS = _runner.args


def _sync_args():
    if args is not _INITIAL_ARGS:
        _runner.args = args


def cloud_to_prod_jobspec(*pos, **kw):
    _sync_args()
    return _runner.cloud_to_prod_jobspec(*pos, **kw)


def cloud_to_cloud_jobspec(*pos, **kw):
    _sync_args()
    return _runner.cloud_to_cloud_jobspec(*pos, **kw)


def server_jobspec(*pos, **kw):
    _sync_args()
    return _runner.server_jobspec(*pos, **kw)


def main(cli_args=None):
    global args, _INITIAL_ARGS
    try:
        _runner.run_interop_tests_main(cli_args)
    finally:
        args = _runner.args
        _INITIAL_ARGS = _runner.args


if __name__ == "__main__":
    main()
