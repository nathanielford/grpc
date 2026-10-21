#!/usr/bin/env python3
# Copyright 2026 gRPC authors.
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
"""Exhaustive verification suite for capability-driven interop and OCI image metadata."""

import ast
import contextlib
import io
import os
import re
import subprocess
import sys
import types
import unittest
from unittest import mock

RUN_TESTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(RUN_TESTS_DIR, "../.."))
INTEROP_MATRIX_DIR = os.path.join(REPO_ROOT, "tools/interop_matrix")
PYTHON_TESTS_DIR = os.path.join(REPO_ROOT, "src/python/grpcio_tests")

for path in (
    RUN_TESTS_DIR,
    os.path.join(RUN_TESTS_DIR, "python_utils"),
    INTEROP_MATRIX_DIR,
    PYTHON_TESTS_DIR,
):
    if path not in sys.path:
        sys.path.insert(0, path)

# Provide a lightweight stub for optional google-api-python-client ('apiclient')
# if not installed in the local test environment.
try:
    import apiclient  # type: ignore  # noqa: F401
except ImportError:
    _apiclient_stub = types.ModuleType("apiclient")
    _apiclient_stub.discovery = types.ModuleType("apiclient.discovery")
    _apiclient_stub.errors = types.ModuleType("apiclient.errors")
    _apiclient_stub.errors.HttpError = Exception
    sys.modules["apiclient"] = _apiclient_stub
    sys.modules["apiclient.discovery"] = _apiclient_stub.discovery
    sys.modules["apiclient.errors"] = _apiclient_stub.errors

try:
    from oauth2client.client import GoogleCredentials  # type: ignore  # noqa: F401
except ImportError:
    _oauth2_stub = types.ModuleType("oauth2client")
    _oauth2_client_stub = types.ModuleType("oauth2client.client")
    _oauth2_client_stub.GoogleCredentials = object
    _oauth2_stub.client = _oauth2_client_stub
    sys.modules["oauth2client"] = _oauth2_stub
    sys.modules["oauth2client.client"] = _oauth2_client_stub

try:
    from setuptools import errors as _setuptools_errors  # type: ignore  # noqa: F401
except ImportError:
    _setuptools_stub = types.ModuleType("setuptools")
    _setuptools_stub.Command = object
    _setuptools_stub.errors = types.SimpleNamespace(
        DistutilsError=Exception,
        DistutilsOptionError=Exception,
    )
    _setuptools_cmd_stub = types.ModuleType("setuptools.command")
    for _sub in ("build_ext", "build_py", "easy_install", "install", "test"):
        _mod = types.ModuleType(f"setuptools.command.{_sub}")
        setattr(_mod, _sub, object)
        setattr(_setuptools_cmd_stub, _sub, _mod)
        sys.modules[f"setuptools.command.{_sub}"] = _mod
    _setuptools_stub.command = _setuptools_cmd_stub
    sys.modules["setuptools"] = _setuptools_stub
    sys.modules["setuptools.command"] = _setuptools_cmd_stub

import client_matrix
import create_matrix_images
from python_utils.interop_capabilities import (
    OCI_LABEL_CAPABILITIES,
    OCI_LABEL_CLIENT_CMD,
    OCI_LABEL_ENV,
    OCI_LABEL_HTTP2_CLIENT_CMD,
    OCI_LABEL_KEYS,
    OCI_LABEL_RELEASE,
    OCI_LABEL_SERVER_CMD,
    OCI_LABEL_WORKDIR,
    Capability,
    ExternalServerEndpoint,
    OciImageMetadata,
    ProdServerEndpoint,
    get_base_name_image_metadata,
    get_matrix_image_metadata,
    resolve_image_metadata,
)
import run_interop_matrix_tests
import run_interop_tests


# Reference legacy negative skip-list tables for ground-truth mathematical comparison
_LEGACY_CLIENT_UNIMPLEMENTED = {
    "c++": (
        run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
    ),
    "aspnetcore": (
        run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "dart": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "java": [],
    "javaokhttp": run_interop_tests._SKIP_DATA_FRAME_PADDING,
    "go": run_interop_tests._SKIP_COMPRESSION,
    "node": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "nodepurejs": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "php7": (
        run_interop_tests._SKIP_SERVER_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "objc": (
        run_interop_tests._TEST_CASES[1:]
        + run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "ruby": (
        run_interop_tests._SKIP_SERVER_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "python": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "pythonasyncio": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._AUTH_TEST_CASES
        + ["timeout_on_sleeping_server"]
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "http2_client": (
        run_interop_tests._TEST_CASES
        + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
    ),
    "http2_server": (
        run_interop_tests._TEST_CASES
        + run_interop_tests._SKIP_DATA_FRAME_PADDING
        + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
        + run_interop_tests._SKIP_GOOGLE_DEFAULT_CREDS
        + run_interop_tests._SKIP_COMPUTE_ENGINE_CHANNEL_CREDS
    ),
}

_LEGACY_SERVER_UNIMPLEMENTED = {
    "c++": [],
    "aspnetcore": run_interop_tests._ORCA_TEST_CASES,
    "dart": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "java": (
        run_interop_tests._SKIP_CLIENT_COMPRESSION
        + ["server_compressed_streaming"]
    ),
    "go": run_interop_tests._SKIP_COMPRESSION,
    "node": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "nodepurejs": run_interop_tests._ORCA_TEST_CASES,
    "php7": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "objc": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "ruby": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "python": (
        run_interop_tests._SKIP_COMPRESSION
        + run_interop_tests._ORCA_TEST_CASES
    ),
    "pythonasyncio": (
        run_interop_tests._TEST_CASES
        + run_interop_tests._AUTH_TEST_CASES
        + run_interop_tests._HTTP2_TEST_CASES
        + run_interop_tests._HTTP2_SERVER_TEST_CASES
    ),
    "http2_client": run_interop_tests._TEST_CASES,
    "http2_server": run_interop_tests._TEST_CASES,
}


def _legacy_compute_engine_creds_required(lang_name, test_case):
    if test_case == "compute_engine_creds":
        return True
    if test_case == "oauth2_auth_token" and lang_name == "c++":
        return True
    return False


def _legacy_auth_options(
    lang_name,
    test_case,
    google_default_creds_use_key_file,
    service_account_key_file,
    default_service_account,
):
    cmdargs = []
    env = {}
    oauth_scope_arg = "--oauth_scope=https://www.googleapis.com/auth/xapi.zoo"
    key_file_arg = "--service_account_key_file=%s" % service_account_key_file
    default_account_arg = (
        "--default_service_account=%s" % default_service_account
    )
    if test_case in ["jwt_token_creds", "per_rpc_creds", "oauth2_auth_token"]:
        if lang_name in [
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
    if test_case == "oauth2_auth_token" and lang_name == "c++":
        cmdargs += [default_account_arg]
    if test_case == "compute_engine_creds":
        cmdargs += [oauth_scope_arg, default_account_arg]
    if test_case == "google_default_credentials":
        if google_default_creds_use_key_file:
            env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
        cmdargs += [default_account_arg]
    if test_case == "compute_engine_channel_credentials":
        cmdargs += [default_account_arg]
    return (cmdargs, env)


class CapabilityMatrixEquivalenceTest(unittest.TestCase):
    """Proves 100% mathematical equivalence between Capability model and legacy matrix."""

    def test_backward_compatible_unimplemented_test_cases_all_endpoints(self):
        for name, lang in run_interop_tests._LANGUAGES.items():
            self.assertEqual(
                set(lang.unimplemented_test_cases()),
                set(_LEGACY_CLIENT_UNIMPLEMENTED[name]),
                f"Client unimplemented_test_cases mismatch for {name}",
            )
            if name in _LEGACY_SERVER_UNIMPLEMENTED:
                self.assertEqual(
                    set(lang.unimplemented_test_cases_server()),
                    set(_LEGACY_SERVER_UNIMPLEMENTED[name]),
                    f"Server unimplemented_test_cases_server mismatch for {name}",
                )

        h2_client = run_interop_tests.Http2Client()
        self.assertEqual(
            set(h2_client.unimplemented_test_cases()),
            set(_LEGACY_CLIENT_UNIMPLEMENTED["http2_client"]),
        )
        self.assertEqual(
            set(h2_client.unimplemented_test_cases_server()),
            set(_LEGACY_SERVER_UNIMPLEMENTED["http2_client"]),
        )

        h2_server = run_interop_tests.Http2Server()
        self.assertEqual(
            set(h2_server.unimplemented_test_cases()),
            set(_LEGACY_CLIENT_UNIMPLEMENTED["http2_server"]),
        )
        self.assertEqual(
            set(h2_server.unimplemented_test_cases_server()),
            set(_LEGACY_SERVER_UNIMPLEMENTED["http2_server"]),
        )

    def test_cloud_to_cloud_matrix_equivalence(self):
        legacy_servers = [
            "c++",
            "node",
            "aspnetcore",
            "java",
            "go",
            "ruby",
            "python",
            "dart",
            "pythonasyncio",
            "php7",
        ]
        legacy_alts_clients = {"java", "go", "c++", "python"}
        legacy_alts_servers = {"java", "go", "c++", "python"}

        checked = 0
        for c_name, client in run_interop_tests._LANGUAGES.items():
            for s_name in legacy_servers:
                server = run_interop_tests.get_server_endpoint(s_name)
                for tc in run_interop_tests._TEST_CASES:
                    for ts in run_interop_tests._TRANSPORT_SECURITY_OPTIONS:
                        legacy_runs = (
                            (
                                ts != "alts"
                                or (
                                    c_name in legacy_alts_clients
                                    and s_name in legacy_alts_servers
                                )
                            )
                            and tc not in _LEGACY_CLIENT_UNIMPLEMENTED[c_name]
                            and tc not in _LEGACY_SERVER_UNIMPLEMENTED[s_name]
                        )
                        cap_runs = client.supports(
                            tc, ts
                        ) and server.supports(tc, ts)
                        self.assertEqual(
                            cap_runs,
                            legacy_runs,
                            f"C2C mismatch on {(c_name, s_name, tc, ts)}",
                        )
                        checked += 1
        self.assertEqual(checked, 13 * 10 * 20 * 3)

    def test_cloud_to_prod_matrix_equivalence(self):
        skip_c2p = set(
            run_interop_tests._SKIP_ADVANCED
            + run_interop_tests._SKIP_COMPRESSION
            + run_interop_tests._SKIP_SPECIAL_STATUS_MESSAGE
            + run_interop_tests._ORCA_TEST_CASES
        )
        checked = 0
        for srv_nick, srv_host in run_interop_tests.prod_servers.items():
            prod_server = ProdServerEndpoint(srv_nick, srv_host)
            for c_name, client in run_interop_tests._LANGUAGES.items():
                for tc in run_interop_tests._TEST_CASES:
                    for cred in run_interop_tests._CUSTOM_CREDENTIALS_TYPE_OPTIONS:
                        legacy_runs = (
                            tc not in _LEGACY_CLIENT_UNIMPLEMENTED[c_name]
                            and tc not in skip_c2p
                            and not (
                                cred == "google_default_credentials"
                                and c_name
                                not in ["c++", "go", "java", "javaokhttp"]
                            )
                            and not (
                                cred == "compute_engine_channel_creds"
                                and c_name not in ["go", "java", "javaokhttp"]
                            )
                        )
                        cap_runs = client.supports(
                            tc, cred
                        ) and prod_server.supports(tc, cred)
                        self.assertEqual(
                            cap_runs,
                            legacy_runs,
                            f"C2P mismatch on {(c_name, srv_nick, tc, cred)}",
                        )
                        checked += 1
        self.assertEqual(checked, 2 * 13 * 20 * 3)

    def test_cloud_to_prod_auth_matrix_and_options_equivalence(self):
        checked = 0
        for srv_nick, srv_host in run_interop_tests.prod_servers.items():
            prod_server = ProdServerEndpoint(srv_nick, srv_host)
            for c_name, client in run_interop_tests._LANGUAGES.items():
                for tc in run_interop_tests._AUTH_TEST_CASES:
                    self.assertEqual(
                        run_interop_tests.compute_engine_creds_required(
                            client, tc
                        ),
                        _legacy_compute_engine_creds_required(c_name, tc),
                    )
                    for use_key_file in (False, True):
                        self.assertEqual(
                            run_interop_tests.auth_options(
                                client,
                                tc,
                                use_key_file,
                                "/path/to/key.json",
                                "acct@gserviceaccount.com",
                            ),
                            _legacy_auth_options(
                                c_name,
                                tc,
                                use_key_file,
                                "/path/to/key.json",
                                "acct@gserviceaccount.com",
                            ),
                        )
                    for skip_gce in (False, True):
                        for cred in run_interop_tests._CUSTOM_CREDENTIALS_TYPE_OPTIONS:
                            if tc == "google_default_credentials":
                                ts = "google_default_credentials"
                            elif tc == "compute_engine_channel_credentials":
                                ts = "compute_engine_channel_creds"
                            else:
                                ts = "tls"
                            legacy_runs = (
                                ts == cred
                                and (
                                    not skip_gce
                                    or not _legacy_compute_engine_creds_required(
                                        c_name, tc
                                    )
                                )
                                and tc
                                not in _LEGACY_CLIENT_UNIMPLEMENTED[c_name]
                            )
                            cap_runs = (
                                ts == cred
                                and client.supports(
                                    tc,
                                    ts,
                                    skip_compute_engine_creds=skip_gce,
                                )
                                and prod_server.supports(tc, ts)
                            )
                            self.assertEqual(
                                cap_runs,
                                legacy_runs,
                                f"C2P Auth mismatch on {(c_name, srv_nick, tc, cred, skip_gce)}",
                            )
                            checked += 1
        self.assertEqual(checked, 2 * 13 * 6 * 2 * 3)

    def test_http2_client_and_server_interop_equivalence(self):
        h2_client = run_interop_tests.Http2Client()
        h2_server = run_interop_tests.Http2Server()

        # HTTP/2 bad client vs cloud-to-cloud servers
        for s_name in run_interop_tests._SERVERS:
            server = run_interop_tests.get_server_endpoint(s_name)
            for tc in run_interop_tests._HTTP2_TEST_CASES:
                legacy_runs = s_name != "go"
                cap_runs = h2_client.supports(
                    tc, "tls"
                ) and server.supports(tc, "tls")
                self.assertEqual(
                    cap_runs,
                    legacy_runs,
                    f"HTTP2 client mismatch on {(s_name, tc)}",
                )

        # HTTP/2 server interop vs all 13 clients
        legacy_h2_badserver_clients = {"java", "go", "python", "c++"}
        for c_name, client in run_interop_tests._LANGUAGES.items():
            for tc in run_interop_tests._HTTP2_SERVER_TEST_CASES:
                if (
                    tc
                    in run_interop_tests._HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS
                ):
                    legacy_runs = (
                        tc not in _LEGACY_CLIENT_UNIMPLEMENTED[c_name]
                    )
                    cap_runs = client.supports(
                        tc, "insecure"
                    ) and h2_server.supports(tc, "insecure")
                else:
                    legacy_runs = c_name in legacy_h2_badserver_clients
                    cap_runs = client.supports(
                        tc, "tls"
                    ) and h2_server.supports(tc, "tls")
                self.assertEqual(
                    cap_runs,
                    legacy_runs,
                    f"HTTP2 server interop mismatch on {(c_name, tc)}",
                )

    def test_jobspec_builders_and_override_server(self):
        cxx = run_interop_tests._LANGUAGES["c++"]
        spec_c2p = run_interop_tests.cloud_to_prod_jobspec(
            cxx,
            "large_unary",
            "default",
            "grpc-test.sandbox.googleapis.com",
            google_default_creds_use_key_file=False,
            docker_image="grpc_interop_cxx:test",
            auth=True,
            service_account_key_file="/root/key.json",
            default_service_account="sa@example.com",
            transport_security="tls",
            allow_flakes=True,
        )
        self.assertEqual(
            spec_c2p.shortname,
            "cloud_to_prod_auth:c++:default:large_unary:tls",
        )
        self.assertEqual(spec_c2p.flake_retries, 4)
        self.assertEqual(spec_c2p.timeout_retries, 2)

        spec_orca = run_interop_tests.cloud_to_cloud_jobspec(
            cxx,
            "orca_per_rpc",
            "java",
            "localhost",
            8080,
            docker_image="grpc_interop_cxx:test",
            transport_security="tls",
        )
        self.assertEqual(
            spec_orca.shortname,
            "cloud_to_cloud:c++:java_server:orca_per_rpc:tls",
        )
        self.assertTrue(
            any("test_backend_metrics_load_balancer" in s for s in spec_orca.cmdline)
        )

        ext_srv = run_interop_tests.get_server_endpoint("custom_override")
        self.assertIsInstance(ext_srv, ExternalServerEndpoint)
        self.assertTrue(ext_srv.supports("orca_oob", "tls"))


class BugFixesVerificationTest(unittest.TestCase):
    """Verifies the 3 bug fixes required in Section 3."""

    def test_pythonasyncio_client_bug_fixed_in_commands_py(self):
        import commands

        dist = types.SimpleNamespace(
            get_option_dict=lambda _: {},
            verbose=1,
            dry_run=0,
        )
        cmd = commands.RunInterop.__new__(commands.RunInterop)
        cmd.initialize_options()
        cmd.client = True
        cmd.use_asyncio = True
        cmd.args = "--server_host=localhost --server_port=8080 --test_case=large_unary"

        called = {"aio_client": False}

        async def _fake_aio_test_interoperability():
            called["aio_client"] = True
            called["argv"] = list(sys.argv[1:])

        fake_aio_client_mod = types.SimpleNamespace(
            test_interoperability=_fake_aio_test_interoperability
        )
        fake_tests_aio_interop = types.SimpleNamespace(
            client=fake_aio_client_mod
        )
        fake_tests_aio = types.SimpleNamespace(interop=fake_tests_aio_interop)

        with mock.patch.dict(
            sys.modules,
            {
                "tests_aio": fake_tests_aio,
                "tests_aio.interop": fake_tests_aio_interop,
                "tests_aio.interop.client": fake_aio_client_mod,
            },
        ):
            cmd.run_client()

        self.assertTrue(
            called["aio_client"],
            "RunInterop.run_client() with use_asyncio=True must invoke tests_aio.interop.client.test_interoperability()",
        )
        self.assertEqual(
            called["argv"],
            [
                "--server_host=localhost",
                "--server_port=8080",
                "--test_case=large_unary",
            ],
        )

    def test_idle_pythonasyncio_server_removed_from_servers(self):
        self.assertNotIn("pythonasyncio", run_interop_tests._SERVERS)
        self.assertEqual(
            sorted(run_interop_tests._SERVERS),
            sorted(
                [
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
            ),
        )
        py_aio = run_interop_tests._LANGUAGES["pythonasyncio"]
        self.assertFalse(
            py_aio.has_capability(Capability.CORE_INTEROP, role="server")
        )

    def test_duplicate_java_and_http2_docker_build_jobs_deduplicated(self):
        languages_to_build = {
            run_interop_tests._LANGUAGES["java"],
            run_interop_tests._LANGUAGES["javaokhttp"],
            run_interop_tests._LANGUAGES["c++"],
            run_interop_tests._LANGUAGES["objc"],
            run_interop_tests.Http2Client(),
            run_interop_tests.Http2Server(),
        }
        build_jobs, docker_images = run_interop_tests.create_docker_build_jobs(
            languages_to_build
        )
        shortnames = [j.shortname for j in build_jobs]
        # Must build cxx, http2, and java exactly once each (3 jobs total, no objc, no duplicate java or http2)
        self.assertEqual(
            sorted(shortnames),
            ["build_docker_cxx", "build_docker_http2", "build_docker_java"],
        )
        self.assertEqual(
            docker_images["java"],
            docker_images["javaokhttp"],
            "java and javaokhttp must share the single built grpc_interop_java image tag",
        )
        self.assertIn("http2", docker_images)
        self.assertNotIn("objc", docker_images)


class OciMetadataAndMatrixTest(unittest.TestCase):
    """Verifies self-describing OCI image labels, Dockerfile generation, and matrix job generation."""

    def test_oci_labels_roundtrip_all_languages(self):
        for name, lang in run_interop_tests._LANGUAGES.items():
            if name == "objc":
                continue
            meta = lang.to_oci_metadata(release="v1.76.0")
            labels = meta.to_oci_labels()
            for key in OCI_LABEL_KEYS:
                self.assertIn(key, labels, f"Missing {key} for {name}")
            parsed = OciImageMetadata.from_oci_labels(labels)
            self.assertIsNotNone(parsed)
            self.assertEqual(parsed.client_cmd, meta.client_cmd)
            self.assertEqual(parsed.server_cmd, meta.server_cmd)
            self.assertEqual(parsed.http2_client_cmd, meta.http2_client_cmd)
            self.assertEqual(parsed.workdir, meta.workdir)
            self.assertEqual(parsed.env, meta.env)
            self.assertEqual(parsed.capabilities, meta.capabilities)
            self.assertEqual(
                parsed.server_capabilities, meta.server_capabilities
            )
            self.assertEqual(parsed.release, "v1.76.0")

    def test_single_pass_dockerfile_generation(self):
        meta = get_base_name_image_metadata(
            "grpc_interop_python", release="v1.75.1"
        )
        dockerfile = meta.generate_dockerfile(
            base_image="us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_python:test",
            base_name="grpc_interop_python",
            propagate_env=["KOKORO_BUILD_ID"],
            include_commit_log=True,
        )
        self.assertIn(
            "FROM us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_python:test",
            dockerfile,
        )
        for key in OCI_LABEL_KEYS:
            self.assertIn(f"LABEL {key}=", dockerfile)
        self.assertIn('LABEL release="v1.75.1"', dockerfile)
        self.assertIn("ARG KOKORO_BUILD_ID", dockerfile)
        self.assertIn(
            "COPY commit_log /var/local/build_info/commit_log", dockerfile
        )
        self.assertIn(
            'RUN bash -l "/var/local/jenkins/grpc/tools/dockerfile/interoptest/grpc_interop_python/build_interop.sh"',
            dockerfile,
        )

        # Also test CLI --emit-dockerfile execution used by build_interop_image.sh
        cap_script = os.path.join(
            RUN_TESTS_DIR, "python_utils/interop_capabilities.py"
        )
        out = subprocess.check_output(
            [
                sys.executable,
                cap_script,
                "--emit-dockerfile",
                "--base-name=grpc_interop_cxx",
                "--base-image=base_cxx:latest",
                "--release=v1.76.0",
                "--include-commit-log",
            ]
        ).decode("utf-8")
        self.assertIn("FROM base_cxx:latest", out)
        self.assertIn("LABEL io.grpc.interop.client_cmd=", out)
        self.assertIn(
            "COPY commit_log /var/local/build_info/commit_log", out
        )

    def test_create_matrix_images_single_pass_jobspec(self):
        job = create_matrix_images.build_image_jobspec(
            runtime="go1.19",
            env={"GRPC_GO_ROOT": "/tmp/grpc-go"},
            gcr_tag="v1.60.1",
            stack_base="/tmp/grpc-go",
            lang="go",
        )
        self.assertEqual(job.shortname, "build_docker_go1.19")
        self.assertEqual(
            job.environ["INTEROP_RELEASE"],
            "v1.60.1",
        )
        self.assertEqual(
            job.environ["COMMIT_LOG_FILE"],
            "/tmp/grpc-go/commit_log",
        )
        for key in OCI_LABEL_KEYS:
            self.assertIn(key, job.oci_labels)

    def test_matrix_jobspecs_equivalence_across_all_releases(self):
        """Verifies matrix JobSpec generation across all 520 (lang, runtime, release) entries."""
        total_releases_checked = 0
        with contextlib.redirect_stdout(io.StringIO()):
            for lang in client_matrix.LANG_RUNTIME_MATRIX:
                for release in client_matrix.get_release_tags(lang):
                    for runtime in client_matrix.get_runtimes_for_lang_release(
                        lang, release
                    ):
                        suite_name = f"{lang}__{runtime}_{release}"
                        # 1. Generate via fallback catalog (simulating pre-OCI GCR image)
                        specs_fallback = run_interop_matrix_tests._generate_test_case_jobspecs(
                            lang,
                            runtime,
                            release,
                            suite_name,
                            server_host="74.125.206.210",
                            allow_flakes=False,
                        )
                        # 2. Generate via OCI labels (simulating self-describing OCI image)
                        meta = get_matrix_image_metadata(lang, runtime, release)
                        specs_oci = run_interop_matrix_tests._generate_test_case_jobspecs(
                            lang,
                            runtime,
                            release,
                            suite_name,
                            oci_labels=meta.to_oci_labels(),
                            server_host="74.125.206.210",
                            allow_flakes=False,
                        )
                        self.assertEqual(len(specs_fallback), 18)
                        self.assertEqual(len(specs_oci), 18)
                        for sf, so in zip(specs_fallback, specs_oci):
                            self.assertEqual(sf.shortname, so.shortname)

                        # 3. Cross-check against the legacy checked-in testcase file
                        rinfo = client_matrix.LANG_RELEASE_MATRIX[lang].get(
                            release
                        )
                        tc_file = (
                            rinfo.testcases_file
                            if (rinfo and rinfo.testcases_file)
                            else f"{lang}__master"
                        )
                        if lang == "csharp" and runtime == "csharpcoreclr":
                            tc_file = tc_file.replace(
                                "csharp_", "csharpcoreclr_"
                            )
                        tc_path = os.path.join(
                            INTEROP_MATRIX_DIR, "testcases", tc_file
                        )
                        with open(tc_path) as f:
                            legacy_lines = [
                                l.strip()
                                for l in f
                                if l.strip()
                                and not l.strip().startswith("#")
                                and not l.strip().startswith("echo")
                            ]
                        self.assertEqual(len(legacy_lines), len(specs_fallback))
                        for line, spec in zip(legacy_lines, specs_fallback):
                            tc_m = re.search(r"--test_case=(\w+)", line)
                            self.assertTrue(
                                spec.shortname.endswith(f":{tc_m.group(1)}")
                            )
                            w_m = re.search(r"-w (\S+)", line)
                            self.assertIn(f"-w {w_m.group(1)}", spec.cmdline)
                            self.assertIn(
                                "--server_host=74.125.206.210", spec.cmdline
                            )
                            self.assertIn(
                                "--server_host_override=", spec.cmdline
                            )
                        total_releases_checked += 1

        self.assertGreaterEqual(total_releases_checked, 280)

    def test_no_duplicate_keys_in_client_matrix_ast(self):
        """Verifies client_matrix.py has no duplicate dictionary keys (e.g. csharp v1.20.0)."""
        matrix_path = os.path.join(INTEROP_MATRIX_DIR, "client_matrix.py")
        with open(matrix_path) as f:
            tree = ast.parse(f.read(), filename=matrix_path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                seen_keys = set()
                for k in node.keys:
                    if isinstance(k, ast.Constant) and isinstance(k.value, str):
                        self.assertNotIn(
                            k.value,
                            seen_keys,
                            f"Duplicate dictionary key {k.value!r} in client_matrix.py",
                        )
                        seen_keys.add(k.value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
