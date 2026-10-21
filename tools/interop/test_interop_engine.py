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
"""Unit and parity tests for the declarative gRPC Interop Engine (tools.interop)."""

import argparse
import copy
import glob
import io
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

import yaml

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, os.path.join(REPO_ROOT, "tools/run_tests"))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools/interop_matrix"))

from tools.interop import builder
from tools.interop import cli
from tools.interop import matrix
from tools.interop import runner
from tools.interop import spec
import tools.interop_matrix.client_matrix as legacy_client_matrix
import tools.run_tests.run_interop_tests as legacy_run_interop


class TestInteropSpecValidation(unittest.TestCase):
    """Tests YAML schema validation and loading."""

    def setUp(self):
        self.spec = spec.InteropSpec.load()
        with open(spec.DEFAULT_SPEC_PATH, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f)

    def test_default_spec_loads_and_validates(self):
        self.assertEqual(self.spec.schema_version, "1.0")
        self.assertEqual(len(self.spec.all_targets), 14)
        self.assertEqual(len(self.spec.languages), 13)
        self.assertIn("http2", self.spec.all_targets)

    def test_rejects_wrong_schema_version(self):
        bad = copy.deepcopy(self.raw_data)
        bad["schema_version"] = 99
        with self.assertRaises(spec.SpecValidationError):
            spec.validate_spec_data(bad)

    def test_rejects_missing_top_level_key(self):
        bad = copy.deepcopy(self.raw_data)
        del bad["languages"]
        with self.assertRaises(spec.SpecValidationError):
            spec.validate_spec_data(bad)

    def test_rejects_unknown_unimplemented_test_case(self):
        bad = copy.deepcopy(self.raw_data)
        bad["languages"]["c++"]["unimplemented_client_cases"].append(
            "nonexistent_test_case"
        )
        with self.assertRaises(spec.SpecValidationError):
            spec.validate_spec_data(bad)

    def test_rejects_unknown_server_unimplemented_test_case(self):
        bad = copy.deepcopy(self.raw_data)
        bad["languages"]["c++"]["unimplemented_server_cases"].append(
            "nonexistent_server_case"
        )
        with self.assertRaises(spec.SpecValidationError):
            spec.validate_spec_data(bad)

    def test_rejects_invalid_auth_credential_mode(self):
        bad = copy.deepcopy(self.raw_data)
        bad["languages"]["c++"]["auth_credentials_mode"] = "magic_beans"
        with self.assertRaises(spec.SpecValidationError):
            spec.validate_spec_data(bad)

    def test_rejects_invalid_matrix_profile_reference(self):
        bad = copy.deepcopy(self.raw_data)
        bad["matrix"]["languages"]["cxx"]["releases"]["v1.0.1"][
            "testcases_file"
        ] = "nonexistent_profile"
        with self.assertRaises(spec.SpecValidationError):
            spec.validate_spec_data(bad)


class TestLanguageAndJobspecParity(unittest.TestCase):
    """Tests 100% parity with original language classes and jobspec helpers."""

    def setUp(self):
        self.spec = spec.InteropSpec.load()
        self.runner = runner.InteropRunner(self.spec)

    def test_language_set_and_names(self):
        expected_names = {
            "c++",
            "aspnetcore",
            "dart",
            "go",
            "java",
            "javaokhttp",
            "node",
            "nodepurejs",
            "php7",
            "python",
            "pythonasyncio",
            "ruby",
            "objc",
        }
        self.assertEqual(set(legacy_run_interop._LANGUAGES.keys()), expected_names)
        for name, lang_obj in legacy_run_interop._LANGUAGES.items():
            self.assertEqual(str(lang_obj), name)

    def test_safenames_and_cwds(self):
        expected = {
            "c++": ("cxx", None, None, None),
            "aspnetcore": (
                "aspnetcore",
                "../grpc-dotnet/output/InteropTestsClient",
                "../grpc-dotnet/output/InteropTestsWebsite",
                None,
            ),
            "dart": (
                "dart",
                "../grpc-dart/interop",
                "../grpc-dart/interop",
                "../grpc-dart/interop",
            ),
            "go": (
                "go",
                "/go/src/google.golang.org/grpc/interop/client",
                "/go/src/google.golang.org/grpc/interop/server",
                "/go/src/google.golang.org/grpc/interop/http2",
            ),
            "java": ("java", "../grpc-java", "../grpc-java", "../grpc-java"),
            "javaokhttp": (
                "java",
                "../grpc-java",
                None,
                None,
            ),
            "node": (
                "node",
                "../../../../home/appuser/grpc-node",
                "../../../../home/appuser/grpc-node",
                None,
            ),
            "nodepurejs": (
                "nodepurejs",
                "../../../../home/appuser/grpc-node",
                "../../../../home/appuser/grpc-node",
                None,
            ),
            "php7": ("php7", None, None, None),
            "python": ("python", None, None, None),
            "pythonasyncio": ("pythonasyncio", None, None, None),
            "ruby": ("ruby", None, None, None),
            "objc": ("objc", "src/objective-c/tests", None, None),
        }
        for name, (
            safename,
            client_cwd,
            server_cwd,
            http2_cwd,
        ) in expected.items():
            lang = legacy_run_interop._LANGUAGES[name]
            self.assertEqual(lang.safename, safename)
            self.assertEqual(lang.client_cwd, client_cwd)
            self.assertEqual(lang.server_cwd, server_cwd)
            self.assertEqual(lang.http2_cwd, http2_cwd)

    def test_client_server_http2_commands(self):
        args = ["--foo=bar", "--baz=1"]
        self.assertEqual(
            legacy_run_interop._LANGUAGES["c++"].client_cmd(args),
            ["cmake/build/interop_client", "--foo=bar", "--baz=1"],
        )
        self.assertEqual(
            legacy_run_interop._LANGUAGES["pythonasyncio"].client_cmd(args),
            [
                "py39/bin/python",
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--use-asyncio",
                "--client",
                '--args="--foo=bar --baz=1"',
            ],
        )
        self.assertEqual(
            legacy_run_interop._LANGUAGES["php7"].client_cmd(args),
            ["src/php/bin/interop_client.sh", "--foo=bar", "--baz=1"],
        )
        self.assertEqual(
            legacy_run_interop._LANGUAGES["ruby"].client_cmd(args),
            [
                "tools/run_tests/interop/with_rvm.sh",
                "ruby",
                "src/ruby/pb/test/client.rb",
                "--foo=bar",
                "--baz=1",
            ],
        )
        self.assertEqual(
            legacy_run_interop._LANGUAGES["javaokhttp"].client_cmd(args),
            [
                "./run-test-client.sh",
                "--use_okhttp=true",
                "--foo=bar",
                "--baz=1",
            ],
        )
        self.assertEqual(
            legacy_run_interop._LANGUAGES["java"].client_cmd_http2interop(args),
            [
                "./interop-testing/build/install/grpc-interop-testing/bin/http2-client",
                "--foo=bar",
                "--baz=1",
            ],
        )

    def test_http2_client_vs_server_unimplemented_parity(self):
        http2_client = legacy_run_interop.Http2Client()
        http2_server = legacy_run_interop.Http2Server()
        self.assertNotIn(
            "data_frame_padding", http2_client.unimplemented_test_cases()
        )
        self.assertIn(
            "data_frame_padding", http2_server.unimplemented_test_cases()
        )
        self.assertEqual(
            set(http2_server.unimplemented_test_cases())
            - set(http2_client.unimplemented_test_cases()),
            {"data_frame_padding"},
        )

    def test_auth_options_parity(self):
        sa_key_file = "/path/to/creds.json"
        default_sa = "default-sa@developer.gserviceaccount.com"
        oauth_scope = "https://www.googleapis.com/auth/xapi.zoo"

        # env-based credential delivery (aspnetcore, node, nodepurejs, php7, ruby, python, pythonasyncio)
        for name in (
            "aspnetcore",
            "node",
            "nodepurejs",
            "php7",
            "ruby",
            "python",
            "pythonasyncio",
        ):
            lang = legacy_run_interop._LANGUAGES[name]
            cmdline, env = legacy_run_interop.auth_options(
                lang, "jwt_token_creds", False, sa_key_file, default_sa
            )
            self.assertEqual(cmdline, [])
            self.assertEqual(
                env, {"GOOGLE_APPLICATION_CREDENTIALS": sa_key_file}
            )

            cmdline_gce, env_gce = legacy_run_interop.auth_options(
                lang, "compute_engine_creds", True, sa_key_file, default_sa
            )
            self.assertEqual(
                cmdline_gce,
                [
                    f"--oauth_scope={oauth_scope}",
                    f"--default_service_account={default_sa}",
                ],
            )
            self.assertEqual(env_gce, {})

        # c++ uses flag-based key delivery AND passes --default_service_account on oauth2_auth_token
        cxx = legacy_run_interop._LANGUAGES["c++"]
        cmdline_cxx, env_cxx = legacy_run_interop.auth_options(
            cxx, "oauth2_auth_token", False, sa_key_file, default_sa
        )
        self.assertEqual(
            cmdline_cxx,
            [
                f"--service_account_key_file={sa_key_file}",
                f"--oauth_scope={oauth_scope}",
                f"--default_service_account={default_sa}",
            ],
        )
        self.assertEqual(env_cxx, {})

        # flag-based credential delivery (go, java, javaokhttp, dart, objc)
        for name in ("go", "java", "javaokhttp", "dart", "objc"):
            lang = legacy_run_interop._LANGUAGES[name]
            cmdline, env = legacy_run_interop.auth_options(
                lang, "oauth2_auth_token", False, sa_key_file, default_sa
            )
            self.assertEqual(
                cmdline,
                [
                    f"--service_account_key_file={sa_key_file}",
                    f"--oauth_scope={oauth_scope}",
                ],
            )
            self.assertEqual(env, {})

    def test_cloud_to_prod_jobspec_parity(self):
        cxx = legacy_run_interop._LANGUAGES["c++"]
        job = legacy_run_interop.cloud_to_prod_jobspec(
            cxx,
            "large_unary",
            "default",
            "grpc-test.sandbox.googleapis.com",
            google_default_creds_use_key_file=False,
            docker_image="grpc_interop_cxx:test",
            auth=False,
            manual_cmd_log=None,
            service_account_key_file="/keys/sa.json",
            default_service_account="test@developer.gserviceaccount.com",
        )
        self.assertEqual(
            job.shortname, "cloud_to_prod:c++:default:large_unary:tls"
        )
        self.assertEqual(job.cmdline[0], "docker")

    def test_cloud_to_cloud_jobspec_variants(self):
        cxx = legacy_run_interop._LANGUAGES["c++"]
        go = legacy_run_interop._LANGUAGES["go"]

        # Standard TLS
        job_tls = legacy_run_interop.cloud_to_cloud_jobspec(
            cxx,
            "large_unary",
            "c++",
            "localhost",
            8080,
            docker_image=None,
            transport_security="tls",
            manual_cmd_log=None,
        )
        self.assertEqual(
            job_tls.shortname, "cloud_to_cloud:c++:c++_server:large_unary:tls"
        )
        self.assertIn("--use_tls=true", " ".join(job_tls.cmdline))

        # ORCA injects --service_config_json
        job_orca = legacy_run_interop.cloud_to_cloud_jobspec(
            cxx,
            "orca_oob",
            "c++",
            "localhost",
            8080,
            docker_image=None,
            transport_security="tls",
            manual_cmd_log=None,
        )
        self.assertIn("--service_config_json=", " ".join(job_orca.cmdline))

        # ALTS transport security
        job_alts = legacy_run_interop.cloud_to_cloud_jobspec(
            cxx,
            "large_unary",
            "c++",
            "localhost",
            8080,
            docker_image=None,
            transport_security="alts",
            manual_cmd_log=None,
        )
        self.assertEqual(
            job_alts.shortname, "cloud_to_cloud:c++:c++_server:large_unary:alts"
        )
        self.assertIn("--use_tls=false", " ".join(job_alts.cmdline))
        self.assertIn("--use_alts=true", " ".join(job_alts.cmdline))

        # HTTP2 server negative test case (goaway) -> uses client_cmd_http2interop
        job_http2_neg = legacy_run_interop.cloud_to_cloud_jobspec(
            go,
            "goaway",
            "http2",
            "localhost",
            8081,
            docker_image=None,
            transport_security="insecure",
            manual_cmd_log=None,
        )
        self.assertIn(
            "negative_http2_client.go", " ".join(job_http2_neg.cmdline)
        )

        # HTTP2 server framing test case (data_frame_padding) -> uses standard client_cmd mapped to large_unary
        job_http2_frame = legacy_run_interop.cloud_to_cloud_jobspec(
            cxx,
            "data_frame_padding",
            "http2",
            "localhost",
            8080,
            docker_image=None,
            transport_security="insecure",
            manual_cmd_log=None,
        )
        self.assertIn("--test_case=large_unary", " ".join(job_http2_frame.cmdline))
        self.assertIn(
            "cmake/build/interop_client", " ".join(job_http2_frame.cmdline)
        )

    def test_server_jobspec_parity(self):
        cxx = legacy_run_interop._LANGUAGES["c++"]
        srv_job = legacy_run_interop.server_jobspec(cxx, "grpc_interop_cxx:tag")
        self.assertEqual(srv_job.shortname, "interop_server_c++")
        self.assertTrue(
            srv_job.container_name.startswith(f"interop_server_{cxx.safename}_")
        )

        # HTTP/2 server exposes 8 ports and healthcheck
        http2_srv = legacy_run_interop.Http2Server()
        h2_job = legacy_run_interop.server_jobspec(
            http2_srv, "grpc_interop_http2:tag"
        )
        self.assertEqual(h2_job.shortname, "interop_server_http2")
        cmd_str = " ".join(h2_job.cmdline)
        self.assertIn("--health-cmd", cmd_str)
        for offset in range(8):
            self.assertIn(f"-p {8080 + offset}", cmd_str)

    def test_aggregate_http2_results(self):
        stdout = (
            'some log\n{"cases": [{"name": "c1", "passed": true}, '
            '{"name": "c2", "passed": false}, {"name": "c3", "skipped": true}]}\n'
        )
        res = legacy_run_interop.aggregate_http2_results(stdout)
        self.assertEqual(
            res,
            {
                "passed": 1,
                "failed": 1,
                "skipped": 1,
                "failed_cases": "c2",
                "percent": 0.5,
            },
        )


class TestMatrixParity(unittest.TestCase):
    """Tests 100% parity with tools/interop_matrix/client_matrix.py and all 29 testcases/* files."""

    def setUp(self):
        self.spec = spec.InteropSpec.load()
        self.matrix_runner = matrix.MatrixRunner(self.spec)

    def test_client_matrix_structures_and_helpers(self):
        self.assertEqual(
            set(legacy_client_matrix.LANG_RUNTIME_MATRIX.keys()),
            {"cxx", "go", "java", "python", "node", "ruby", "php", "csharp"},
        )
        self.assertEqual(
            legacy_client_matrix.get_github_repo("go"),
            "https://github.com/grpc/grpc-go.git",
        )
        self.assertEqual(
            legacy_client_matrix.get_github_repo("java"),
            "https://github.com/grpc/grpc-java.git",
        )
        self.assertEqual(
            legacy_client_matrix.get_github_repo("cxx"),
            "https://github.com/grpc/grpc.git",
        )
        self.assertTrue(
            legacy_client_matrix.should_build_docker_interop_image_from_release_tag(
                "cxx"
            )
        )
        self.assertFalse(
            legacy_client_matrix.should_build_docker_interop_image_from_release_tag(
                "go"
            )
        )
        self.assertEqual(
            legacy_client_matrix.get_runtimes_for_lang_release("java", "v1.0.3"),
            ["java_oracle8"],
        )
        self.assertEqual(
            legacy_client_matrix.get_runtimes_for_lang_release(
                "java", "v1.70.0"
            ),
            ["java"],
        )
        self.assertEqual(
            legacy_client_matrix.get_runtimes_for_lang_release(
                "python", "v1.46.3"
            ),
            ["python", "pythonasyncio"],
        )

    def test_all_31_testcase_files_exact_match(self):
        testcase_dir = os.path.join(REPO_ROOT, "tools/interop_matrix/testcases")
        files = sorted(glob.glob(os.path.join(testcase_dir, "*")))
        self.assertEqual(len(files), 31)

        for filepath in files:
            name = os.path.basename(filepath)
            with open(filepath, "r", encoding="utf-8") as f:
                disk_lines = [
                    line.strip()
                    for line in f.readlines()
                    if line.strip()
                    and not line.strip().startswith("#")
                    and not line.strip().startswith("echo")
                ]
            spec_lines = self.spec.get_matrix_testcase_commands(name)
            self.assertEqual(
                spec_lines,
                disk_lines,
                f"Mismatch in matrix testcase profile {name}",
            )


class TestZeroCodeChangeExtensibility(unittest.TestCase):
    """Proves that adding a new language or test case in YAML requires zero Python changes."""

    def test_add_rust_language_and_custom_testcase_via_yaml_only(self):
        with open(spec.DEFAULT_SPEC_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        # 1. Add a new test case 'zstd_compressed_unary' in YAML data only
        data["test_cases"]["standard"].append(
            {
                "name": "zstd_compressed_unary",
                "cloud_to_prod_eligible": False,
            }
        )

        # 2. Add a new language 'rust' in YAML data only
        data["languages"]["rust"] = {
            "safename": "rust",
            "repo": "hyperium/tonic",
            "included_in_all_clients": True,
            "supports_server": True,
            "docker_supported": True,
            "auth_credentials_mode": "flag",
            "client_cwd": "/var/local/git/tonic-interop",
            "server_cwd": "/var/local/git/tonic-interop",
            "http2_cwd": "/var/local/git/tonic-interop",
            "global_env": {"RUST_BACKTRACE": "1"},
            "cloud_to_prod_env": {},
            "client_cmd": {
                "command": [
                    "cargo",
                    "run",
                    "--release",
                    "--bin",
                    "interop_client",
                    "--",
                ],
                "args_mode": "append",
            },
            "server_cmd": {
                "command": [
                    "cargo",
                    "run",
                    "--release",
                    "--bin",
                    "interop_server",
                    "--",
                ],
                "args_mode": "append",
            },
            "http2_client_cmd": {
                "command": [
                    "cargo",
                    "run",
                    "--release",
                    "--bin",
                    "http2_client",
                    "--",
                ],
                "args_mode": "append",
            },
            "unimplemented_client_cases": ["orca_per_rpc"],
            "unimplemented_server_cases": ["orca_oob"],
            "docker_build": {
                "base_name": "grpc_interop_rust",
                "current_version_file": "tools/dockerfile/interoptest/grpc_interop_rust.current_version",
                "dockerfile_dir": "tools/dockerfile/interoptest/grpc_interop_rust",
                "build_script": "tools/dockerfile/interoptest/grpc_interop_rust/build_interop.sh",
            },
        }

        custom_spec = spec.InteropSpec(data)
        custom_runner = runner.InteropRunner(custom_spec)

        # Verify 'rust' is a first-class language and 'zstd_compressed_unary' is recognized
        self.assertIn("rust", custom_spec.languages)
        self.assertIn("rust", custom_spec.servers)
        self.assertIn("zstd_compressed_unary", custom_spec.standard_test_cases)

        rust_lang = custom_spec.get_language("rust")
        c2c_job = custom_runner.cloud_to_cloud_jobspec(
            rust_lang,
            "zstd_compressed_unary",
            "rust",
            "localhost",
            8080,
            docker_image=None,
            transport_security="tls",
            manual_cmd_log=None,
        )
        self.assertEqual(
            c2c_job.shortname,
            "cloud_to_cloud:rust:rust_server:zstd_compressed_unary:tls",
        )
        self.assertEqual(
            c2c_job.cmdline,
            [
                "bash",
                "-c",
                "cargo run --release --bin interop_client -- "
                "--test_case=zstd_compressed_unary --server_host=localhost "
                "--server_port=8080 --server_host_override=foo.test.google.fr "
                "--use_test_ca=true --use_tls=true",
            ],
        )
        self.assertEqual(c2c_job.environ, {"RUST_BACKTRACE": "1"})


class TestDockerBuilderAndDeduplication(unittest.TestCase):
    """Tests Docker image build deduplication and direct Dockerfile generation."""

    def setUp(self):
        self.spec = spec.InteropSpec.load()
        self.builder = builder.DockerImageBuilder(self.spec)

    def test_deduplicates_java_and_javaokhttp_builds(self):
        java = self.spec.get_language("java")
        javaokhttp = self.spec.get_language("javaokhttp")
        jobs, images = self.builder.plan_image_builds(
            [java, javaokhttp], deduplicate=True
        )
        self.assertEqual(
            len(jobs),
            1,
            "java and javaokhttp share safename 'java' and must only build once",
        )
        self.assertEqual(images["java"], images["javaokhttp"])

    def test_deduplicates_http2_client_and_server_builds(self):
        h2_client = self.spec.get_language("http2")
        jobs, images = self.builder.plan_image_builds(
            [h2_client],
            http2_interop=True,
            http2_server_interop=True,
            deduplicate=True,
        )
        self.assertEqual(
            len(jobs),
            1,
            "http2 client and server share safename 'http2' and must only build once",
        )
        self.assertIn("http2", images)

    def test_direct_dockerfile_generation_for_all_languages(self):
        for name, lang in self.spec.all_targets.items():
            if not lang.docker_build:
                continue
            dockerfile = self.builder.generate_dockerfile(lang)
            self.assertIn("FROM us-docker.pkg.dev/", dockerfile)
            self.assertIn(
                "# Auto-generated by tools.interop.builder.DockerImageBuilder",
                dockerfile,
            )
            dockerfile_script = self.builder.generate_dockerfile(
                lang, use_build_script=True
            )
            self.assertIn(lang.docker_build.build_script, dockerfile_script)


class TestPythonAsyncioFix(unittest.TestCase):
    """Verifies the bug fix in src/python/grpcio_tests/commands.py for --use-asyncio client."""

    def test_commands_py_supports_use_asyncio_in_run_client(self):
        commands_path = os.path.join(
            REPO_ROOT, "src/python/grpcio_tests/commands.py"
        )
        with open(commands_path, "r", encoding="utf-8") as f:
            source = f.read()
        self.assertIn("from tests_aio.interop import client", source)
        self.assertIn("client.test_interoperability()", source)


class TestCLIIntrospectionAndDryRun(unittest.TestCase):
    """Tests unified CLI flags (--list-languages, --list-tests, --explain-matrix, --dry-run, --show-dockerfile)."""

    def test_cli_list_languages(self):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = cli.main(["--list-languages"])
        self.assertEqual(rc, 0)
        output = buf.getvalue()
        self.assertIn("pythonasyncio", output)
        self.assertIn("javaokhttp", output)

    def test_cli_list_tests(self):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = cli.main(["--list-tests"])
        self.assertEqual(rc, 0)
        output = buf.getvalue()
        self.assertIn("=== Standard Test Cases (20) ===", output)
        self.assertIn("large_unary", output)
        self.assertIn("=== HTTP/2 Server Test Cases (8) ===", output)

    def test_cli_explain_matrix(self):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = cli.main(["--explain-matrix", "go"])
        self.assertEqual(rc, 0)
        output = buf.getvalue()
        self.assertIn("Cloud-to-Cloud Matrix Explanation", output)
        self.assertIn("go ->", output)

    def test_cli_show_dockerfile(self):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = cli.main(["--show-dockerfile", "c++"])
        self.assertEqual(rc, 0)
        output = buf.getvalue()
        self.assertIn("FROM us-docker.pkg.dev/", output)
        self.assertIn("make interop_client interop_server", output)

    def test_cli_dry_run_deduplication_and_plan(self):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = cli.main(
                [
                    "-l",
                    "java",
                    "-l",
                    "javaokhttp",
                    "-s",
                    "java",
                    "--use_docker",
                    "--http2_interop",
                    "--http2_server_interop",
                    "--dry-run",
                ]
            )
        self.assertEqual(rc, 0)
        output = buf.getvalue()
        # java, javaokhttp, http2 client, http2 server -> 2 unique images (java, http2)
        self.assertIn(
            "Selected Client Languages : ['java', 'javaokhttp']", output
        )
        self.assertIn("Docker Build Jobs         : 2", output)
        self.assertIn("Server Containers         : 2", output)


if __name__ == "__main__":
    unittest.main()
