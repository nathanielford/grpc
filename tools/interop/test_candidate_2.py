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
"""Comprehensive verification suite for Candidate 2 (tools/interop/harness.py + interop.toml)."""

import ast
import glob
import os
import re
import subprocess
import sys
import tempfile
import tomllib
import types
import unittest
from unittest import mock

_THIS_DIR = os.path.abspath(os.path.dirname(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
_RUN_TESTS_DIR = os.path.join(_REPO_ROOT, "tools/run_tests")
_MATRIX_DIR = os.path.join(_REPO_ROOT, "tools/interop_matrix")
_PYTHON_TESTS_DIR = os.path.join(_REPO_ROOT, "src/python/grpcio_tests")

for _p in (_REPO_ROOT, _RUN_TESTS_DIR, _MATRIX_DIR, _PYTHON_TESTS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import client_matrix
import create_matrix_images
import run_interop_matrix_tests
import run_interop_tests
from tools.interop import harness


class TomlConfigAndZeroHardcodingTest(unittest.TestCase):
    """Verifies interop.toml structure and zero hardcoded language names in harness.py."""

    def test_toml_parses_with_stdlib_tomllib_and_has_no_duplicate_releases(
        self,
    ):
        toml_path = os.path.join(_THIS_DIR, "interop.toml")
        with open(toml_path, "rb") as f:
            cfg = tomllib.load(f)
        self.assertIn("defaults", cfg)
        self.assertIn("prod_servers", cfg)
        self.assertIn("transport_security", cfg)
        self.assertIn("custom_credentials_types", cfg)
        self.assertIn("suites", cfg)
        self.assertIn("test_cases", cfg)
        self.assertIn("languages", cfg)
        self.assertIn("http2_endpoints", cfg)
        self.assertIn("matrix", cfg)

        for lang, lcfg in cfg["matrix"]["languages"].items():
            tags = [entry["tag"] for entry in lcfg["releases"]]
            self.assertEqual(
                len(tags),
                len(set(tags)),
                f"Duplicate release tags found in matrix.languages.{lang}",
            )

    def test_zero_hardcoded_language_names_in_harness_py(self):
        harness_path = os.path.join(_THIS_DIR, "harness.py")
        with open(harness_path, "r") as f:
            source = f.read()

        forbidden_literals = {
            "c++",
            "cxx",
            "aspnetcore",
            "dart",
            "go",
            "java",
            "javaokhttp",
            "node",
            "nodepurejs",
            "php",
            "php7",
            "objc",
            "ruby",
            "python",
            "pythonasyncio",
            "csharp",
            "csharpcoreclr",
        }
        tree = ast.parse(source, filename=harness_path)
        found_in_ast = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value.lower() in forbidden_literals:
                    found_in_ast.append((node.lineno, node.value))
        self.assertEqual(
            found_in_ast,
            [],
            f"Hardcoded language string literals found in harness.py: {found_in_ast}",
        )


class LanguageConfigParityTest(unittest.TestCase):
    """Verifies language configurations and backward-compatible class exports."""

    def test_expected_languages_and_classes_present(self):
        expected = {
            "aspnetcore",
            "c++",
            "dart",
            "go",
            "java",
            "javaokhttp",
            "node",
            "nodepurejs",
            "objc",
            "php7",
            "python",
            "pythonasyncio",
            "ruby",
        }
        self.assertEqual(set(run_interop_tests._LANGUAGES.keys()), expected)

        for cls_name in (
            "CXXLanguage",
            "AspNetCoreLanguage",
            "CSharpAspNetCoreLanguage",
            "DartLanguage",
            "GoLanguage",
            "JavaLanguage",
            "JavaOkHttpClient",
            "NodeLanguage",
            "NodePureJSLanguage",
            "PHP7Language",
            "PHPLanguage",
            "ObjcLanguage",
            "ObjCLanguage",
            "RubyLanguage",
            "PythonLanguage",
            "PythonAsyncIOLanguage",
            "PythonAsyncioLanguage",
            "Http2Client",
            "Http2Server",
        ):
            cls = getattr(run_interop_tests, cls_name)
            inst = cls()
            self.assertTrue(isinstance(inst, harness.InteropLanguage))

    def test_safenames_and_str(self):
        expected_safenames = {
            "c++": "cxx",
            "aspnetcore": "aspnetcore",
            "dart": "dart",
            "go": "go",
            "java": "java",
            "javaokhttp": "java",
            "node": "node",
            "nodepurejs": "nodepurejs",
            "php7": "php7",
            "objc": "objc",
            "ruby": "ruby",
            "python": "python",
            "pythonasyncio": "pythonasyncio",
        }
        for name, expected_safename in expected_safenames.items():
            lang = run_interop_tests._LANGUAGES[name]
            self.assertEqual(str(lang), name)
            self.assertEqual(lang.safename, expected_safename)

        http2_client = run_interop_tests.Http2Client()
        http2_server = run_interop_tests.Http2Server()
        self.assertEqual(str(http2_client), "http2")
        self.assertEqual(http2_client.safename, "http2")
        self.assertEqual(str(http2_server), "http2")
        self.assertEqual(http2_server.safename, "http2")

    def test_client_server_http2_commands(self):
        sample_args = ["--server_host=localhost", "--server_port=8080"]

        cxx = run_interop_tests._LANGUAGES["c++"]
        self.assertEqual(
            cxx.client_cmd(sample_args),
            ["cmake/build/interop_client"] + sample_args,
        )
        self.assertEqual(
            cxx.server_cmd(sample_args),
            ["cmake/build/interop_server"] + sample_args,
        )
        self.assertEqual(
            cxx.client_cmd_http2interop(sample_args),
            ["cmake/build/http2_client"] + sample_args,
        )

        java = run_interop_tests._LANGUAGES["java"]
        self.assertEqual(
            java.client_cmd(sample_args),
            ["./run-test-client.sh"] + sample_args,
        )
        self.assertEqual(
            java.server_cmd(sample_args),
            ["./run-test-server.sh"] + sample_args,
        )
        self.assertEqual(
            java.client_cmd_http2interop(sample_args),
            [
                "./interop-testing/build/install/grpc-interop-testing/bin/http2-client"
            ]
            + sample_args,
        )

        javaokhttp = run_interop_tests._LANGUAGES["javaokhttp"]
        self.assertEqual(
            javaokhttp.client_cmd(sample_args),
            ["./run-test-client.sh", "--use_okhttp=true"] + sample_args,
        )

        go = run_interop_tests._LANGUAGES["go"]
        self.assertEqual(
            go.client_cmd(sample_args),
            ["go", "run", "client.go"] + sample_args,
        )
        self.assertEqual(
            go.server_cmd(sample_args),
            ["go", "run", "server.go"] + sample_args,
        )
        self.assertEqual(
            go.client_cmd_http2interop(sample_args),
            ["go", "run", "negative_http2_client.go"] + sample_args,
        )
        self.assertEqual(go.global_env(), {"GO111MODULE": "on"})

        py = run_interop_tests._LANGUAGES["python"]
        self.assertEqual(
            py.client_cmd(sample_args),
            [
                "py39/bin/python",
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--client",
                '--args="--server_host=localhost --server_port=8080"',
            ],
        )
        self.assertEqual(
            py.server_cmd(sample_args),
            [
                "py39/bin/python",
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--server",
                '--args="--server_host=localhost --server_port=8080"',
            ],
        )
        self.assertEqual(
            py.client_cmd_http2interop(sample_args),
            [
                "py39/bin/python",
                "src/python/grpcio_tests/tests/http2/negative_http2_client.py",
                "--server_host=localhost",
                "--server_port=8080",
            ],
        )
        self.assertEqual(
            py.global_env(),
            {
                "LD_LIBRARY_PATH": "/var/local/git/grpc/libs/opt",
                "PYTHONPATH": "/var/local/git/grpc/src/python/gens",
            },
        )

        py_aio = run_interop_tests._LANGUAGES["pythonasyncio"]
        self.assertEqual(
            py_aio.client_cmd(sample_args),
            [
                "py39/bin/python",
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--use-asyncio",
                "--client",
                '--args="--server_host=localhost --server_port=8080"',
            ],
        )
        self.assertEqual(
            py_aio.server_cmd(sample_args),
            [
                "py39/bin/python",
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--use-asyncio",
                "--server",
                '--args="--server_host=localhost --server_port=8080"',
            ],
        )

        objc = run_interop_tests._LANGUAGES["objc"]
        self.assertEqual(
            objc.client_cmd(sample_args),
            [
                "pod install && xcodebuild -workspace Tests.xcworkspace"
                ' -scheme InteropTestsLocalSSL -destination name="iPhone 6"'
                " HOST_PORT_LOCALSSL=localhost:8080 test"
            ],
        )
        self.assertIsNone(objc.client_cmd(["--server_host=localhost"]))

        http2_server = run_interop_tests.Http2Server()
        self.assertEqual(
            http2_server.server_cmd(["--port=8080", "--use_tls=true"]),
            ["python test/http2_test/http2_test_server.py"],
        )
        http2_client = run_interop_tests.Http2Client()
        self.assertEqual(
            http2_client.client_cmd(sample_args),
            ["tools/http2_interop/http2_interop.test", "-test.v"] + sample_args,
        )


class DockerBuildDeduplicationTest(unittest.TestCase):
    """Verifies compute_docker_build_jobs deduplicates shared safenames."""

    def test_java_and_javaokhttp_share_single_build_job(self):
        java = run_interop_tests._LANGUAGES["java"]
        javaokhttp = run_interop_tests._LANGUAGES["javaokhttp"]
        build_jobs, docker_images = run_interop_tests.compute_docker_build_jobs(
            languages=[java, javaokhttp],
            servers=["java"],
        )
        self.assertEqual(len(build_jobs), 1)
        self.assertEqual(
            build_jobs[0].environ["BASE_NAME"], "grpc_interop_java"
        )
        self.assertEqual(docker_images["java"], docker_images["javaokhttp"])
        self.assertTrue(
            docker_images["java"].startswith("grpc_interop_java:")
        )

    def test_http2_client_and_server_share_single_build_job(self):
        build_jobs, docker_images = run_interop_tests.compute_docker_build_jobs(
            languages=[],
            servers=[],
            http2_interop=True,
            http2_server_interop=True,
        )
        self.assertEqual(len(build_jobs), 1)
        self.assertEqual(
            build_jobs[0].environ["BASE_NAME"], "grpc_interop_http2"
        )
        self.assertIn("http2", docker_images)

    def test_objc_skipped_in_docker_builds(self):
        objc = run_interop_tests._LANGUAGES["objc"]
        build_jobs, docker_images = run_interop_tests.compute_docker_build_jobs(
            languages=[objc],
            servers=[],
        )
        self.assertEqual(build_jobs, [])
        self.assertEqual(docker_images, {})

    def test_all_languages_and_http2_deduplicated_count(self):
        all_but_objc = [
            lang
            for name, lang in run_interop_tests._LANGUAGES.items()
            if name != "objc"
        ]
        build_jobs, docker_images = run_interop_tests.compute_docker_build_jobs(
            languages=all_but_objc,
            servers=run_interop_tests._SERVERS,
            http2_interop=True,
            http2_server_interop=True,
        )
        self.assertEqual(len(build_jobs), 12)
        base_names = [j.environ["BASE_NAME"] for j in build_jobs]
        self.assertEqual(len(base_names), len(set(base_names)))
        self.assertEqual(docker_images["java"], docker_images["javaokhttp"])


class BugFixesVerificationTest(unittest.TestCase):
    """Verifies all 5 bug fixes."""

    def test_commands_run_interop_client_dispatches_asyncio(self):
        import asyncio

        fake_setuptools = types.ModuleType("setuptools")
        fake_setuptools.Command = object
        fake_setuptools.errors = types.ModuleType("setuptools.errors")
        fake_cmd_pkg = types.ModuleType("setuptools.command")
        for sub in ("build_ext", "build_py", "easy_install", "install", "test"):
            mod = types.ModuleType(f"setuptools.command.{sub}")
            setattr(mod, sub, object)
            setattr(fake_cmd_pkg, sub, mod)
        fake_setuptools.command = fake_cmd_pkg
        fake_support = types.ModuleType("support")
        fake_support.run_with_gevent = mock.MagicMock()

        sync_client_mod = types.ModuleType("tests.interop.client")
        sync_client_mod.parse_interop_client_args = mock.MagicMock(
            return_value="parsed_args"
        )
        sync_client_mod.test_interoperability = mock.MagicMock()
        tests_interop_pkg = types.ModuleType("tests.interop")
        tests_interop_pkg.client = sync_client_mod

        aio_client_mod = types.ModuleType("tests_aio.interop.client")

        async def fake_coro():
            return "aio_ran"

        aio_client_mod.test_interoperability = mock.MagicMock(
            side_effect=fake_coro
        )
        tests_aio_interop_pkg = types.ModuleType("tests_aio.interop")
        tests_aio_interop_pkg.client = aio_client_mod

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            with mock.patch.dict(
                sys.modules,
                {
                    "setuptools": fake_setuptools,
                    "setuptools.errors": fake_setuptools.errors,
                    "setuptools.command": fake_cmd_pkg,
                    "setuptools.command.build_ext": fake_cmd_pkg.build_ext,
                    "setuptools.command.build_py": fake_cmd_pkg.build_py,
                    "setuptools.command.easy_install": fake_cmd_pkg.easy_install,
                    "setuptools.command.install": fake_cmd_pkg.install,
                    "setuptools.command.test": fake_cmd_pkg.test,
                    "support": fake_support,
                    "tests": types.ModuleType("tests"),
                    "tests.interop": tests_interop_pkg,
                    "tests.interop.client": sync_client_mod,
                    "tests_aio": types.ModuleType("tests_aio"),
                    "tests_aio.interop": tests_aio_interop_pkg,
                    "tests_aio.interop.client": aio_client_mod,
                },
            ):
                import commands

                cmd = commands.RunInterop()
                cmd.initialize_options()
                cmd.client = True
                cmd.args = "--server_host=localhost --server_port=8080"

                cmd.use_asyncio = False
                cmd.run_client()
                sync_client_mod.test_interoperability.assert_called_once_with(
                    "parsed_args"
                )
                aio_client_mod.test_interoperability.assert_not_called()

                sync_client_mod.test_interoperability.reset_mock()
                cmd.use_asyncio = True
                cmd.run_client()
                aio_client_mod.test_interoperability.assert_called_once_with()
                sync_client_mod.test_interoperability.assert_not_called()
        finally:
            loop.close()

    def test_pythonasyncio_excluded_from_active_servers(self):
        self.assertNotIn("pythonasyncio", run_interop_tests._SERVERS)
        self.assertFalse(
            run_interop_tests._server_has_implemented_test_cases(
                "pythonasyncio", http2_interop=False
            )
        )
        self.assertFalse(
            run_interop_tests._server_has_implemented_test_cases(
                "pythonasyncio", http2_interop=True
            )
        )
        for server_name in run_interop_tests._SERVERS:
            self.assertTrue(
                run_interop_tests._server_has_implemented_test_cases(
                    server_name, http2_interop=False
                ),
                f"Expected server {server_name} to have implemented test cases",
            )

    def test_docker_kill_removes_container_synchronously_without_sleep(self):
        with mock.patch.object(
            run_interop_tests.dockerjob.subprocess, "call", return_value=0
        ) as mock_call:
            res = run_interop_tests.dockerjob.docker_kill("cid_123")
            self.assertTrue(res)
            cmds = [c.args[0] for c in mock_call.call_args_list]
            self.assertIn(["docker", "kill", "cid_123"], cmds)
            self.assertIn(["docker", "rm", "-f", "cid_123"], cmds)

        fake_job = types.SimpleNamespace(
            _spec=types.SimpleNamespace(container_name="cid_456")
        )
        with mock.patch.object(
            run_interop_tests.dockerjob, "docker_kill"
        ) as mock_dk, mock.patch.object(
            run_interop_tests.time, "sleep"
        ) as mock_sleep:
            run_interop_tests._job_kill_handler(fake_job)
            mock_dk.assert_called_once_with("cid_456")
            mock_sleep.assert_not_called()

    def test_dead_functions_removed(self):
        self.assertFalse(
            hasattr(run_interop_tests, "aggregate_http2_results"),
            "aggregate_http2_results should be removed",
        )


class MatrixTestcasesParityTest(unittest.TestCase):
    """Verifies 100% parity with all 25 deleted tools/interop_matrix/testcases/* files."""

    def test_bash_testcase_files_and_create_testcases_sh_deleted(self):
        self.assertFalse(
            os.path.exists(
                os.path.join(_REPO_ROOT, "tools/interop_matrix/create_testcases.sh")
            )
        )
        self.assertEqual(
            glob.glob(
                os.path.join(_REPO_ROOT, "tools/interop_matrix/testcases/*")
            ),
            [],
        )

    def test_all_matrix_releases_match_master_testcase_files(self):
        for lang, release_dict in client_matrix.LANG_RELEASE_MATRIX.items():
            for release in release_dict.keys():
                for runtime in client_matrix.get_runtimes_for_lang_release(
                    lang, release
                ):
                    profile_name = harness._resolve_matrix_profile_name(
                        lang, runtime, release
                    )
                    git_raw = subprocess.check_output(
                        [
                            "git",
                            "-C",
                            _REPO_ROOT,
                            "show",
                            f"master:tools/interop_matrix/testcases/{profile_name}",
                        ],
                        text=True,
                    )
                    expected_lines = []
                    for line in git_raw.splitlines():
                        line = re.sub(r"#.*$", "", line).strip()
                        if line and not line.startswith("echo"):
                            expected_lines.append(line)

                    actual_lines = run_interop_matrix_tests._read_test_cases_file(
                        lang, runtime, release
                    )
                    if profile_name == "node__v1.0.1":
                        expected_norm = [
                            re.sub(
                                r" --name interop_client_node_[0-9a-f-]+",
                                "",
                                l,
                            )
                            for l in expected_lines
                        ]
                        self.assertEqual(actual_lines, expected_norm)
                    else:
                        self.assertEqual(
                            actual_lines,
                            expected_lines,
                            f"Mismatch for {lang}/{runtime}/{release} ({profile_name})",
                        )


class JobSpecAndEndToEndTest(unittest.TestCase):
    """Verifies JobSpec generation, CLI validation, and end-to-end manual run."""

    def test_cloud_to_cloud_jobspec_tls_and_orca(self):
        cxx = run_interop_tests._LANGUAGES["c++"]
        manual_log = []
        spec = run_interop_tests.cloud_to_cloud_jobspec(
            language=cxx,
            test_case="orca_per_rpc",
            server_name="java",
            server_host="localhost",
            server_port=8080,
            docker_image="grpc_interop_cxx:tag1",
            transport_security="tls",
            manual_cmd_log=manual_log,
        )
        self.assertEqual(
            spec.shortname, "cloud_to_cloud:c++:java_server:orca_per_rpc:tls"
        )
        self.assertIsNotNone(spec.container_name)
        cmd_str = " ".join(spec.cmdline)
        self.assertIn("--use_tls=true", cmd_str)
        self.assertIn("test_backend_metrics_load_balancer", cmd_str)
        self.assertEqual(len(manual_log), 2)

    def test_cloud_to_cloud_jobspec_http2_server_cases(self):
        go = run_interop_tests._LANGUAGES["go"]
        rst_spec = run_interop_tests.cloud_to_cloud_jobspec(
            language=go,
            test_case="rst_after_header",
            server_name="http2",
            server_host="localhost",
            server_port=8085,
            docker_image="grpc_interop_go:tag1",
            transport_security="insecure",
        )
        self.assertIn("negative_http2_client.go", " ".join(rst_spec.cmdline))

        df_spec = run_interop_tests.cloud_to_cloud_jobspec(
            language=go,
            test_case="data_frame_padding",
            server_name="http2",
            server_host="localhost",
            server_port=8080,
            docker_image="grpc_interop_go:tag1",
            transport_security="insecure",
        )
        df_cmd = " ".join(df_spec.cmdline)
        self.assertIn("client.go", df_cmd)
        self.assertIn("--test_case=large_unary", df_cmd)

    def test_cloud_to_prod_and_auth_jobspec(self):
        py = run_interop_tests._LANGUAGES["python"]
        auth_spec = run_interop_tests.cloud_to_prod_jobspec(
            language=py,
            test_case="jwt_token_creds",
            server_host_nickname="default",
            server_host="grpc-test.sandbox.googleapis.com",
            google_default_creds_use_key_file=False,
            docker_image="grpc_interop_python:tag1",
            auth=True,
            service_account_key_file="/root/key.json",
            default_service_account="test@developer.gserviceaccount.com",
            transport_security="tls",
        )
        self.assertEqual(
            auth_spec.shortname,
            "cloud_to_prod_auth:python:default:jwt_token_creds:tls",
        )
        self.assertIn(
            "GOOGLE_APPLICATION_CREDENTIALS=/root/key.json",
            auth_spec.cmdline,
        )

    def test_server_jobspec_regular_and_http2(self):
        java = run_interop_tests._LANGUAGES["java"]
        java_server = run_interop_tests.server_jobspec(
            java, "grpc_interop_java:tag1", transport_security="tls"
        )
        self.assertEqual(java_server.shortname, "interop_server_java")
        self.assertIn("-p", java_server.cmdline)
        self.assertIn("8080", java_server.cmdline)

        http2_srv = run_interop_tests.Http2Server()
        http2_job = run_interop_tests.server_jobspec(
            http2_srv, "grpc_interop_http2:tag1"
        )
        self.assertEqual(http2_job.shortname, "interop_server_http2")
        for offset in range(len(run_interop_tests._HTTP2_SERVER_TEST_CASES)):
            self.assertIn(str(8080 + offset), http2_job.cmdline)
        self.assertTrue(
            any(arg.startswith("--health-cmd=") for arg in http2_job.cmdline)
        )

    def test_invalid_override_server_rejected(self):
        with self.assertRaises(SystemExit):
            run_interop_tests.parse_interop_args(
                ["--override_server", "malformed_no_equals"]
            )
        with self.assertRaises(SystemExit):
            run_interop_tests.parse_interop_args(
                ["--override_server", "c++=localhost_no_port"]
            )

    def test_main_end_to_end_manual_run(self):
        captured_runs = []

        def fake_jobset_run(jobs, **kwargs):
            captured_runs.append((list(jobs), kwargs))
            return 0, {}

        test_argv = [
            "run_interop_tests.py",
            "-l",
            "all",
            "-s",
            "all",
            "--use_docker",
            "--manual_run",
            "--cloud_to_prod",
            "--cloud_to_prod_auth",
            "--http2_interop",
            "--http2_server_interop",
        ]
        with mock.patch.object(sys, "argv", test_argv), mock.patch.object(
            run_interop_tests.jobset, "run", side_effect=fake_jobset_run
        ), mock.patch.object(
            run_interop_tests.report_utils, "render_junit_xml_report"
        ), mock.patch.object(
            run_interop_tests, "write_cmdlog_maybe"
        ) as mock_cmdlog, mock.patch.object(
            run_interop_tests.dockerjob, "finish_jobs"
        ), mock.patch.object(
            run_interop_tests.dockerjob, "remove_image"
        ), mock.patch.object(
            run_interop_tests.time, "sleep"
        ):
            with self.assertRaises(SystemExit) as ctx:
                run_interop_tests.main()
            self.assertEqual(ctx.exception.code, 0)

        self.assertEqual(len(captured_runs), 2)
        build_jobs, _ = captured_runs[0]
        test_jobs, test_kwargs = captured_runs[1]
        self.assertEqual(len(build_jobs), 12)
        self.assertTrue(test_kwargs.get("skip_jobs"))
        self.assertGreater(len(test_jobs), 1000)
        self.assertEqual(mock_cmdlog.call_count, 2)


class MultiStageDockerfilesAndScriptsTest(unittest.TestCase):
    """Verifies multi-stage Dockerfiles, template cleanup, and build_interop_image.sh."""

    def test_all_17_interop_dockerfiles_are_multistage_and_have_no_build_sh(
        self,
    ):
        dockerfiles = sorted(
            glob.glob(
                os.path.join(
                    _REPO_ROOT,
                    "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile",
                )
            )
        )
        self.assertEqual(len(dockerfiles), 17)
        for dockerfile in dockerfiles:
            d = os.path.dirname(dockerfile)
            build_sh = os.path.join(d, "build_interop.sh")
            self.assertFalse(
                os.path.exists(build_sh),
                f"Obsolete {build_sh} should be deleted",
            )
            with open(dockerfile, "r") as f:
                content = f.read()
            self.assertIn("ARG BASE_IMAGE=base", content, dockerfile)
            self.assertIn("AS base", content, dockerfile)
            self.assertIn(
                "#=== INTEROP_BUILD_STAGE ===", content, dockerfile
            )
            self.assertIn("FROM ${BASE_IMAGE} AS interop", content, dockerfile)
            self.assertIn("/interop/client", content, dockerfile)
            self.assertIn("/interop/server", content, dockerfile)
            check_res = subprocess.run(
                [
                    "docker",
                    "buildx",
                    "build",
                    "--check",
                    f"--build-context=grpc={_REPO_ROOT}",
                    f"--build-context=grpc_java={d}",
                    f"--build-context=grpc_go={d}",
                    f"--build-context=grpc_dart={d}",
                    f"--build-context=grpc_node={d}",
                    f"--build-context=grpc_dotnet={d}",
                    f"--build-context=service_account={d}",
                    f"--build-context=build_info={d}",
                    d,
                ],
                cwd=_REPO_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            self.assertEqual(
                check_res.returncode,
                0,
                f"docker buildx build --check failed for {d}:\n{check_res.stdout}\n{check_res.stderr}",
            )

    def test_obsolete_templates_deleted(self):
        self.assertEqual(
            glob.glob(
                os.path.join(
                    _REPO_ROOT, "templates/tools/dockerfile/interoptest/*"
                )
            ),
            [],
        )
        self.assertEqual(
            glob.glob(
                os.path.join(
                    _REPO_ROOT,
                    "templates/tools/dockerfile/*_build_interop.sh.include",
                )
            ),
            [],
        )

    def test_build_interop_image_sh_dry_run(self):
        script = os.path.join(
            _REPO_ROOT, "tools/run_tests/dockerize/build_interop_image.sh"
        )
        env = os.environ.copy()
        env.update(
            {
                "BASE_NAME": "grpc_interop_cxx",
                "INTEROP_IMAGE": "grpc_interop_cxx:test_tag",
                "RELEASE_LABEL": "1.60.0",
                "INTEROP_BUILD_DRY_RUN": "1",
                "DOCKERHUB_ORGANIZATION": "grpctesting",
            }
        )
        res = subprocess.run(
            [script],
            cwd=_REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
        )
        self.assertIn("DRY_RUN: docker buildx build", res.stdout)
        self.assertIn("--target interop", res.stdout)
        self.assertIn("--tag grpc_interop_cxx:test_tag", res.stdout)
        self.assertIn("--build-arg RELEASE_LABEL=1.60.0", res.stdout)
        self.assertIn("--label release=1.60.0", res.stdout)
        self.assertIn("--build-context grpc=", res.stdout)

    def test_create_matrix_images_has_no_secondary_docker_build(self):
        self.assertFalse(
            hasattr(create_matrix_images, "add_files_to_image"),
            "add_files_to_image secondary docker build should be removed",
        )
        with open(create_matrix_images.__file__, "r") as f:
            source = f.read()
        self.assertIn("RELEASE_LABEL", source)
        self.assertIn("BUILD_INFO_FILE", source)
        self.assertIn("INTEROP_DOCKERFILE_ROOT", source)

    def test_check_testing_docker_images_sanity_script(self):
        script = os.path.join(
            _REPO_ROOT, "tools/run_tests/sanity/check_testing_docker_images.sh"
        )
        res = subprocess.run(
            [script],
            cwd=_REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        self.assertEqual(
            res.returncode,
            0,
            f"check_testing_docker_images.sh failed:\n{res.stdout}\n{res.stderr}",
        )


if __name__ == "__main__":
    unittest.main()
