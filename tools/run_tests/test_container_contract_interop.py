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
"""Unit and integration tests for Standardized Container Entrypoint Contract."""

import glob
import os
import stat
import subprocess
import sys
import tempfile
import types
from unittest import mock
import unittest

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
_INTEROP_MATRIX_DIR = os.path.join(_REPO_ROOT, "tools/interop_matrix")
_PYTHON_TESTS_DIR = os.path.join(_REPO_ROOT, "src/python/grpcio_tests")

for _path in (_THIS_DIR, _INTEROP_MATRIX_DIR, _PYTHON_TESTS_DIR):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import client_matrix
import create_matrix_images
import run_interop_matrix_tests
import run_interop_tests


class ContainerContractLanguagesTest(unittest.TestCase):
    """Tests standardized /interop/* contract methods on all language classes."""

    def test_contract_entrypoint_constants(self):
        self.assertEqual(
            run_interop_tests.CONTRACT_CLIENT_ENTRYPOINT, "/interop/client"
        )
        self.assertEqual(
            run_interop_tests.CONTRACT_SERVER_ENTRYPOINT, "/interop/server"
        )
        self.assertEqual(
            run_interop_tests.CONTRACT_HTTP2_CLIENT_ENTRYPOINT,
            "/interop/http2_client",
        )

    def test_standard_languages_contract_commands(self):
        standard_langs = [
            run_interop_tests.CXXLanguage(),
            run_interop_tests.AspNetCoreLanguage(),
            run_interop_tests.DartLanguage(),
            run_interop_tests.GoLanguage(),
            run_interop_tests.Http2Client(),
            run_interop_tests.JavaLanguage(),
            run_interop_tests.NodeLanguage(),
            run_interop_tests.NodePureJSLanguage(),
            run_interop_tests.PHP7Language(),
            run_interop_tests.PythonLanguage(),
            run_interop_tests.PythonAsyncIOLanguage(),
            run_interop_tests.RubyLanguage(),
        ]
        for lang in standard_langs:
            with self.subTest(lang=str(lang)):
                self.assertEqual(
                    lang.contract_client_cmd(["--server_port=8080"]),
                    ["/interop/client", "--server_port=8080"],
                )
                self.assertEqual(
                    lang.contract_server_cmd(["--port=8080"]),
                    ["/interop/server", "--port=8080"],
                )
                self.assertEqual(
                    lang.contract_http2_client_cmd(["--test_case=goaway"]),
                    ["/interop/http2_client", "--test_case=goaway"],
                )

    def test_http2_server_contract_command_ignores_tls_args(self):
        http2_server = run_interop_tests.Http2Server()
        self.assertEqual(
            http2_server.contract_server_cmd(
                ["--port=8080", "--use_tls=false"]
            ),
            ["/interop/server"],
        )

    def test_java_okhttp_client_injects_use_okhttp_flag(self):
        java_okhttp = run_interop_tests.JavaOkHttpClient()
        self.assertEqual(
            java_okhttp.contract_client_cmd(["--server_port=8080"]),
            ["/interop/client", "--use_okhttp=true", "--server_port=8080"],
        )

    def test_cloud_to_cloud_jobspec_contract_vs_legacy_dispatch(self):
        lang = run_interop_tests.PythonAsyncIOLanguage()
        contract_job = run_interop_tests.cloud_to_cloud_jobspec(
            language=lang,
            test_case="large_unary",
            server_name="cxx",
            server_host="localhost",
            server_port=8080,
            docker_image="grpctesting/pythonasyncio:test",
            contract_entrypoints=True,
        )
        self.assertIn("/interop/client", contract_job.cmdline)
        self.assertNotIn("-w", contract_job.cmdline)

        legacy_job = run_interop_tests.cloud_to_cloud_jobspec(
            language=lang,
            test_case="large_unary",
            server_name="cxx",
            server_host="localhost",
            server_port=8080,
            docker_image="grpctesting/pythonasyncio:test",
            contract_entrypoints=False,
        )
        joined_legacy = " ".join(legacy_job.cmdline)
        self.assertIn("py39/bin/python", joined_legacy)
        self.assertIn("--use-asyncio", joined_legacy)
        self.assertIn("-w", legacy_job.cmdline)

    def test_cloud_to_cloud_http2_server_test_case_contract_vs_legacy(self):
        lang = run_interop_tests.GoLanguage()
        contract_job = run_interop_tests.cloud_to_cloud_jobspec(
            language=lang,
            test_case="goaway",
            server_name="http2",
            server_host="localhost",
            server_port=8080,
            docker_image="grpctesting/go:test",
            transport_security="insecure",
            contract_entrypoints=True,
        )
        self.assertIn("/interop/http2_client", contract_job.cmdline)
        self.assertNotIn("-w", contract_job.cmdline)

        legacy_job = run_interop_tests.cloud_to_cloud_jobspec(
            language=lang,
            test_case="goaway",
            server_name="http2",
            server_host="localhost",
            server_port=8080,
            docker_image="grpctesting/go:test",
            transport_security="insecure",
            contract_entrypoints=False,
        )
        joined_legacy = " ".join(legacy_job.cmdline)
        self.assertIn("negative_http2_client.go", joined_legacy)
        self.assertIn("-w", legacy_job.cmdline)

    def test_cloud_to_prod_jobspec_contract_vs_legacy_dispatch(self):
        lang = run_interop_tests.JavaOkHttpClient()
        contract_job = run_interop_tests.cloud_to_prod_jobspec(
            language=lang,
            test_case="large_unary",
            server_host_nickname="default",
            server_host="grpc-test.sandbox.googleapis.com",
            google_default_creds_use_key_file=False,
            docker_image="grpctesting/java:test",
            contract_entrypoints=True,
        )
        self.assertIn("/interop/client", contract_job.cmdline)
        self.assertIn("--use_okhttp=true", contract_job.cmdline)
        self.assertNotIn("-w", contract_job.cmdline)

        legacy_job = run_interop_tests.cloud_to_prod_jobspec(
            language=lang,
            test_case="large_unary",
            server_host_nickname="default",
            server_host="grpc-test.sandbox.googleapis.com",
            google_default_creds_use_key_file=False,
            docker_image="grpctesting/java:test",
            contract_entrypoints=False,
        )
        joined_legacy = " ".join(legacy_job.cmdline)
        self.assertIn("./run-test-client.sh --use_okhttp=true", joined_legacy)
        self.assertIn("-w", legacy_job.cmdline)

    def test_server_jobspec_contract_vs_legacy_dispatch(self):
        lang = run_interop_tests.CXXLanguage()
        contract_job = run_interop_tests.server_jobspec(
            language=lang,
            docker_image="grpctesting/cxx:test",
            transport_security="tls",
            contract_entrypoints=True,
        )
        self.assertIn("/interop/server", contract_job.cmdline)
        self.assertNotIn("-w", contract_job.cmdline)

        legacy_job = run_interop_tests.server_jobspec(
            language=lang,
            docker_image="grpctesting/cxx:test",
            transport_security="tls",
            contract_entrypoints=False,
        )
        joined_legacy = " ".join(legacy_job.cmdline)
        self.assertIn("cmake/build/interop_server", joined_legacy)
        self.assertIn("-w", legacy_job.cmdline)


class PlanDockerImageBuildsDeduplicationTest(unittest.TestCase):
    """Tests deduplication of Docker image builds by safename."""

    def test_deduplicates_shared_safenames(self):
        java = run_interop_tests.JavaLanguage()
        java_okhttp = run_interop_tests.JavaOkHttpClient()
        http2_server = run_interop_tests.Http2Server()
        http2_client = run_interop_tests.Http2Client()
        cxx = run_interop_tests.CXXLanguage()
        objc = run_interop_tests.ObjcLanguage()

        build_jobs, images = run_interop_tests.plan_docker_image_builds(
            [java, java_okhttp, http2_server, http2_client, cxx, objc]
        )

        self.assertEqual(
            set(images.keys()), {"c++", "http2", "java", "javaokhttp"}
        )
        # java and javaokhttp must share the exact same built image tag
        self.assertEqual(images["java"], images["javaokhttp"])
        self.assertEqual(len(build_jobs), 3)
        shortnames = [j.shortname for j in build_jobs]
        self.assertEqual(
            shortnames,
            [
                "build_docker_cxx",
                "build_docker_http2",
                "build_docker_java",
            ],
        )


class PythonAsyncioRunInteropFixTest(unittest.TestCase):
    """Tests the bugfix in src/python/grpcio_tests/commands.py RunInterop."""

    def test_run_client_dispatches_to_aio_when_use_asyncio_true(self):
        import asyncio

        sync_client_mock = mock.MagicMock()
        parse_args_mock = mock.MagicMock(return_value="parsed_sync_args")
        aio_client_mock = mock.AsyncMock()

        fake_setuptools = types.ModuleType("setuptools")
        fake_setuptools.Command = object
        fake_setuptools.__path__ = []
        fake_setuptools_errors = types.ModuleType("setuptools.errors")
        fake_setuptools_errors.OptionError = RuntimeError
        fake_setuptools.errors = fake_setuptools_errors
        fake_setuptools_cmd = types.ModuleType("setuptools.command")
        fake_setuptools_cmd.__path__ = []
        for submod in ("build_ext", "build_py", "easy_install", "install", "test"):
            m = types.ModuleType(f"setuptools.command.{submod}")
            setattr(m, submod, object)
            setattr(fake_setuptools_cmd, submod, m)
        fake_setuptools.command = fake_setuptools_cmd

        fake_tests = types.ModuleType("tests")
        fake_tests_interop = types.ModuleType("tests.interop")
        fake_tests_interop_client = types.ModuleType("tests.interop.client")
        fake_tests_interop_client.test_interoperability = sync_client_mock
        fake_tests_interop_client.parse_interop_client_args = parse_args_mock

        fake_tests_aio = types.ModuleType("tests_aio")
        fake_tests_aio_interop = types.ModuleType("tests_aio.interop")
        fake_tests_aio_interop_client = types.ModuleType(
            "tests_aio.interop.client"
        )
        fake_tests_aio_interop_client.test_interoperability = aio_client_mock

        patched_modules = {
            "setuptools": fake_setuptools,
            "setuptools.errors": fake_setuptools_errors,
            "setuptools.command": fake_setuptools_cmd,
            "setuptools.command.build_ext": fake_setuptools_cmd.build_ext,
            "setuptools.command.build_py": fake_setuptools_cmd.build_py,
            "setuptools.command.easy_install": fake_setuptools_cmd.easy_install,
            "setuptools.command.install": fake_setuptools_cmd.install,
            "setuptools.command.test": fake_setuptools_cmd.test,
            "tests": fake_tests,
            "tests.interop": fake_tests_interop,
            "tests.interop.client": fake_tests_interop_client,
            "tests_aio": fake_tests_aio,
            "tests_aio.interop": fake_tests_aio_interop,
            "tests_aio.interop.client": fake_tests_aio_interop_client,
        }

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            with mock.patch.dict(sys.modules, patched_modules):
                sys.modules.pop("commands", None)
                import commands

                dummy_cmd = object.__new__(commands.RunInterop)
                dummy_cmd.args = "--server_port=8080"
                dummy_cmd.use_asyncio = True
                dummy_cmd.run_client()
                aio_client_mock.assert_awaited_once_with()
                sync_client_mock.assert_not_called()

                aio_client_mock.reset_mock()
                dummy_cmd.use_asyncio = False
                dummy_cmd.run_client()
                sync_client_mock.assert_called_once_with("parsed_sync_args")
                aio_client_mock.assert_not_called()
        finally:
            loop.close()
            asyncio.set_event_loop(None)


class BuildInteropImageStageEntrypointsTest(unittest.TestCase):
    """Tests build_interop_image.sh STAGE_ENTRYPOINTS_ONLY and build_interop.sh scripts."""

    def test_stage_entrypoints_for_all_base_images(self):
        script_path = os.path.join(
            _REPO_ROOT, "tools/run_tests/dockerize/build_interop_image.sh"
        )
        expected_entrypoints = {
            "grpc_interop_cxx": ("client", "server", "http2_client"),
            "grpc_interop_java": ("client", "server", "http2_client"),
            "grpc_interop_go": ("client", "server", "http2_client"),
            "grpc_interop_go1.8": ("client", "server", "http2_client"),
            "grpc_interop_go1.11": ("client", "server", "http2_client"),
            "grpc_interop_go1.16": ("client", "server", "http2_client"),
            "grpc_interop_go1.19": ("client", "server", "http2_client"),
            "grpc_interop_go1.x": ("client", "server", "http2_client"),
            "grpc_interop_aspnetcore": ("client", "server"),
            "grpc_interop_dart": ("client", "server"),
            "grpc_interop_node": ("client", "server"),
            "grpc_interop_nodepurejs": ("client",),
            "grpc_interop_php7": ("client", "server"),
            "grpc_interop_python": ("client", "server", "http2_client"),
            "grpc_interop_pythonasyncio": ("client", "server", "http2_client"),
            "grpc_interop_ruby": ("client", "server"),
            "grpc_interop_http2": ("client", "server"),
            "grpc_interop_csharp": ("client",),
            "grpc_interop_csharpcoreclr": ("client",),
        }

        for base_name, expected_bins in expected_entrypoints.items():
            with self.subTest(base_name=base_name):
                with tempfile.TemporaryDirectory() as tmpdir:
                    env = os.environ.copy()
                    env["BASE_NAME"] = base_name
                    env["STAGE_ENTRYPOINTS_ONLY"] = tmpdir
                    res = subprocess.run(
                        ["bash", script_path],
                        env=env,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(
                        res.returncode,
                        0,
                        f"Failed staging {base_name}: {res.stderr}",
                    )
                    for bin_name in expected_bins:
                        bin_path = os.path.join(tmpdir, bin_name)
                        self.assertTrue(
                            os.path.isfile(bin_path),
                            f"Missing {bin_name} for {base_name}",
                        )
                        mode = os.stat(bin_path).st_mode
                        self.assertTrue(
                            bool(mode & stat.S_IXUSR),
                            f"{bin_name} is not executable for {base_name}",
                        )
                        syntax_check = subprocess.run(
                            ["bash", "-n", bin_path],
                            capture_output=True,
                            text=True,
                            check=False,
                        )
                        self.assertEqual(
                            syntax_check.returncode,
                            0,
                            f"Syntax error in {bin_path}: {syntax_check.stderr}",
                        )

    def test_all_dockerfile_build_interop_scripts_valid_syntax_and_contract(self):
        scripts = sorted(
            glob.glob(
                os.path.join(
                    _REPO_ROOT,
                    "tools/dockerfile/interoptest/*/build_interop.sh",
                )
            )
        )
        self.assertGreaterEqual(len(scripts), 14)
        for script in scripts:
            with self.subTest(script=os.path.relpath(script, _REPO_ROOT)):
                syntax_check = subprocess.run(
                    ["bash", "-n", script],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    syntax_check.returncode,
                    0,
                    f"Syntax error in {script}: {syntax_check.stderr}",
                )
                with open(script, "r", encoding="utf-8") as f:
                    content = f.read()
                self.assertTrue(
                    "mkdir -p /interop" in content
                    or 'INTEROP_DIR="/interop"' in content,
                    f"Missing /interop creation in {script}",
                )
                self.assertIn("chmod +x", content)


class InteropMatrixProgrammaticGenerationTest(unittest.TestCase):
    """Tests programmatic matrix command generation and removal of testcases/*."""

    def test_legacy_testcases_directory_and_script_removed(self):
        self.assertFalse(
            os.path.exists(
                os.path.join(_REPO_ROOT, "tools/interop_matrix/testcases")
            )
        )
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    _REPO_ROOT, "tools/interop_matrix/create_testcases.sh"
                )
            )
        )

    def test_parse_release_version(self):
        self.assertEqual(
            client_matrix.parse_release_version("v1.31.1"), (1, 31, 1)
        )
        self.assertEqual(
            client_matrix.parse_release_version("v1.0.x"), (1, 0, 0)
        )
        self.assertEqual(
            client_matrix.parse_release_version("master"), (999, 999, 999)
        )

    def test_historical_release_thresholds(self):
        # C++ before and after v1.31.1 (bins/opt vs cmake/build)
        cxx_old = client_matrix.resolve_matrix_client_spec(
            "cxx", "cxx", "v1.30.0"
        )
        self.assertEqual(cxx_old.client_prefix, "bins/opt/interop_client")
        cxx_new = client_matrix.resolve_matrix_client_spec(
            "cxx", "cxx", "v1.32.0"
        )
        self.assertEqual(cxx_new.client_prefix, "cmake/build/interop_client")

        # Go before and after v1.34.0 (GO111MODULE=on)
        go_old = client_matrix.resolve_matrix_client_spec(
            "go", "go1.8", "v1.33.0"
        )
        self.assertEqual(go_old.env, ())
        go_new = client_matrix.resolve_matrix_client_spec(
            "go", "go1.16", "v1.34.0"
        )
        self.assertEqual(go_new.env, (("GO111MODULE", "on"),))

        # Python 5-tier historical threshold across virtualenvs
        py_v1_0 = client_matrix.resolve_matrix_client_spec(
            "python", "python", "v1.0.x"
        )
        self.assertIn("py27/bin/python", py_v1_0.client_prefix)
        py_v1_11 = client_matrix.resolve_matrix_client_spec(
            "python", "python", "v1.11.0"
        )
        self.assertIn("py27_native/bin/python", py_v1_11.client_prefix)
        py_v1_20 = client_matrix.resolve_matrix_client_spec(
            "python", "python", "v1.20.0"
        )
        self.assertIn("py37_native/bin/python", py_v1_20.client_prefix)
        py_v1_40 = client_matrix.resolve_matrix_client_spec(
            "python", "python", "v1.40.0"
        )
        self.assertIn("py39_native/bin/python", py_v1_40.client_prefix)
        py_v1_44 = client_matrix.resolve_matrix_client_spec(
            "python", "python", "v1.44.0"
        )
        self.assertIn("py39/bin/python", py_v1_44.client_prefix)

        # C# csharp vs csharpcoreclr target frameworks
        cs_mono = client_matrix.resolve_matrix_client_spec(
            "csharp", "csharp", "v1.2.5"
        )
        self.assertEqual(
            cs_mono.client_prefix, "mono Grpc.IntegrationTesting.Client.exe"
        )
        self.assertTrue(cs_mono.workdir.endswith("bin/Debug"))
        cs_clr_10 = client_matrix.resolve_matrix_client_spec(
            "csharp", "csharpcoreclr", "v1.15.0"
        )
        self.assertTrue(cs_clr_10.workdir.endswith("netcoreapp1.0"))
        cs_clr_31 = client_matrix.resolve_matrix_client_spec(
            "csharp", "csharpcoreclr", "v1.45.0"
        )
        self.assertTrue(cs_clr_31.workdir.endswith("netcoreapp3.1"))

    def test_all_matrix_releases_generate_valid_legacy_and_contract_commands(self):
        count = 0
        for lang, release_dict in client_matrix.LANG_RELEASE_MATRIX.items():
            for release in release_dict:
                runtimes = client_matrix.get_runtimes_for_lang_release(
                    lang, release
                )
                for runtime in runtimes:
                    count += 1
                    legacy_cmd = client_matrix.build_matrix_docker_cmdline(
                        lang=lang,
                        runtime=runtime,
                        release=release,
                        server_host="74.125.206.210",
                        server_host_override="grpc-test.sandbox.googleapis.com",
                        test_case="large_unary",
                        docker_image=f"gcr.io/grpc-testing/grpc_interop_{runtime}:{release}",
                        contract_entrypoints=False,
                    )
                    self.assertTrue(legacy_cmd.startswith("docker run "))
                    self.assertIn("--test_case=large_unary", legacy_cmd)

                    contract_cmd = client_matrix.build_matrix_docker_cmdline(
                        lang=lang,
                        runtime=runtime,
                        release=release,
                        server_host="74.125.206.210",
                        server_host_override="grpc-test.sandbox.googleapis.com",
                        test_case="large_unary",
                        docker_image=f"gcr.io/grpc-testing/grpc_interop_{runtime}:{release}",
                        contract_entrypoints=True,
                    )
                    self.assertTrue(contract_cmd.startswith("docker run "))
                    self.assertIn("/interop/client", contract_cmd)
                    self.assertIn("--test_case=large_unary", contract_cmd)
        self.assertEqual(count, 521)

    def test_run_interop_matrix_tests_generate_test_case_jobspecs(self):
        expected_count = len(client_matrix.INTEROP_MATRIX_SERVERS) * len(
            client_matrix.INTEROP_MATRIX_TEST_CASES
        )
        legacy_jobs = run_interop_matrix_tests._generate_test_case_jobspecs(
            lang="go",
            runtime="go1.19",
            release="v1.55.0",
            suite_name="go__go1.19_v1.55.0",
            contract_entrypoints=False,
        )
        self.assertEqual(len(legacy_jobs), expected_count)
        self.assertIn("go run client.go", legacy_jobs[0].cmdline)

        contract_jobs = run_interop_matrix_tests._generate_test_case_jobspecs(
            lang="go",
            runtime="go1.19",
            release="v1.55.0",
            suite_name="go__go1.19_v1.55.0",
            contract_entrypoints=True,
        )
        self.assertEqual(len(contract_jobs), expected_count)
        self.assertIn("/interop/client", contract_jobs[0].cmdline)

    def test_create_matrix_images_passes_label_and_commit_log_to_single_stage_build(
        self,
    ):
        spec = create_matrix_images.build_image_jobspec(
            runtime="cxx",
            env={},
            gcr_tag="v1.50.0",
            stack_base="/tmp/fake_grpc_stack",
            lang="cxx",
        )
        self.assertEqual(spec.environ["INTEROP_IMAGE_LABEL"], "release=v1.50.0")
        self.assertEqual(
            spec.environ["BUILD_INFO_COMMIT_LOG"],
            "/tmp/fake_grpc_stack/commit_log",
        )


if __name__ == "__main__":
    unittest.main()
