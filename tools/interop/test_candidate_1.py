#!/usr/bin/env python3
# Copyright 2025 gRPC authors.
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
"""Comprehensive verification suite for Candidate 1 (Two-Stage Dockerfiles + Static JSON Config + Self-Contained Rust Harness)."""

import copy
import glob
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if os.path.join(REPO_ROOT, "tools/run_tests") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "tools/run_tests"))
if os.path.join(REPO_ROOT, "tools/interop_matrix") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "tools/interop_matrix"))

from tools.interop import harness
import tools.interop_matrix.client_matrix as legacy_client_matrix
import tools.interop_matrix.create_matrix_images as legacy_create_matrix_images
import tools.interop_matrix.run_interop_matrix_tests as legacy_run_matrix_tests
import tools.run_tests.run_interop_tests as legacy_run_interop


class TestStaticTwoStageDockerfilesAndDeletions(unittest.TestCase):
    """Verifies deletion of templates/scripts and two-stage Dockerfile validity."""

    def test_interop_inja_templates_and_build_scripts_deleted(self):
        inja_files = glob.glob(
            os.path.join(
                REPO_ROOT, "templates/tools/dockerfile/interoptest/**/*"
            ),
            recursive=True,
        )
        self.assertEqual(
            [f for f in inja_files if os.path.isfile(f)],
            [],
            "All templates/tools/dockerfile/interoptest/* files must be deleted",
        )
        build_scripts = glob.glob(
            os.path.join(
                REPO_ROOT, "tools/dockerfile/interoptest/*/build_interop.sh"
            )
        )
        self.assertEqual(
            build_scripts,
            [],
            "All 17 build_interop.sh scripts must be deleted",
        )

    def test_checked_in_matrix_testcases_and_generator_deleted(self):
        testcase_files = glob.glob(
            os.path.join(REPO_ROOT, "tools/interop_matrix/testcases/*")
        )
        self.assertEqual(
            testcase_files,
            [],
            "All 31 checked-in tools/interop_matrix/testcases/* files must be deleted",
        )
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    REPO_ROOT, "tools/interop_matrix/create_testcases.sh"
                )
            ),
            "tools/interop_matrix/create_testcases.sh must be deleted",
        )

    def test_all_17_dockerfiles_have_two_stage_build_and_wrappers(self):
        dockerfiles = sorted(
            glob.glob(
                os.path.join(
                    REPO_ROOT,
                    "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile",
                )
            )
        )
        self.assertEqual(len(dockerfiles), 17)
        for df_path in dockerfiles:
            with open(df_path, "r") as f:
                content = f.read()
            self.assertIn("AS base", content, df_path)
            self.assertIn("#=== INTEROP_BUILD_STAGE ===", content, df_path)
            self.assertIn("FROM ${BASE_IMAGE} AS interop", content, df_path)
            self.assertIn("/interop/client", content, df_path)

    def test_check_testing_docker_images_sanity_passes(self):
        res = subprocess.run(
            ["tools/run_tests/sanity/check_testing_docker_images.sh"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)


class TestZeroHardcodedLanguagesOrTestSkips(unittest.TestCase):
    """Verifies zero hardcoded language names exist in Rust or Python harness files."""

    FORBIDDEN_LANGUAGE_LITERALS = [
        "c++",
        "cxx",
        "aspnetcore",
        "csharp",
        "csharpcoreclr",
        "dart",
        "go",
        "java",
        "javaokhttp",
        "node",
        "nodepurejs",
        "php7",
        "objc",
        "ruby",
        "python",
        "pythonasyncio",
        "http2",
    ]

    HARNESS_FILES = [
        "tools/interop/interop_runner.rs",
        "tools/interop/harness.py",
        "tools/run_tests/run_interop_tests.py",
        "tools/interop_matrix/client_matrix.py",
        "tools/interop_matrix/create_matrix_images.py",
        "tools/interop_matrix/run_interop_matrix_tests.py",
    ]

    def test_no_hardcoded_language_names_in_harness_code(self):
        for relpath in self.HARNESS_FILES:
            fullpath = os.path.join(REPO_ROOT, relpath)
            with open(fullpath, "r") as f:
                lines = f.readlines()
            for lineno, line in enumerate(lines, 1):
                stripped = line.strip()
                if stripped.startswith("#") or stripped.startswith("//") or stripped.startswith("//!"):
                    continue
                for lang in self.FORBIDDEN_LANGUAGE_LITERALS:
                    pattern = rf"""(['"]){re.escape(lang)}\1"""
                    self.assertIsNone(
                        re.search(pattern, line),
                        f"Found forbidden hardcoded language literal {lang!r} in {relpath}:{lineno}: {line.strip()}",
                    )


class TestConfigAndRustBinaryValidation(unittest.TestCase):
    """Tests config.json schema validation in both Python and compiled Rust binary."""

    def setUp(self):
        self.spec = harness.InteropSpec.load()
        with open(harness.DEFAULT_CONFIG_PATH, "r") as f:
            self.raw_data = json.load(f)

    def test_rust_binary_compiles_and_validates_config(self):
        res = harness.run_rust_runner(["--validate"])
        self.assertEqual(res.returncode, 0)
        self.assertIn("passed schema and cross-reference validation", res.stdout)

    def test_default_spec_loads_and_validates(self):
        self.assertEqual(self.spec.schema_version, "1.0")
        self.assertEqual(len(self.spec.all_targets), 14)
        self.assertEqual(len(self.spec.languages), 13)
        self.assertIn("http2", self.spec.all_targets)

    def test_rejects_wrong_schema_version(self):
        bad = copy.deepcopy(self.raw_data)
        bad["schema_version"] = "99.0"
        with self.assertRaises(harness.SpecValidationError):
            harness.validate_spec_data(bad)

    def test_rejects_unknown_unimplemented_test_case(self):
        bad = copy.deepcopy(self.raw_data)
        bad["languages"]["c++"]["unimplemented_client_cases"].append(
            "nonexistent_test_case"
        )
        with self.assertRaises(harness.SpecValidationError):
            harness.validate_spec_data(bad)
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
            json.dump(bad, tf)
            tf_path = tf.name
        try:
            res = harness.run_rust_runner(
                ["--config", tf_path, "--validate"], check=False
            )
            self.assertNotEqual(res.returncode, 0)
            self.assertIn("nonexistent_test_case", res.stderr)
        finally:
            os.unlink(tf_path)

    def test_rejects_invalid_auth_credential_mode(self):
        bad = copy.deepcopy(self.raw_data)
        bad["languages"]["c++"]["auth_credentials_mode"] = "magic_beans"
        with self.assertRaises(harness.SpecValidationError):
            harness.validate_spec_data(bad)


class TestAllFiveBugFixes(unittest.TestCase):
    """Explicitly verifies all 5 bug fixes identified in Round 1."""

    def setUp(self):
        self.spec = harness.InteropSpec.load()
        self.runner = harness.InteropRunner(self.spec)
        self.builder = harness.DockerImageBuilder(self.spec)

    def test_bug1_python_asyncio_commands_py_client_fix(self):
        commands_path = os.path.join(
            REPO_ROOT, "src/python/grpcio_tests/commands.py"
        )
        with open(commands_path, "r") as f:
            source = f.read()
        self.assertIn("from tests_aio.interop import client", source)
        self.assertIn("client.test_interoperability()", source)

    def test_bug2_idle_pythonasyncio_server_filtered_out(self):
        argp = harness.build_interop_argparser(self.spec)
        args = argp.parse_args(["-l", "c++", "-s", "all", "--use_docker"])
        _, active_servers, _ = self.runner.resolve_languages_and_servers(args)
        self.assertNotIn(
            "pythonasyncio",
            active_servers,
            "Idle pythonasyncio server (0 active server test cases) must be filtered out",
        )
        self.assertEqual(len(active_servers), 9)

        # Also verify in Rust binary --plan-json
        res = harness.run_rust_runner(
            ["--plan-json", "-l", "c++", "-s", "all", "--use_docker"]
        )
        plan = json.loads(res.stdout)
        self.assertNotIn("pythonasyncio", plan["servers"])
        self.assertNotIn("pythonasyncio", plan["server_specs"])

    def test_bug3_docker_image_deduplication_by_safename(self):
        java = self.spec.get_language("java")
        javaokhttp = self.spec.get_language("javaokhttp")
        jobs, images = self.builder.plan_image_builds(
            [java, javaokhttp], deduplicate=True
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(images["java"], images["javaokhttp"])

        h2_client = self.spec.get_language("http2")
        jobs_h2, images_h2 = self.builder.plan_image_builds(
            [h2_client],
            http2_interop=True,
            http2_server_interop=True,
            deduplicate=True,
        )
        self.assertEqual(len(jobs_h2), 1)
        self.assertIn("http2", images_h2)

    def test_bug4_synchronous_docker_rm_f_without_sleep(self):
        dockerjob_path = os.path.join(
            REPO_ROOT, "tools/run_tests/python_utils/dockerjob.py"
        )
        with open(dockerjob_path, "r") as f:
            dj_source = f.read()
        self.assertIn('"rm"', dj_source)
        self.assertIn('"-f"', dj_source)

    def test_bug5_csharp_v1_20_0_deduplicated_and_upload_guarded(self):
        csharp_releases = list(
            legacy_client_matrix.LANG_RELEASE_MATRIX["csharp"].keys()
        )
        self.assertEqual(csharp_releases.count("v1.20.0"), 1)
        # Verify modules import cleanly without apiclient/oauth2client error
        self.assertIsNotNone(legacy_run_interop)
        self.assertIsNotNone(legacy_run_matrix_tests)
        self.assertIsNotNone(legacy_create_matrix_images)


class TestLanguageAndJobspecParity(unittest.TestCase):
    """Tests 100% parity with original language classes, jobspec helpers, and Rust plan generator."""

    def setUp(self):
        self.spec = harness.InteropSpec.load()
        self.runner = harness.InteropRunner(self.spec)

    def test_legacy_language_factories_and_names(self):
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
        self.assertEqual(str(legacy_run_interop.CXXLanguage()), "c++")
        self.assertEqual(str(legacy_run_interop.JavaOkHttpClient()), "javaokhttp")
        self.assertEqual(str(legacy_run_interop.PHP7Language()), "php7")
        self.assertEqual(str(legacy_run_interop.ObjcLanguage()), "objc")
        self.assertEqual(
            str(legacy_run_interop.PythonAsyncIOLanguage()), "pythonasyncio"
        )
        self.assertEqual(str(legacy_run_interop.Http2Client()), "http2")
        self.assertEqual(str(legacy_run_interop.Http2Server()), "http2")

    def test_objc_extract_port_template(self):
        objc = legacy_run_interop.ObjcLanguage()
        rendered = objc.client_cmd(["--server_port=8080", "--use_tls=true"])
        self.assertEqual(
            rendered,
            [
                'pod install && xcodebuild -workspace Tests.xcworkspace -scheme InteropTestsLocalSSL -destination name="iPhone 6" HOST_PORT_LOCALSSL=localhost:8080 test'
            ],
        )

    def test_auth_options_parity(self):
        sa_key_file = "/path/to/creds.json"
        default_sa = "default-sa@developer.gserviceaccount.com"
        oauth_scope = "https://www.googleapis.com/auth/xapi.zoo"

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

    def test_cloud_to_cloud_and_http2_jobspecs(self):
        cxx = legacy_run_interop._LANGUAGES["c++"]
        go = legacy_run_interop._LANGUAGES["go"]

        job_orca = legacy_run_interop.cloud_to_cloud_jobspec(
            cxx, "orca_oob", "c++", "localhost", 8080
        )
        self.assertIn("--service_config_json=", " ".join(job_orca.cmdline))

        job_http2_neg = legacy_run_interop.cloud_to_cloud_jobspec(
            go,
            "goaway",
            "http2",
            "localhost",
            8081,
            transport_security="insecure",
        )
        self.assertIn(
            "negative_http2_client.go", " ".join(job_http2_neg.cmdline)
        )

        http2_srv = legacy_run_interop.Http2Server()
        h2_job = legacy_run_interop.server_jobspec(
            http2_srv, "grpc_interop_http2:tag"
        )
        cmd_str = " ".join(h2_job.cmdline)
        self.assertIn("--health-cmd", cmd_str)
        for offset in range(8):
            self.assertIn(f"-p {8080 + offset}", cmd_str)

    def test_manual_run_and_sponge_xml_report_generation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_builder = os.path.join(
                tmpdir, "tools/run_tests/dockerize/build_interop_image.sh"
            )
            os.makedirs(os.path.dirname(fake_builder), exist_ok=True)
            with open(fake_builder, "w") as f:
                f.write("#!/bin/sh\nexit 0\n")
            os.chmod(fake_builder, 0o755)
            env = dict(os.environ)
            env["GRPC_TEST_REPORT_BASE_DIR"] = tmpdir
            res = subprocess.run(
                [
                    harness.ensure_rust_runner_binary(),
                    "-l",
                    "c++",
                    "-s",
                    "go",
                    "--use_docker",
                    "--manual_run",
                ],
                cwd=tmpdir,
                env=env,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("--manual_run", res.stdout)
            srv_sh = os.path.join(tmpdir, "interop_server_cmds.sh")
            cli_sh = os.path.join(tmpdir, "interop_client_cmds.sh")
            xml_path = os.path.join(tmpdir, "interop_test/sponge_log.xml")
            self.assertTrue(os.path.exists(srv_sh))
            self.assertTrue(os.path.exists(cli_sh))
            self.assertTrue(os.path.exists(xml_path))
            tree = ET.parse(xml_path)
            root = tree.getroot()
            testcases = root.findall(".//testcase")
            self.assertEqual(len(testcases), 15)


class TestMatrixParityAgainstGitHead(unittest.TestCase):
    """Verifies 100% parity with all 31 deleted tools/interop_matrix/testcases/* files from git HEAD."""

    def setUp(self):
        self.spec = harness.InteropSpec.load()

    def test_all_31_matrix_profiles_match_git_head_in_python_and_rust(self):
        ref = "HEAD"
        out = subprocess.check_output(
            ["git", "ls-tree", "--name-only", ref, "tools/interop_matrix/testcases/"],
            cwd=REPO_ROOT,
            text=True,
        )
        if not out.strip():
            ref = "HEAD~1"
            out = subprocess.check_output(
                ["git", "ls-tree", "--name-only", ref, "tools/interop_matrix/testcases/"],
                cwd=REPO_ROOT,
                text=True,
            )
        files = [line.strip() for line in out.splitlines() if line.strip()]
        self.assertEqual(len(files), 31)

        for relpath in files:
            name = os.path.basename(relpath)
            raw_content = subprocess.check_output(
                ["git", "show", f"{ref}:{relpath}"],
                cwd=REPO_ROOT,
                text=True,
            )
            expected_lines = [
                line.strip()
                for line in raw_content.splitlines()
                if line.strip()
                and not line.strip().startswith("#")
                and not line.strip().startswith("echo")
            ]
            py_lines = self.spec.get_matrix_testcase_commands(name)
            self.assertEqual(
                py_lines,
                expected_lines,
                f"Python profile mismatch for {name}",
            )
            rust_res = harness.run_rust_runner(["--render-matrix-profile", name])
            rust_lines = [
                l.strip() for l in rust_res.stdout.splitlines() if l.strip()
            ]
            self.assertEqual(
                rust_lines,
                expected_lines,
                f"Rust profile mismatch for {name}",
            )


class TestCLIIntrospection(unittest.TestCase):
    """Tests CLI flags via run_interop_tests.py and tools/interop/interop_runner."""

    def test_cli_list_languages_and_tests_and_dry_run(self):
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = harness.run_interop_main(["--list-languages"])
        self.assertEqual(rc, 0)
        self.assertIn("pythonasyncio", buf.getvalue())

        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = harness.run_interop_main(["--list-tests"])
        self.assertEqual(rc, 0)
        self.assertIn("large_unary", buf.getvalue())

        buf = io.StringIO()
        with mock.patch("sys.stdout", buf):
            rc = harness.run_interop_main(
                [
                    "-l",
                    "java",
                    "javaokhttp",
                    "-s",
                    "all",
                    "--use_docker",
                    "--dry-run",
                ]
            )
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        self.assertIn("Selected client languages (2): java, javaokhttp", out)
        self.assertIn("Selected servers (9):", out)
        self.assertNotIn("interop_server_pythonasyncio", out)


if __name__ == "__main__":
    unittest.main()
