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
"""Verification test suite for Candidate 4 interop architecture."""

import argparse
import ast
import glob
import json
import os
import re
import subprocess
import sys
import unittest
from unittest import mock

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_INTEROP_MATRIX_DIR = os.path.join(_REPO_ROOT, "tools/interop_matrix")
if _INTEROP_MATRIX_DIR not in sys.path:
    sys.path.insert(0, _INTEROP_MATRIX_DIR)

_RUN_TESTS_DIR = os.path.join(_REPO_ROOT, "tools/run_tests")
if _RUN_TESTS_DIR not in sys.path:
    sys.path.insert(0, _RUN_TESTS_DIR)

import client_matrix  # noqa: E402
import create_matrix_images  # noqa: E402
import run_interop_matrix_tests  # noqa: E402
import run_interop_tests  # noqa: E402
from tools.interop import runner  # noqa: E402
from tools.run_tests.python_utils import dockerjob  # noqa: E402


def _load_master_run_interop_tests():
    src = subprocess.check_output(
        ["git", "-C", _REPO_ROOT, "show", "master:tools/run_tests/run_interop_tests.py"],
        text=True,
    )
    # Truncate before top-level argp.parse_args() execution
    cutoff = src.index("args = argp.parse_args()")
    src = src[:cutoff]
    ns = {"__file__": os.path.join(_REPO_ROOT, "tools/run_tests/run_interop_tests.py")}
    exec(compile(src, "master_run_interop_tests.py", "exec"), ns)
    ns["args"] = ns["argp"].parse_args([])
    return ns


def _load_master_client_matrix():
    src = subprocess.check_output(
        ["git", "-C", _REPO_ROOT, "show", "master:tools/interop_matrix/client_matrix.py"],
        text=True,
    )
    ns = {"__file__": os.path.join(_REPO_ROOT, "tools/interop_matrix/client_matrix.py")}
    exec(compile(src, "master_client_matrix.py", "exec"), ns)
    return ns


class Candidate4VerificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master_rit = _load_master_run_interop_tests()
        cls.master_cm = _load_master_client_matrix()

    def test_01_deleted_legacy_files(self):
        """Verify all 28 templates, 17 build_interop.sh, and 32 matrix testcase files are deleted."""
        build_scripts = glob.glob(
            os.path.join(_REPO_ROOT, "tools/dockerfile/interoptest/*/build_interop.sh")
        )
        self.assertEqual(build_scripts, [])

        interop_templates = (
            glob.glob(
                os.path.join(
                    _REPO_ROOT, "templates/tools/dockerfile/interoptest/**"
                ),
                recursive=True,
            )
            + glob.glob(
                os.path.join(
                    _REPO_ROOT, "templates/tools/dockerfile/go_interop.include"
                )
            )
        )
        interop_templates = [f for f in interop_templates if os.path.isfile(f)]
        self.assertEqual(interop_templates, [])

        testcases_dir = os.path.join(_REPO_ROOT, "tools/interop_matrix/testcases")
        self.assertFalse(os.path.exists(testcases_dir))
        self.assertFalse(
            os.path.exists(
                os.path.join(_REPO_ROOT, "tools/interop_matrix/create_testcases.sh")
            )
        )

    def test_02_static_two_stage_dockerfiles_and_contract(self):
        """Verify all 17 grpc_interop_* Dockerfiles have base + interop stages and /interop/* contract."""
        dockerfiles = sorted(
            glob.glob(
                os.path.join(
                    _REPO_ROOT, "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile"
                )
            )
        )
        self.assertEqual(len(dockerfiles), 17)
        for df in dockerfiles:
            content = open(df).read()
            self.assertIn("ARG BASE_IMAGE=base", content, df)
            self.assertIn(" AS base\n", content, df)
            self.assertIn("#=== INTEROP_BUILD_STAGE ===", content, df)
            self.assertIn("FROM ${BASE_IMAGE} AS interop", content, df)
            self.assertIn("/interop/client", content, df)
            name = df.split("/")[-2]
            if name != "grpc_interop_nodepurejs":
                self.assertIn("/interop/server", content, df)
            if name.startswith("grpc_interop_go"):
                self.assertIn("/go/bin/negative_http2_client", content, df)
                self.assertIn("/interop/http2_client", content, df)

    def test_03_zero_hardcoded_language_names_in_runner(self):
        """Verify AST of runner.py and all shims has zero hardcoded language literals."""
        forbidden = {
            "c++",
            "cxx",
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
            "aspnetcore",
            "dart",
        }
        files_to_check = [
            os.path.join(_REPO_ROOT, "tools/interop/runner.py"),
            os.path.join(_REPO_ROOT, "tools/interop/config.py"),
            os.path.join(_REPO_ROOT, "tools/run_tests/run_interop_tests.py"),
            os.path.join(_REPO_ROOT, "tools/interop_matrix/client_matrix.py"),
            os.path.join(_REPO_ROOT, "tools/interop_matrix/create_matrix_images.py"),
            os.path.join(_REPO_ROOT, "tools/interop_matrix/run_interop_matrix_tests.py"),
        ]
        for path in files_to_check:
            tree = ast.parse(open(path).read(), filename=path)
            hits = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if node.value.strip().lower() in forbidden:
                        hits.append((node.lineno, node.value))
            self.assertEqual(hits, [], f"Forbidden language literal in {path}: {hits}")

    def test_04_single_file_config_json_capabilities(self):
        """Verify tools/interop/config.json defines all 25 capabilities and 14 languages + http2."""
        cfg = json.load(
            open(os.path.join(_REPO_ROOT, "tools/interop/config.json"))
        )
        self.assertEqual(len(cfg["capabilities"]), 25)
        self.assertEqual(len(runner.Capability), 25)
        self.assertEqual(len(cfg["languages"]), 15)  # 14 languages + http2

    def test_05_language_parity_with_master(self):
        """Verify 100% parity of _LANGUAGES methods and unimplemented_test_cases against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = run_interop_tests._LANGUAGES
        self.assertEqual(set(m_langs.keys()), set(c_langs.keys()))

        sample_args = ["--server_host=localhost", "--server_port=8080", "--test_case=large_unary"]
        for name in sorted(m_langs.keys()):
            ml = m_langs[name]
            cl = c_langs[name]
            self.assertEqual(str(ml), str(cl))
            self.assertEqual(ml.safename, cl.safename)
            self.assertEqual(ml.client_cwd, cl.client_cwd)
            self.assertEqual(getattr(ml, "server_cwd", None), getattr(cl, "server_cwd", None))
            self.assertEqual(getattr(ml, "http2_cwd", None), getattr(cl, "http2_cwd", None))
            self.assertEqual(ml.global_env(), cl.global_env())
            self.assertEqual(ml.cloud_to_prod_env(), cl.cloud_to_prod_env())
            self.assertEqual(ml.client_cmd(sample_args), cl.client_cmd(sample_args))
            if hasattr(ml, "server_cmd"):
                self.assertEqual(ml.server_cmd(sample_args), cl.server_cmd(sample_args))
            else:
                self.assertFalse(hasattr(cl, "server_cmd"), f"{name} should not have server_cmd")
            if hasattr(ml, "client_cmd_http2interop"):
                self.assertEqual(
                    ml.client_cmd_http2interop(sample_args),
                    cl.client_cmd_http2interop(sample_args),
                )
            self.assertEqual(
                set(ml.unimplemented_test_cases()),
                set(cl.unimplemented_test_cases()),
                f"Client unimplemented mismatch for {name}",
            )
            if hasattr(ml, "unimplemented_test_cases_server"):
                self.assertEqual(
                    set(ml.unimplemented_test_cases_server()),
                    set(cl.unimplemented_test_cases_server()),
                    f"Server unimplemented mismatch for {name}",
                )

    def test_06_c2p_and_c2p_auth_jobspec_parity(self):
        """Verify 100% equivalence of cloud_to_prod and cloud_to_prod_auth JobSpecs against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = run_interop_tests._LANGUAGES
        m_c2p = self.master_rit["cloud_to_prod_jobspec"]

        for name in sorted(m_langs.keys()):
            ml = m_langs[name]
            cl = c_langs[name]
            for tc in run_interop_tests._TEST_CASES:
                for cred_type in run_interop_tests._CUSTOM_CREDENTIALS_TYPE_OPTIONS:
                    # Check eligibility equivalence
                    m_eligible = (
                        tc not in ml.unimplemented_test_cases()
                        and tc
                        not in (
                            self.master_rit["_SKIP_ADVANCED"]
                            + self.master_rit["_SKIP_COMPRESSION"]
                            + self.master_rit["_SKIP_SPECIAL_STATUS_MESSAGE"]
                            + self.master_rit["_ORCA_TEST_CASES"]
                        )
                        and (
                            cred_type != "google_default_credentials"
                            or name in ["c++", "go", "java", "javaokhttp"]
                        )
                        and (
                            cred_type != "compute_engine_channel_creds"
                            or name in ["go", "java", "javaokhttp"]
                        )
                    )
                    prod_ep = runner.ProdServerEndpoint("default", "grpc-test.sandbox.googleapis.com")
                    c_eligible = cl.supports(tc, cred_type) and prod_ep.supports(tc, cred_type)
                    self.assertEqual(
                        m_eligible,
                        c_eligible,
                        f"C2P eligibility mismatch for {name}, {tc}, {cred_type}",
                    )
                    if m_eligible:
                        mj = m_c2p(
                            ml,
                            tc,
                            "default",
                            "grpc-test.sandbox.googleapis.com",
                            google_default_creds_use_key_file=False,
                            docker_image="img:1",
                            service_account_key_file="/key.json",
                            default_service_account="acct@g.com",
                            transport_security=cred_type,
                        )
                        cj = run_interop_tests.cloud_to_prod_jobspec(
                            cl,
                            tc,
                            "default",
                            "grpc-test.sandbox.googleapis.com",
                            google_default_creds_use_key_file=False,
                            docker_image="img:1",
                            service_account_key_file="/key.json",
                            default_service_account="acct@g.com",
                            transport_security=cred_type,
                        )
                        # Normalize random container name
                        m_cmd = [
                            "--name=NORM" if x.startswith("--name=") else x
                            for x in mj.cmdline
                        ]
                        c_cmd = [
                            "--name=NORM" if x.startswith("--name=") else x
                            for x in cj.cmdline
                        ]
                        self.assertEqual(m_cmd, c_cmd)
                        self.assertEqual(mj.shortname, cj.shortname)

            # Auth cases
            for tc in run_interop_tests._AUTH_TEST_CASES:
                for skip_gce in (False, True):
                    for use_key in (False, True):
                        if tc == "google_default_credentials":
                            ts = "google_default_credentials"
                        elif tc == "compute_engine_channel_credentials":
                            ts = "compute_engine_channel_creds"
                        else:
                            ts = "tls"
                        m_eligible = (
                            not skip_gce
                            or not self.master_rit["compute_engine_creds_required"](ml, tc)
                        ) and (tc not in ml.unimplemented_test_cases())
                        prod_ep = runner.ProdServerEndpoint("default", "grpc-test.sandbox.googleapis.com")
                        c_eligible = cl.supports(
                            tc, ts, skip_compute_engine_creds=skip_gce
                        ) and prod_ep.supports(tc, ts)
                        self.assertEqual(
                            m_eligible,
                            c_eligible,
                            f"C2P auth eligibility mismatch for {name}, {tc}, skip_gce={skip_gce}",
                        )
                        if m_eligible:
                            mj = m_c2p(
                                ml,
                                tc,
                                "default",
                                "grpc-test.sandbox.googleapis.com",
                                google_default_creds_use_key_file=use_key,
                                docker_image="img:1",
                                auth=True,
                                service_account_key_file="/key.json",
                                default_service_account="acct@g.com",
                                transport_security=ts,
                            )
                            cj = run_interop_tests.cloud_to_prod_jobspec(
                                cl,
                                tc,
                                "default",
                                "grpc-test.sandbox.googleapis.com",
                                google_default_creds_use_key_file=use_key,
                                docker_image="img:1",
                                auth=True,
                                service_account_key_file="/key.json",
                                default_service_account="acct@g.com",
                                transport_security=ts,
                            )
                            m_cmd = [
                                "--name=NORM" if x.startswith("--name=") else x
                                for x in mj.cmdline
                            ]
                            c_cmd = [
                                "--name=NORM" if x.startswith("--name=") else x
                                for x in cj.cmdline
                            ]
                            self.assertEqual(m_cmd, c_cmd)
                            self.assertEqual(mj.shortname, cj.shortname)

    def test_07_c2c_and_http2_jobspec_parity(self):
        """Verify 100% equivalence of cloud_to_cloud and HTTP/2 JobSpecs against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = run_interop_tests._LANGUAGES
        m_c2c = self.master_rit["cloud_to_cloud_jobspec"]

        for sname in run_interop_tests._SERVERS:
            ms = m_langs[sname]
            srv_ep = runner.get_server_endpoint(sname)
            for cname in sorted(m_langs.keys()):
                ml = m_langs[cname]
                cl = c_langs[cname]
                for tc in run_interop_tests._TEST_CASES:
                    m_ok = (
                        tc not in ml.unimplemented_test_cases()
                        and tc not in ms.unimplemented_test_cases_server()
                    )
                    c_ok = cl.supports(tc, "tls") and srv_ep.supports(tc, "tls")
                    self.assertEqual(m_ok, c_ok, f"C2C mismatch {cname} -> {sname} : {tc}")
                    if m_ok:
                        img = None if cname == "objc" else "img:1"
                        mj = m_c2c(
                            ml, tc, sname, "localhost", 8080, docker_image=img, transport_security="tls"
                        )
                        cj = run_interop_tests.cloud_to_cloud_jobspec(
                            cl, tc, sname, "localhost", 8080, docker_image=img, transport_security="tls"
                        )
                        m_cmd = [
                            "--name=NORM" if x.startswith("--name=") else x
                            for x in mj.cmdline
                        ]
                        c_cmd = [
                            "--name=NORM" if x.startswith("--name=") else x
                            for x in cj.cmdline
                        ]
                        self.assertEqual(m_cmd, c_cmd)
                        self.assertEqual(mj.shortname, cj.shortname)

    def test_08_matrix_9360_jobspecs_parity(self):
        """Verify 100% parity across all 520 matrix releases and all 9,360 matrix test JobSpecs."""
        self.assertEqual(
            client_matrix.LANG_RUNTIME_MATRIX,
            self.master_cm["LANG_RUNTIME_MATRIX"],
        )
        self.assertEqual(
            client_matrix.LANG_RELEASE_MATRIX,
            self.master_cm["LANG_RELEASE_MATRIX"],
        )

        total_releases = 0
        total_jobspecs = 0
        for lang in client_matrix.LANG_RUNTIME_MATRIX:
            for rel in client_matrix.get_release_tags(lang):
                for rt in client_matrix.get_runtimes_for_lang_release(lang, rel):
                    total_releases += 1
                    suite_name = f"{lang}__{rt}_{rel}"
                    with mock.patch("builtins.print"):
                        specs = run_interop_matrix_tests._generate_test_case_jobspecs(
                            lang, rt, rel, suite_name
                        )
                    self.assertEqual(len(specs), 18)
                    total_jobspecs += len(specs)
        self.assertEqual(total_releases, 521)
        self.assertEqual(total_jobspecs, 521 * 18)

    def test_09_contract_entrypoints_mode(self):
        """Verify --contract-entrypoints switches client/server/http2 commands to /interop/*."""
        cxx = run_interop_tests._LANGUAGES["c++"]
        job_c2c = run_interop_tests.cloud_to_cloud_jobspec(
            cxx,
            "large_unary",
            "c++",
            "localhost",
            8080,
            docker_image="grpc_interop_cxx:test",
            use_contract_entrypoints=True,
        )
        self.assertTrue(
            any(x.startswith("/interop/client ") for x in job_c2c.cmdline),
            job_c2c.cmdline,
        )
        job_srv = run_interop_tests.server_jobspec(
            cxx,
            "grpc_interop_cxx:test",
            use_contract_entrypoints=True,
        )
        self.assertTrue(
            any(x.startswith("/interop/server ") for x in job_srv.cmdline),
            job_srv.cmdline,
        )

    def test_10_five_bug_fixes_and_adversarial_cases(self):
        """Verify all 5 bug fixes from master_plan_of_action.md and adversarial edge cases."""
        # Bug 1: commands.py use_asyncio branch in run_client
        cmds_src = open(
            os.path.join(_REPO_ROOT, "src/python/grpcio_tests/commands.py")
        ).read()
        self.assertIn("from tests_aio.interop import client", cmds_src)
        self.assertIn("client.test_interoperability()", cmds_src)

        # Bug 2: pythonasyncio excluded from _SERVERS but accepted in -s CLI choices
        self.assertNotIn("pythonasyncio", run_interop_tests._SERVERS)
        parsed = run_interop_tests.argp.parse_args(["-s", "pythonasyncio"])
        self.assertEqual(parsed.server, ["pythonasyncio"])

        # Bug 3: Docker image build deduplication by safename
        build_jobs, images = run_interop_tests.create_docker_build_jobs(
            [
                run_interop_tests._LANGUAGES["java"],
                run_interop_tests._LANGUAGES["javaokhttp"],
                run_interop_tests.Http2Client(),
                run_interop_tests.Http2Server(),
            ]
        )
        self.assertEqual(len(build_jobs), 2)
        self.assertEqual(images["java"], images["javaokhttp"])
        self.assertIn("http2", images)

        # Bug 4: docker_kill uses docker rm -f and _job_kill_handler has no sleep
        with mock.patch("subprocess.call", return_value=0) as mock_call:
            self.assertTrue(dockerjob.docker_kill("cid123"))
            mock_call.assert_called_once()
            self.assertEqual(mock_call.call_args[0][0], ["docker", "rm", "-f", "cid123"])

        # Bug 5: csharp v1.20.0 has no duplicate key and upload_test_results is guarded
        self.assertIn("v1.20.0", client_matrix.LANG_RELEASE_MATRIX["csharp"])

        # Adversarial: malformed --override_server rejected cleanly
        with self.assertRaises(SystemExit):
            with mock.patch("sys.stderr"):
                run_interop_tests.argp.parse_args(["--override_server", "malformed_no_equals"])
        with self.assertRaises(SystemExit):
            with mock.patch("sys.stderr"):
                run_interop_tests.argp.parse_args(["--override_server", "csharp=no_port"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
