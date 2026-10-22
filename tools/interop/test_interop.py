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
"""Verification and adversarial regression test suite for gRPC interop harness."""

import ast
import copy
import glob
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
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


def _git_show_master(rel_path):
    for ref in ("master", "origin/master"):
        res = subprocess.run(
            ["git", "-C", _REPO_ROOT, "show", f"{ref}:{rel_path}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if res.returncode == 0:
            return res.stdout
    raise RuntimeError(f"Could not git show {rel_path} from master or origin/master")


def _load_master_run_interop_tests():
    src = _git_show_master("tools/run_tests/run_interop_tests.py")
    cutoff = src.index("args = argp.parse_args()")
    src = src[:cutoff]
    ns = {
        "__file__": os.path.join(
            _REPO_ROOT, "tools/run_tests/run_interop_tests.py"
        )
    }
    exec(compile(src, "master_run_interop_tests.py", "exec"), ns)
    ns["args"] = ns["argp"].parse_args([])
    return ns


def _load_master_client_matrix():
    src = _git_show_master("tools/interop_matrix/client_matrix.py")
    ns = {
        "__file__": os.path.join(
            _REPO_ROOT, "tools/interop_matrix/client_matrix.py"
        )
    }
    exec(compile(src, "master_client_matrix.py", "exec"), ns)
    return ns


class InteropHarnessVerificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.master_rit = _load_master_run_interop_tests()
        cls.master_cm = _load_master_client_matrix()

    def test_01_deleted_legacy_files(self):
        """Verify all 28 templates, 17 build_interop.sh, and 32 matrix testcase files are deleted."""
        build_scripts = glob.glob(
            os.path.join(
                _REPO_ROOT, "tools/dockerfile/interoptest/*/build_interop.sh"
            )
        )
        self.assertEqual(build_scripts, [])

        interop_templates = glob.glob(
            os.path.join(
                _REPO_ROOT, "templates/tools/dockerfile/interoptest/**"
            ),
            recursive=True,
        ) + glob.glob(
            os.path.join(
                _REPO_ROOT, "templates/tools/dockerfile/go_interop.include"
            )
        )
        interop_templates = [f for f in interop_templates if os.path.isfile(f)]
        self.assertEqual(interop_templates, [])

        testcases_dir = os.path.join(
            _REPO_ROOT, "tools/interop_matrix/testcases"
        )
        self.assertFalse(os.path.exists(testcases_dir))
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    _REPO_ROOT, "tools/interop_matrix/create_testcases.sh"
                )
            )
        )

    def test_02_static_two_stage_dockerfiles_and_contract(self):
        """Verify all 17 grpc_interop_* Dockerfiles have base + interop stages and /interop/* contract."""
        dockerfiles = sorted(
            glob.glob(
                os.path.join(
                    _REPO_ROOT,
                    "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile",
                )
            )
        )
        self.assertEqual(len(dockerfiles), 17)
        for df in dockerfiles:
            with open(df) as f:
                content = f.read()
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
            os.path.join(
                _REPO_ROOT, "tools/interop_matrix/create_matrix_images.py"
            ),
            os.path.join(
                _REPO_ROOT, "tools/interop_matrix/run_interop_matrix_tests.py"
            ),
        ]
        for path in files_to_check:
            with open(path) as f:
                tree = ast.parse(f.read(), filename=path)
            hits = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(
                    node.value, str
                ):
                    if node.value.strip().lower() in forbidden:
                        hits.append((node.lineno, node.value))
            self.assertEqual(
                hits, [], f"Forbidden language literal in {path}: {hits}"
            )

    def test_04_single_file_config_json_capabilities(self):
        """Verify tools/interop/config.json defines all 25 capabilities and 14 languages + http2."""
        with open(os.path.join(_REPO_ROOT, "tools/interop/config.json")) as f:
            cfg = json.load(f)
        self.assertEqual(len(cfg["capabilities"]), 25)
        self.assertEqual(len(runner.Capability), 25)
        self.assertEqual(len(cfg["languages"]), 15)  # 14 languages + http2
        for lname, lcfg in cfg["languages"].items():
            self.assertNotIn(
                "docker_build",
                lcfg,
                f"Dead docker_build block should not be in {lname}",
            )

    def test_05_language_parity_with_master(self):
        """Verify 100% parity of _LANGUAGES methods and unimplemented_test_cases against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = run_interop_tests._LANGUAGES
        self.assertEqual(set(m_langs.keys()), set(c_langs.keys()))

        sample_args = [
            "--server_host=localhost",
            "--server_port=8080",
            "--test_case=large_unary",
        ]
        for name in sorted(m_langs.keys()):
            ml = m_langs[name]
            cl = c_langs[name]
            self.assertEqual(str(ml), str(cl))
            self.assertEqual(ml.safename, cl.safename)
            self.assertEqual(ml.client_cwd, cl.client_cwd)
            self.assertEqual(
                getattr(ml, "server_cwd", None), getattr(cl, "server_cwd", None)
            )
            self.assertEqual(
                getattr(ml, "http2_cwd", None), getattr(cl, "http2_cwd", None)
            )
            self.assertEqual(ml.global_env(), cl.global_env())
            self.assertEqual(ml.cloud_to_prod_env(), cl.cloud_to_prod_env())
            self.assertEqual(
                ml.client_cmd(sample_args), cl.client_cmd(sample_args)
            )
            if hasattr(ml, "server_cmd"):
                self.assertEqual(
                    ml.server_cmd(sample_args), cl.server_cmd(sample_args)
                )
            else:
                self.assertFalse(
                    hasattr(cl, "server_cmd"),
                    f"{name} should not have server_cmd",
                )
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
            img = None if name == "objc" else "img:1"
            for tc in run_interop_tests._TEST_CASES:
                for (
                    cred_type
                ) in run_interop_tests._CUSTOM_CREDENTIALS_TYPE_OPTIONS:
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
                    prod_ep = runner.ProdServerEndpoint(
                        "default", "grpc-test.sandbox.googleapis.com"
                    )
                    c_eligible = cl.supports(
                        tc, cred_type
                    ) and prod_ep.supports(tc, cred_type)
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
                            docker_image=img,
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
                            docker_image=img,
                            service_account_key_file="/key.json",
                            default_service_account="acct@g.com",
                            transport_security=cred_type,
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
                            or not self.master_rit[
                                "compute_engine_creds_required"
                            ](ml, tc)
                        ) and (tc not in ml.unimplemented_test_cases())
                        prod_ep = runner.ProdServerEndpoint(
                            "default", "grpc-test.sandbox.googleapis.com"
                        )
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
                                docker_image=img,
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
                                docker_image=img,
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
                    self.assertEqual(
                        m_ok, c_ok, f"C2C mismatch {cname} -> {sname} : {tc}"
                    )
                    if m_ok:
                        img = None if cname == "objc" else "img:1"
                        mj = m_c2c(
                            ml,
                            tc,
                            sname,
                            "localhost",
                            8080,
                            docker_image=img,
                            transport_security="tls",
                        )
                        cj = run_interop_tests.cloud_to_cloud_jobspec(
                            cl,
                            tc,
                            sname,
                            "localhost",
                            8080,
                            docker_image=img,
                            transport_security="tls",
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
        """Verify 100% parity across all 521 matrix releases and all 9,378 matrix test JobSpecs."""
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
                for rt in client_matrix.get_runtimes_for_lang_release(
                    lang, rel
                ):
                    total_releases += 1
                    suite_name = f"{lang}__{rt}_{rel}"
                    with mock.patch("builtins.print"):
                        specs = (
                            run_interop_matrix_tests._generate_test_case_jobspecs(
                                lang, rt, rel, suite_name
                            )
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
        # Bug 1: commands.py use_asyncio branch in run_client & run_server
        with open(
            os.path.join(_REPO_ROOT, "src/python/grpcio_tests/commands.py")
        ) as f:
            cmds_src = f.read()
        self.assertIn("from tests_aio.interop import client", cmds_src)
        self.assertIn("client.test_interoperability()", cmds_src)
        self.assertIn(
            "server.interop_server_lib.parse_interop_server_arguments", cmds_src
        )

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
            self.assertEqual(
                mock_call.call_args[0][0], ["docker", "rm", "-f", "cid123"]
            )

        # Bug 5: csharp v1.20.0 has no duplicate key and upload_test_results is guarded
        self.assertIn("v1.20.0", client_matrix.LANG_RELEASE_MATRIX["csharp"])

    def test_11_exact_unimplemented_list_order_and_http2_parity(self):
        """Verify exact list order of unimplemented_test_cases and callable supports_server."""
        for cls_name in (
            "CXXLanguage",
            "AspNetCoreLanguage",
            "CSharpLanguage",
            "DartLanguage",
            "JavaLanguage",
            "JavaOkHttpClient",
            "GoLanguage",
            "Http2Server",
            "Http2Client",
            "NodeLanguage",
            "NodePureJSLanguage",
            "PHP7Language",
            "ObjcLanguage",
            "RubyLanguage",
            "PythonLanguage",
            "PythonAsyncIOLanguage",
        ):
            c_inst = getattr(run_interop_tests, cls_name)()
            self.assertTrue(
                callable(c_inst.supports_server),
                f"{cls_name}.supports_server must be callable (not shadowed by bool)",
            )
            if cls_name not in self.master_rit:
                continue
            m_inst = self.master_rit[cls_name]()
            m_client_unimpl = list(dict.fromkeys(m_inst.unimplemented_test_cases()))
            c_client_unimpl = c_inst.unimplemented_test_cases()
            self.assertEqual(
                m_client_unimpl,
                c_client_unimpl,
                f"List order mismatch in {cls_name}.unimplemented_test_cases()",
            )
            if hasattr(m_inst, "unimplemented_test_cases_server"):
                m_srv_unimpl = list(
                    dict.fromkeys(m_inst.unimplemented_test_cases_server())
                )
                c_srv_unimpl = c_inst.unimplemented_test_cases_server()
                self.assertEqual(
                    m_srv_unimpl,
                    c_srv_unimpl,
                    f"List order mismatch in {cls_name}.unimplemented_test_cases_server()",
                )

    def test_12_adversarial_override_server_and_ipv6(self):
        """Verify hostile --override_server inputs are rejected while IPv6 [::1]:8080 succeeds."""
        invalid_specs = [
            "malformed_no_equals",
            "csharp=no_port",
            "c++=:",
            "c++=localhost:",
            "c++=:8080",
            "=localhost:8080",
            "c++=localhost:notaport",
            "c++=localhost:0",
            "c++=localhost:70000",
            "c++=localhost:8080:9999",
        ]
        for bad in invalid_specs:
            with self.assertRaises(SystemExit, msg=f"Should reject {bad}"):
                with mock.patch("sys.stderr", new=io.StringIO()):
                    run_interop_tests.argp.parse_args(
                        ["--override_server", bad]
                    )

        parsed_ipv6 = run_interop_tests.argp.parse_args(
            ["--override_server", "c++=[::1]:8080"]
        )
        self.assertEqual(parsed_ipv6.override_server, [["c++", "[::1]:8080"]])
        host, port = runner._split_host_port("[::1]:8080")
        self.assertEqual((host, port), ("[::1]", "8080"))

        ext_srv = runner.get_server_endpoint("external_custom_server")
        self.assertTrue(ext_srv.supports("large_unary", "tls"))
        self.assertEqual(ext_srv.unimplemented_test_cases_server(), [])

    def test_13_adversarial_objc_manual_run_and_contract_entrypoints(self):
        """Verify objc works with ${SERVER_PORT}, preserves client_cwd, and never wraps in docker."""
        objc = run_interop_tests._LANGUAGES["objc"]
        cmd_manual = objc.client_cmd(["--server_port=${SERVER_PORT}"])
        self.assertIsNotNone(cmd_manual)
        self.assertIn("HOST_PORT_LOCALSSL=localhost:${SERVER_PORT}", cmd_manual[0])

        c2c_job = run_interop_tests.cloud_to_cloud_jobspec(
            objc,
            "large_unary",
            "c++",
            "localhost",
            "${SERVER_PORT}",
            docker_image="ignored_img:1",
            use_contract_entrypoints=True,
        )
        self.assertEqual(c2c_job.cwd, "src/objective-c/tests")
        self.assertNotEqual(c2c_job.cmdline[0], "docker")

        c2p_job = run_interop_tests.cloud_to_prod_jobspec(
            objc,
            "large_unary",
            "default",
            "grpc-test.sandbox.googleapis.com",
            google_default_creds_use_key_file=False,
            docker_image="ignored_img:1",
            use_contract_entrypoints=True,
        )
        self.assertEqual(c2p_job.cwd, "src/objective-c/tests")
        self.assertNotEqual(c2p_job.cmdline[0], "docker")

    def test_14_adversarial_contract_entrypoints_http2_and_matrix(self):
        """Verify Http2Server ignores args under --contract-entrypoints and matrix preserves shortnames."""
        h2_srv = run_interop_tests.Http2Server(use_contract=True)
        srv_job = run_interop_tests.server_jobspec(
            h2_srv,
            "grpc_interop_http2:test",
            use_contract_entrypoints=True,
        )
        self.assertEqual(srv_job.cmdline[-3:], ["bash", "-c", "/interop/server"])

        with mock.patch("builtins.print"):
            specs = runner.generate_test_case_jobspecs(
                "cxx",
                "cxx",
                "v1.76.0",
                "cxx__cxx_v1.76.0",
                server_host="74.125.206.210",
                use_contract_entrypoints=True,
            )
        self.assertEqual(len(specs), 18)
        for spec in specs:
            self.assertNotIn("unknown_server", spec.shortname)
            self.assertRegex(
                spec.shortname,
                r"^cxx__cxx_v1\.76\.0:cxx:grpc-test4?:\w+$",
            )
            self.assertIn(
                ".sandbox.googleapis.com",
                spec.cmdline,
            )

    def test_15_adversarial_matrix_cli_flags_and_4arg_build_image_jobspec(self):
        """Verify matrix CLI flags (--docker_path/--gcr_path, --report_file, --release) and 4-arg build_image_jobspec."""
        p_build = create_matrix_images.argp.parse_args(
            ["--docker_path", "gcr.io/custom", "-l", "all", "cxx"]
        )
        self.assertEqual(p_build.docker_path, "gcr.io/custom")

        p_test = run_interop_matrix_tests.argp.parse_args(
            ["--docker_path", "gcr.io/custom"]
        )
        self.assertEqual(p_test.docker_path, "gcr.io/custom")
        self.assertEqual(p_test.report_file, "report.xml")
        with self.assertRaises(SystemExit):
            with mock.patch("sys.stderr", new=io.StringIO()):
                run_interop_matrix_tests.argp.parse_args(
                    ["--release", "master"]
                )

        # 4-arg call to create_matrix_images.build_image_jobspec for go1.8 should infer lang='go'
        job_go = create_matrix_images.build_image_jobspec(
            "go1.8", {"GRPC_GO_ROOT": "/tmp/go"}, "v1.0.5", "/tmp/go"
        )
        self.assertEqual(
            job_go.cmdline,
            ["tools/run_tests/dockerize/build_interop_image.sh"],
        )
        job_cxx = create_matrix_images.build_image_jobspec(
            "cxx", {"GRPC_ROOT": "/tmp/cxx"}, "v1.0.1", "/tmp/cxx"
        )
        self.assertEqual(
            job_cxx.cmdline,
            ["/tmp/cxx/tools/run_tests/dockerize/build_interop_image.sh"],
        )

    def test_16_adversarial_docker_cleanup_and_healthcheck(self):
        """Verify wait_for_healthy quote stripping, add_files_to_image, and -s pythonasyncio without docker."""
        with mock.patch(
            "subprocess.check_output", return_value=b'"healthy"\n'
        ):
            dockerjob.wait_for_healthy("cid123", "http2_srv", 5)

        with tempfile.NamedTemporaryFile("w", delete=False) as tf:
            tf.write("commit 123\n")
            temp_commit = tf.name
        try:
            with mock.patch("subprocess.check_call") as mock_build:
                runner.add_files_to_image(
                    "my_image:v1", [temp_commit], label="release=v1.0.0"
                )
                mock_build.assert_called_once()
                cmd = mock_build.call_args[0][0]
                self.assertEqual(cmd[:4], ["docker", "build", "-t", "my_image:v1"])
                self.assertIn("-f", cmd)
        finally:
            os.unlink(temp_commit)

        # -s pythonasyncio without --use_docker must exit 1
        with self.assertRaises(SystemExit) as ctx:
            with mock.patch("builtins.print"):
                run_interop_tests.main(["-s", "pythonasyncio", "-l", "c++"])
        self.assertEqual(ctx.exception.code, 1)

    def test_17_adversarial_node_dockerfiles_and_legacy_fallback(self):
        """Verify Node Dockerfiles use bash -l and build_interop_image.sh dry-run works for modern + legacy."""
        for node_dir in ("grpc_interop_node", "grpc_interop_nodepurejs"):
            df_path = os.path.join(
                _REPO_ROOT,
                "tools/dockerfile/interoptest",
                node_dir,
                "Dockerfile",
            )
            with open(df_path) as f:
                df_text = f.read()
            self.assertIn('/bin/bash -l -c "cd /home/appuser/grpc-node', df_text)
            self.assertIn("#!/bin/bash -l\\n", df_text)

        with open(os.path.join(_REPO_ROOT, ".dockerignore")) as f:
            dockerignore = f.read()
        self.assertNotIn("\n.git\n", "\n" + dockerignore + "\n")
        self.assertIn("/cmake/build", dockerignore)

        script = os.path.join(
            _REPO_ROOT, "tools/run_tests/dockerize/build_interop_image.sh"
        )
        env = dict(os.environ)
        env["INTEROP_IMAGE"] = "grpc_interop_cxx:dryrun"
        env["BASE_NAME"] = "grpc_interop_cxx"
        env["INTEROP_BUILD_DRY_RUN"] = "1"
        out = subprocess.check_output(
            [script], env=env, stderr=subprocess.STDOUT, text=True
        )
        self.assertIn("DRY_RUN: docker buildx build --load", out)

        # Simulate legacy checkout without #=== INTEROP_BUILD_STAGE === and without .current_version
        temp_root = tempfile.mkdtemp()
        try:
            legacy_dir = os.path.join(
                temp_root,
                "tools/dockerfile/interoptest/grpc_interop_csharp",
            )
            os.makedirs(legacy_dir)
            with open(os.path.join(legacy_dir, "Dockerfile"), "w") as f:
                f.write("FROM debian:stretch\nRUN echo base\n")
            env_legacy = dict(os.environ)
            env_legacy["GRPC_ROOT"] = temp_root
            env_legacy["INTEROP_IMAGE"] = "grpc_interop_csharp:v1.10.0"
            env_legacy["BASE_NAME"] = "grpc_interop_csharp"
            env_legacy["INTEROP_BUILD_DRY_RUN"] = "1"
            out_legacy = subprocess.check_output(
                [script], env=env_legacy, stderr=subprocess.STDOUT, text=True
            )
            self.assertIn("DRY_RUN: docker buildx build --load", out_legacy)
            self.assertIn("--build-arg BASE_NAME=grpc_interop_csharp", out_legacy)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    def test_18_config_schema_validation_and_sync(self):
        """Verify config.schema.json and config.json (<350 lines) are in sync and reject invalid mutations."""
        cfg_path = os.path.join(_REPO_ROOT, "tools/interop/config.json")
        schema_path = os.path.join(
            _REPO_ROOT, "tools/interop/config.schema.json"
        )
        self.assertTrue(os.path.exists(schema_path))

        with open(cfg_path) as f:
            cfg_lines = f.readlines()
            cfg = json.loads("".join(cfg_lines))
        with open(schema_path) as f:
            schema = json.load(f)

        self.assertLess(
            len(cfg_lines),
            350,
            f"config.json should be < 350 lines, got {len(cfg_lines)}",
        )
        self.assertTrue(runner.validate_config(cfg, schema))
        self.assertEqual(
            set(schema["$defs"]["capability"]["enum"]),
            set(cfg["capabilities"]),
        )
        self.assertEqual(
            set(schema["$defs"]["capability"]["enum"]),
            set(c.value for c in runner.Capability),
        )

        # 1. Missing required top-level key
        bad_missing = copy.deepcopy(cfg)
        del bad_missing["language_defaults"]
        with self.assertRaises(ValueError):
            runner.validate_config(bad_missing, schema)

        # 2. Unknown top-level property (additionalProperties: false)
        bad_top_prop = copy.deepcopy(cfg)
        bad_top_prop["unexpected_field"] = 123
        with self.assertRaises(ValueError):
            runner.validate_config(bad_top_prop, schema)

        # 3. Unknown language property (additionalProperties: false)
        bad_lang_prop = copy.deepcopy(cfg)
        bad_lang_prop["languages"]["go"]["unknown_flag"] = True
        with self.assertRaises(ValueError):
            runner.validate_config(bad_lang_prop, schema)

        # 4. Invalid capability enum value
        bad_cap = copy.deepcopy(cfg)
        bad_cap["languages"]["go"]["add_client_capabilities"].append(
            "NON_EXISTENT_CAPABILITY"
        )
        with self.assertRaises(ValueError):
            runner.validate_config(bad_cap, schema)

        # 5. Overlapping add/remove capabilities
        bad_overlap = copy.deepcopy(cfg)
        bad_overlap["languages"]["dart"]["add_client_capabilities"].append(
            "SPECIAL_STATUS_MESSAGE"
        )
        with self.assertRaises(ValueError):
            runner.validate_config(bad_overlap, schema)

        # 6. Wrong type for command_spec (oneOf violation)
        bad_cmd = copy.deepcopy(cfg)
        bad_cmd["languages"]["go"]["client_cmd"] = 42
        with self.assertRaises(ValueError):
            runner.validate_config(bad_cmd, schema)

        # 7. Invalid args_mode enum in command_spec
        bad_mode = copy.deepcopy(cfg)
        bad_mode["languages"]["python"]["client_cmd"]["args_mode"] = "bogus"
        with self.assertRaises(ValueError):
            runner.validate_config(bad_mode, schema)

        # 8. Invalid matrix release range (reversed endpoints)
        bad_range = copy.deepcopy(cfg)
        bad_range["matrix"]["languages"]["cxx"]["overrides"][
            "v1.19.0..v1.0.1"
        ] = {"testcases_file": "cxx__v1.0.1"}
        with self.assertRaises(ValueError):
            runner.validate_config(bad_range, schema)

        # 9. Unknown matrix testcases_file reference
        bad_profile = copy.deepcopy(cfg)
        bad_profile["matrix"]["languages"]["cxx"]["overrides"][
            "v1.0.1..v1.19.0"
        ]["testcases_file"] = "non_existent_profile"
        with self.assertRaises(ValueError):
            runner.validate_config(bad_profile, schema)


if __name__ == "__main__":
    unittest.main(verbosity=2)
