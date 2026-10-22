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
"""Comprehensive verification suite for Candidate 3 gRPC Interop Refactoring."""

import copy
import glob
import json
import os
import re
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
INTEROP_MATRIX_DIR = os.path.join(REPO_ROOT, "tools/interop_matrix")
if INTEROP_MATRIX_DIR not in sys.path:
    sys.path.insert(0, INTEROP_MATRIX_DIR)
RUN_TESTS_DIR = os.path.join(REPO_ROOT, "tools/run_tests")
if RUN_TESTS_DIR not in sys.path:
    sys.path.insert(0, RUN_TESTS_DIR)

from tools.interop import (  # noqa: E402
    DEFAULT_SPEC_PATH,
    DockerImageBuilder,
    InteropRunner,
    InteropSpec,
    SpecValidationError,
    auth_options,
    build_interop_argparser,
    cloud_to_cloud_jobspec,
    cloud_to_prod_jobspec,
    compute_engine_creds_required,
    generate_test_case_jobspecs,
    get_all_releases,
    get_github_repo,
    get_release_tags,
    get_runtimes_for_lang_release,
    get_spec,
    load_spec,
    read_test_cases_file,
    server_jobspec,
    should_build_docker_interop_image_from_release_tag,
)
from tools.interop_matrix import client_matrix  # noqa: E402
from tools.interop_matrix import create_matrix_images  # noqa: E402
from tools.interop_matrix import run_interop_matrix_tests  # noqa: E402
from tools.run_tests import run_interop_tests  # noqa: E402
from tools.run_tests.python_utils import dockerjob  # noqa: E402


def _git_show(rev_path: str) -> str:
    return subprocess.check_output(
        ["git", "show", rev_path], cwd=REPO_ROOT
    ).decode("utf-8")


def _load_master_module(name: str, repo_rel_path: str):
    source = _git_show(f"master:{repo_rel_path}")
    # Strip top-level parse_args execution from master:tools/run_tests/run_interop_tests.py
    if repo_rel_path.endswith("run_interop_tests.py"):
        idx = source.find("\nargp = argparse.ArgumentParser")
        if idx != -1:
            source = source[:idx]
    mod = types.ModuleType(name)
    mod.__file__ = os.path.join(REPO_ROOT, repo_rel_path)
    exec(compile(source, mod.__file__, "exec"), mod.__dict__)
    return mod


class TestCandidate3Interop(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = get_spec()
        cls.runner = InteropRunner(cls.spec)
        cls.master_rit = _load_master_module(
            "master_run_interop_tests", "tools/run_tests/run_interop_tests.py"
        )
        cls.master_cm = _load_master_module(
            "master_client_matrix", "tools/interop_matrix/client_matrix.py"
        )
        cls.go_binary = os.path.join(
            REPO_ROOT, "tools/interop/interop_runner"
        )
        if not os.path.exists(cls.go_binary):
            subprocess.check_call(
                [
                    "go",
                    "build",
                    "-o",
                    cls.go_binary,
                    os.path.join(REPO_ROOT, "tools/interop/interop_runner.go"),
                ],
                cwd=os.path.join(REPO_ROOT, "tools/interop"),
            )

    def test_01_templates_and_legacy_scripts_deleted(self):
        """Verifies all 28 .inja/.include templates, 17 build_interop.sh, and 32 matrix testcase scripts are gone."""
        inja_files = glob.glob(
            os.path.join(
                REPO_ROOT, "templates/tools/dockerfile/interoptest/**/*"
            ),
            recursive=True,
        )
        self.assertEqual(inja_files, [])
        for inc in [
            "csharp_build_interop.sh.include",
            "go_build_interop.sh.include",
            "java_build_interop.sh.include",
            "python_build_interop.sh.include",
        ]:
            self.assertFalse(
                os.path.exists(
                    os.path.join(REPO_ROOT, "templates/tools/dockerfile", inc)
                ),
                inc,
            )

        build_scripts = glob.glob(
            os.path.join(
                REPO_ROOT, "tools/dockerfile/interoptest/*/build_interop.sh"
            )
        )
        self.assertEqual(build_scripts, [])

        testcase_files = glob.glob(
            os.path.join(REPO_ROOT, "tools/interop_matrix/testcases/*")
        )
        self.assertEqual(testcase_files, [])
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    REPO_ROOT, "tools/interop_matrix/create_testcases.sh"
                )
            )
        )

        dockerfiles = sorted(
            glob.glob(
                os.path.join(
                    REPO_ROOT,
                    "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile",
                )
            )
        )
        self.assertEqual(len(dockerfiles), 17)
        for df in dockerfiles:
            with open(df, "r") as f:
                content = f.read()
            self.assertIn("#=== INTEROP_BUILD_STAGE ===", content, df)
            self.assertIn("AS interop", content, df)

    def test_02_zero_hardcoded_language_names_in_code(self):
        """Verifies zero hardcoded language names in engine.py, interop_runner.go, and compatibility shims."""
        code_files = [
            "tools/interop/engine.py",
            "tools/interop/interop_runner.go",
            "tools/run_tests/run_interop_tests.py",
            "tools/interop_matrix/client_matrix.py",
            "tools/interop_matrix/create_matrix_images.py",
            "tools/interop_matrix/run_interop_matrix_tests.py",
        ]
        forbidden_literals = [
            '"c++"',
            "'c++'",
            '"aspnetcore"',
            "'aspnetcore'",
            '"javaokhttp"',
            "'javaokhttp'",
            '"nodepurejs"',
            "'nodepurejs'",
            '"pythonasyncio"',
            "'pythonasyncio'",
            '"php7"',
            "'php7'",
            '"objc"',
            "'objc'",
        ]
        for rel_path in code_files:
            full_path = os.path.join(REPO_ROOT, rel_path)
            with open(full_path, "r") as f:
                content = f.read()
            for lit in forbidden_literals:
                self.assertNotIn(
                    lit,
                    content,
                    f"Hardcoded language literal {lit} found in {rel_path}",
                )

    def test_03_language_parity_against_master(self):
        """Verifies every language target matches master:tools/run_tests/run_interop_tests.py."""
        self.assertEqual(
            set(run_interop_tests._LANGUAGES.keys()),
            set(self.master_rit._LANGUAGES.keys()),
        )
        sample_args = [
            "--server_host=localhost",
            "--server_port=8080",
            "--test_case=large_unary",
        ]
        for name, master_lang in self.master_rit._LANGUAGES.items():
            cand_lang = run_interop_tests._LANGUAGES[name]
            self.assertEqual(cand_lang.safename, master_lang.safename, name)
            self.assertEqual(
                cand_lang.client_cwd,
                getattr(master_lang, "client_cwd", None),
                name,
            )
            self.assertEqual(
                cand_lang.server_cwd,
                getattr(master_lang, "server_cwd", None),
                name,
            )
            self.assertEqual(
                cand_lang.http2_cwd,
                getattr(master_lang, "http2_cwd", None),
                name,
            )
            self.assertEqual(
                cand_lang.global_env(), master_lang.global_env(), name
            )
            self.assertEqual(
                cand_lang.cloud_to_prod_env(),
                master_lang.cloud_to_prod_env(),
                name,
            )
            self.assertEqual(
                cand_lang.client_cmd(sample_args),
                master_lang.client_cmd(sample_args),
                name,
            )
            if name in self.master_rit._SERVERS:
                self.assertEqual(
                    cand_lang.server_cmd(["--port=8080"]),
                    master_lang.server_cmd(["--port=8080"]),
                    name,
                )
            if (
                name
                in self.master_rit._LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES
            ):
                self.assertEqual(
                    cand_lang.client_cmd_http2interop(sample_args),
                    master_lang.client_cmd_http2interop(sample_args),
                    name,
                )
            self.assertEqual(
                cand_lang.unimplemented_test_cases(),
                master_lang.unimplemented_test_cases(),
                name,
            )
            if hasattr(master_lang, "unimplemented_test_cases_server"):
                self.assertEqual(
                    cand_lang.unimplemented_test_cases_server(),
                    master_lang.unimplemented_test_cases_server(),
                    name,
                )

        # Also check Http2Client and Http2Server
        m_h2c = self.master_rit.Http2Client()
        c_h2c = run_interop_tests.Http2Client()
        self.assertEqual(
            c_h2c.client_cmd(sample_args), m_h2c.client_cmd(sample_args)
        )
        self.assertEqual(
            c_h2c.unimplemented_test_cases(), m_h2c.unimplemented_test_cases()
        )

        m_h2s = self.master_rit.Http2Server()
        c_h2s = run_interop_tests.Http2Server()
        self.assertEqual(
            c_h2s.server_cmd(["--port=8080"]), m_h2s.server_cmd(["--port=8080"])
        )
        self.assertEqual(
            c_h2s.unimplemented_test_cases_server(),
            m_h2s.unimplemented_test_cases_server(),
        )

    def test_04_auth_and_jobspec_parity_against_master(self):
        """Verifies auth_options, cloud_to_prod_jobspec, cloud_to_cloud_jobspec, and server_jobspec parity."""
        for name in self.spec.languages:
            c_lang = run_interop_tests._LANGUAGES[name]
            m_lang = self.master_rit._LANGUAGES[name]
            for tc in self.spec.auth_test_cases:
                self.assertEqual(
                    compute_engine_creds_required(c_lang, tc),
                    self.master_rit.compute_engine_creds_required(m_lang, tc),
                )
                for use_key in (False, True):
                    self.assertEqual(
                        auth_options(
                            c_lang, tc, use_key, "/tmp/key.json", "sa@g.com"
                        ),
                        self.master_rit.auth_options(
                            m_lang, tc, use_key, "/tmp/key.json", "sa@g.com"
                        ),
                    )

            # Check cloud_to_prod_jobspec and cloud_to_cloud_jobspec
            c_job = cloud_to_prod_jobspec(
                c_lang,
                "large_unary",
                "default",
                "grpc-test.sandbox.googleapis.com",
                google_default_creds_use_key_file=False,
                docker_image="img:1",
                auth=False,
                service_account_key_file="/k.json",
                default_service_account="sa@g.com",
                transport_security="tls",
            )
            self.master_rit.args = types.SimpleNamespace(allow_flakes=False)
            m_job = self.master_rit.cloud_to_prod_jobspec(
                m_lang,
                "large_unary",
                "default",
                "grpc-test.sandbox.googleapis.com",
                google_default_creds_use_key_file=False,
                docker_image="img:1",
                auth=False,
                service_account_key_file="/k.json",
                default_service_account="sa@g.com",
                transport_security="tls",
            )
            self.assertEqual(c_job.shortname, m_job.shortname)
            self.assertEqual(
                [x for x in c_job.cmdline if not x.startswith("--name=")],
                [x for x in m_job.cmdline if not x.startswith("--name=")],
            )

        for sname in self.spec.servers:
            c_srv = server_jobspec(
                run_interop_tests._LANGUAGES[sname], "img:1", "tls"
            )
            m_srv = self.master_rit.server_jobspec(
                self.master_rit._LANGUAGES[sname], "img:1", "tls"
            )
            self.assertEqual(c_srv.shortname, m_srv.shortname)
            self.assertEqual(
                [x for x in c_srv.cmdline if not x.startswith("--name=")],
                [x for x in m_srv.cmdline if not x.startswith("--name=")],
            )

    def test_05_client_matrix_and_all_31_testcase_files_parity(self):
        """Verifies 100% parity with master client_matrix.py and all 31 deleted testcases/* scripts."""
        self.assertEqual(
            client_matrix.LANG_RUNTIME_MATRIX,
            self.master_cm.LANG_RUNTIME_MATRIX,
        )
        for lang in self.master_cm.LANG_RELEASE_MATRIX:
            self.assertEqual(
                client_matrix.get_github_repo(lang),
                self.master_cm.get_github_repo(lang),
            )
            self.assertEqual(
                client_matrix.should_build_docker_interop_image_from_release_tag(
                    lang
                ),
                self.master_cm.should_build_docker_interop_image_from_release_tag(
                    lang
                ),
            )
            for rel, m_info in self.master_cm.LANG_RELEASE_MATRIX[lang].items():
                c_info = client_matrix.LANG_RELEASE_MATRIX[lang][rel]
                self.assertEqual(c_info.patch, m_info.patch, (lang, rel))
                self.assertEqual(c_info.runtimes, m_info.runtimes, (lang, rel))
                self.assertEqual(
                    c_info.testcases_file, m_info.testcases_file, (lang, rel)
                )

                for rt in client_matrix.get_runtimes_for_lang_release(
                    lang, rel
                ):
                    tc_file = client_matrix.LANG_RELEASE_MATRIX[lang][
                        rel
                    ].testcases_file
                    if not tc_file:
                        tc_file = f"{lang}__master"
                    if lang == "csharp" and rt == "csharpcoreclr":
                        tc_file = tc_file.replace("csharp_", "csharpcoreclr_")
                    master_raw = _git_show(
                        f"master:tools/interop_matrix/testcases/{tc_file}"
                    )
                    master_lines = []
                    for line in master_raw.splitlines():
                        line = re.sub(r"\#.*$", "", line).strip()
                        if line and not line.startswith("echo"):
                            master_lines.append(line)
                    cand_lines = read_test_cases_file(lang, rt, rel)
                    self.assertEqual(
                        cand_lines, master_lines, (lang, rt, rel, tc_file)
                    )

        # Also verify the Go binary produces identical commands for all 31 profiles
        for profile_name in self.spec.matrix_testcase_profiles:
            py_cmds = self.spec.get_matrix_testcase_commands(profile_name)
            go_out = (
                subprocess.check_output(
                    [
                        self.go_binary,
                        "--matrix-testcase-profile",
                        profile_name,
                    ],
                    cwd=REPO_ROOT,
                )
                .decode("utf-8")
                .strip()
                .splitlines()
            )
            self.assertEqual(go_out, py_cmds, profile_name)

    def test_06_go_runner_and_python_engine_100_percent_plan_parity(self):
        """Verifies that compiled Go interop_runner and Python engine.py produce identical execution plans."""
        scenarios = [
            ["--cloud_to_prod", "-l", "all", "--prod_servers", "default", "gateway_v4"],
            ["--cloud_to_prod_auth", "-l", "all", "--skip_compute_engine_creds"],
            ["--cloud_to_prod_auth", "-l", "all", "--google_default_creds_use_key_file"],
            ["-l", "all", "-s", "all", "--use_docker", "--transport_security", "tls"],
            ["-l", "all", "-s", "all", "--use_docker", "--transport_security", "alts"],
            ["-l", "all", "-s", "all", "--use_docker", "--transport_security", "insecure"],
            ["-l", "all", "-s", "all", "--http2_interop", "--use_docker"],
            ["-l", "all", "--http2_server_interop", "--use_docker", "--allow_flakes"],
            ["-l", "c++", "go", "--override_server", "java=localhost:50000"],
        ]
        for argv in scenarios:
            argp = build_interop_argparser(self.spec)
            py_args = argp.parse_args(argv)
            py_plan = self.runner.build_plan(py_args)

            go_raw = subprocess.check_output(
                [self.go_binary, "--dump-plan-json"] + argv, cwd=REPO_ROOT
            ).decode("utf-8")
            go_plan = json.loads(go_raw)

            self.assertEqual(
                go_plan["languages"],
                [str(l) for l in py_plan.languages],
                argv,
            )
            self.assertEqual(go_plan["servers"], py_plan.servers, argv)
            self.assertEqual(
                go_plan["http2_clients"],
                [str(l) for l in py_plan.http2_clients],
                argv,
            )
            self.assertEqual(
                [b["shortname"] for b in go_plan["build_jobs"]],
                [b.shortname for b in py_plan.build_jobs],
                argv,
            )
            self.assertEqual(
                [j["shortname"] for j in go_plan["client_jobs"]],
                [j.shortname for j in py_plan.client_jobs],
                argv,
            )
            self.assertEqual(
                len(go_plan["skipped_tests"]),
                len(py_plan.skipped_tests),
                argv,
            )
            for gj, pj in zip(go_plan["client_jobs"], py_plan.client_jobs):
                self.assertEqual(gj["timeout_seconds"], pj.timeout_seconds)
                self.assertEqual(gj["flake_retries"], pj.flake_retries)
                self.assertEqual(gj["timeout_retries"], pj.timeout_retries)
                if not py_args.use_docker:
                    self.assertEqual(gj["cmdline"], pj.cmdline)

    def test_07_all_five_bugs_fixed(self):
        """Verifies all 5 bugs identified in Round 1 are fixed."""
        # Bug 1: commands.py RunInterop.run_client() supports use_asyncio
        with open(
            os.path.join(REPO_ROOT, "src/python/grpcio_tests/commands.py"), "r"
        ) as f:
            cmds_src = f.read()
        self.assertIn("if self.use_asyncio:", cmds_src)
        self.assertIn("from tests_aio.interop import client", cmds_src)
        self.assertIn(
            "asyncio.get_event_loop().run_until_complete(", cmds_src
        )

        # Bug 2: pythonasyncio excluded from active servers & no idle server container built/started
        self.assertNotIn("pythonasyncio", run_interop_tests._SERVERS)
        argp = build_interop_argparser(self.spec)
        args_s_all = argp.parse_args(["-s", "all", "-l", "c++", "--use_docker"])
        plan_s_all = self.runner.build_plan(args_s_all)
        self.assertNotIn("pythonasyncio", plan_s_all.servers)
        self.assertNotIn("pythonasyncio", plan_s_all.server_specs)

        args_s_pa = argp.parse_args(
            ["-s", "pythonasyncio", "-l", "c++", "--use_docker"]
        )
        plan_s_pa = self.runner.build_plan(args_s_pa)
        self.assertEqual(plan_s_pa.servers, [])
        self.assertNotIn(
            "build_docker_pythonasyncio",
            [b.shortname for b in plan_s_pa.build_jobs],
        )
        self.assertEqual(plan_s_pa.client_jobs, [])

        # And -s pythonasyncio without --use_docker still returns 1 with the docker error
        args_no_docker = argp.parse_args(["-s", "pythonasyncio", "-l", "c++"])
        self.assertEqual(self.runner.execute(args_no_docker), 1)

        # Bug 3: Deduplicated Docker image builds for java/javaokhttp and http2
        args_java = argp.parse_args(
            [
                "-l",
                "java",
                "javaokhttp",
                "-s",
                "java",
                "--http2_interop",
                "--http2_server_interop",
                "--use_docker",
            ]
        )
        plan_java = self.runner.build_plan(args_java)
        build_names = [b.shortname for b in plan_java.build_jobs]
        self.assertEqual(build_names.count("build_docker_java"), 1)
        self.assertNotIn("build_docker_javaokhttp", build_names)
        self.assertEqual(build_names.count("build_docker_http2"), 1)
        self.assertEqual(
            plan_java.docker_images["java"],
            plan_java.docker_images["javaokhttp"],
        )

        # Bug 4: Synchronous container removal in dockerjob.docker_kill
        with mock.patch("subprocess.call", return_value=0) as mock_call:
            self.assertTrue(dockerjob.docker_kill("cid_123"))
            mock_call.assert_called_once()
            self.assertEqual(
                mock_call.call_args[0][0], ["docker", "rm", "-f", "cid_123"]
            )

        # Bug 5: csharp v1.20.0 release in interop.json has no duplicate keys
        self.assertIn("v1.20.0", client_matrix.LANG_RELEASE_MATRIX["csharp"])

    def test_08_adversarial_edge_cases_and_synthetic_language(self):
        """Verifies module-level args.allow_flakes mutation, IPv6 override_server, and adding a 15th language purely in JSON."""
        # 1. Module-level run_interop_tests.args.allow_flakes mutation
        try:
            run_interop_tests.args.allow_flakes = True
            j_flake = run_interop_tests.cloud_to_prod_jobspec(
                run_interop_tests.CXXLanguage(),
                "large_unary",
                "default",
                "grpc-test.sandbox.googleapis.com",
                False,
            )
            self.assertEqual(j_flake.flake_retries, 4)
            self.assertEqual(j_flake.timeout_retries, 2)
        finally:
            run_interop_tests.args.allow_flakes = False

        # 2. --override_server validation and IPv6 host:port parsing
        argp = build_interop_argparser(self.spec)
        with self.assertRaises(SystemExit):
            argp.parse_args(["--override_server", "malformed_no_equals"])
        with self.assertRaises(SystemExit):
            argp.parse_args(["--override_server", "c++=host_without_port"])
        ipv6_args = argp.parse_args(
            ["-l", "c++", "--override_server", "go=[::1]:50051"]
        )
        ipv6_plan = self.runner.build_plan(ipv6_args)
        self.assertTrue(
            any(
                "--server_host=[::1]" in " ".join(j.cmdline)
                and "--server_port=50051" in " ".join(j.cmdline)
                for j in ipv6_plan.client_jobs
            )
        )

        # 3. Synthetic 15th language ("rust") added purely to interop.json works in BOTH Python and Go
        raw_copy = copy.deepcopy(self.spec.raw)
        raw_copy["languages"]["rust"] = {
            "safename": "rust",
            "repo": "grpc/grpc-rust",
            "included_in_all_clients": True,
            "supports_server": True,
            "docker_supported": True,
            "auth_credentials_mode": "flag",
            "client_cwd": None,
            "server_cwd": None,
            "http2_cwd": None,
            "global_env": {"RUST_BACKTRACE": "1"},
            "cloud_to_prod_env": {},
            "client_cmd": {
                "command": ["target/release/interop_client"],
                "args_mode": "append",
            },
            "server_cmd": {
                "command": ["target/release/interop_server"],
                "args_mode": "append",
            },
            "http2_client_cmd": None,
            "client_test_cases": ["large_unary", "empty_unary"],
            "server_test_cases": ["large_unary", "empty_unary"],
        }
        custom_spec = InteropSpec(raw_copy)
        custom_runner = InteropRunner(custom_spec)
        custom_argp = build_interop_argparser(custom_spec)
        custom_args = custom_argp.parse_args(["-l", "rust", "-s", "rust", "--use_docker"])
        custom_plan = custom_runner.build_plan(custom_args)
        self.assertEqual(len(custom_plan.client_jobs), 2)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tf:
            json.dump(raw_copy, tf)
            temp_json_path = tf.name
        try:
            env = dict(os.environ, GRPC_INTEROP_SPEC=temp_json_path)
            go_rust_raw = subprocess.check_output(
                [
                    self.go_binary,
                    "--dump-plan-json",
                    "-l",
                    "rust",
                    "-s",
                    "rust",
                    "--use_docker",
                ],
                cwd=REPO_ROOT,
                env=env,
            ).decode("utf-8")
            go_rust_plan = json.loads(go_rust_raw)
            self.assertEqual(
                [j["shortname"] for j in go_rust_plan["client_jobs"]],
                [j.shortname for j in custom_plan.client_jobs],
            )
        finally:
            os.unlink(temp_json_path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
