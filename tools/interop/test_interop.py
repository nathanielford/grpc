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
import contextlib
import copy
import glob
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tools.interop import runner  # noqa: E402
from tools.run_tests.python_utils import dockerjob  # noqa: E402


def _load_master_run_interop_tests():
    run_tests_dir = os.path.join(_REPO_ROOT, "tools/run_tests")
    if run_tests_dir not in sys.path:
        sys.path.insert(0, run_tests_dir)
    src = subprocess.check_output(
        [
            "git",
            "-C",
            _REPO_ROOT,
            "show",
            "master:tools/run_tests/run_interop_tests.py",
        ],
        text=True,
    )
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
    src = subprocess.check_output(
        [
            "git",
            "-C",
            _REPO_ROOT,
            "show",
            "master:tools/interop_matrix/client_matrix.py",
        ],
        text=True,
    )
    ns = {
        "__file__": os.path.join(
            _REPO_ROOT, "tools/interop_matrix/client_matrix.py"
        )
    }
    exec(compile(src, "master_client_matrix.py", "exec"), ns)
    return ns


# master's run_interop_matrix_tests.py defaulted --server_host to this address.
_LEGACY_MATRIX_SERVER_HOST = "74.125.206.210"

# The legacy node__v1.0.1 snapshot hardcoded a container name on every line.
# The refactored harness lets Docker assign names (containers use --rm=true).
_LEGACY_SNAPSHOT_CONTAINER_NAME_RE = re.compile(
    r"--name interop_client_node_[0-9a-f-]+ "
)


def _load_master_matrix_generator(master_cm, tmp_dir):
    """Returns master's `_generate_test_case_jobspecs` and its testcase files.

    master's run_interop_matrix_tests.py reads tools/interop_matrix/testcases/*
    relative to its own path, so those files are materialized from master into
    tmp_dir and the script is executed up to (excluding) its main loop.
    """
    testcases_dir = os.path.join(tmp_dir, "testcases")
    os.makedirs(testcases_dir)
    paths = subprocess.check_output(
        [
            "git",
            "-C",
            _REPO_ROOT,
            "ls-tree",
            "--name-only",
            "master",
            "tools/interop_matrix/testcases/",
        ],
        text=True,
    ).split()
    for path in paths:
        content = subprocess.check_output(
            ["git", "-C", _REPO_ROOT, "show", "master:%s" % path], text=True
        )
        target = os.path.join(testcases_dir, os.path.basename(path))
        with open(target, "w") as f:
            f.write(content)

    src = subprocess.check_output(
        [
            "git",
            "-C",
            _REPO_ROOT,
            "show",
            "master:tools/interop_matrix/run_interop_matrix_tests.py",
        ],
        text=True,
    )
    src = src[: src.index("\nlanguages = args.language")]
    src = src.replace(
        "args = argp.parse_args()",
        "args = argp.parse_args([%r])"
        % ("--server_host=%s" % _LEGACY_MATRIX_SERVER_HOST),
    )
    client_matrix = types.ModuleType("client_matrix")
    client_matrix.__dict__.update(master_cm)
    ns = {"__file__": os.path.join(tmp_dir, "run_interop_matrix_tests.py")}
    saved_path = list(sys.path)
    try:
        with mock.patch.dict(
            sys.modules,
            {
                "client_matrix": client_matrix,
                "upload_test_results": types.ModuleType("upload_test_results"),
            },
        ):
            exec(compile(src, "master_run_interop_matrix_tests.py", "exec"), ns)
    finally:
        sys.path[:] = saved_path
    return ns["_generate_test_case_jobspecs"]


def _load_master_create_matrix_images(master_cm):
    """Returns master's create_matrix_images.py namespace (up to its main loop)."""
    src = subprocess.check_output(
        [
            "git",
            "-C",
            _REPO_ROOT,
            "show",
            "master:tools/interop_matrix/create_matrix_images.py",
        ],
        text=True,
    )
    src = src[: src.index("\nlanguages = args.language")]
    src = src.replace(
        "args = argp.parse_args()",
        "args = argp.parse_args(['--git_checkout', '--release', 'all'])",
    )
    client_matrix = types.ModuleType("client_matrix")
    client_matrix.__dict__.update(master_cm)
    ns = {
        "__file__": os.path.join(
            _REPO_ROOT, "tools/interop_matrix/create_matrix_images.py"
        )
    }
    saved_path = list(sys.path)
    try:
        with mock.patch.dict(sys.modules, {"client_matrix": client_matrix}):
            with mock.patch("atexit.register"):
                exec(compile(src, "master_create_matrix_images.py", "exec"), ns)
    finally:
        sys.path[:] = saved_path
    return ns


def _dockerignore_rules(text):
    """Parses .dockerignore text into (negated, regex) rules, Docker style."""
    rules = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        negated = line.startswith("!")
        pattern = (line[1:] if negated else line).strip("/")
        regex, i = "", 0
        while i < len(pattern):
            if pattern.startswith("**/", i):
                regex, i = regex + "(?:.*/)?", i + 3
            elif pattern.startswith("**", i):
                regex, i = regex + ".*", i + 2
            elif pattern[i] == "*":
                regex, i = regex + "[^/]*", i + 1
            elif pattern[i] == "?":
                regex, i = regex + "[^/]", i + 1
            elif pattern[i] == "[":
                end = pattern.index("]", i)
                regex, i = regex + pattern[i : end + 1], end + 1
            else:
                regex, i = regex + re.escape(pattern[i]), i + 1
        rules.append((negated, re.compile("^%s$" % regex)))
    return rules


def _dockerignore_excludes(rules, relpath):
    """True if Docker leaves relpath out of the build context (last match wins)."""
    parts = relpath.split("/")
    prefixes = ["/".join(parts[: n + 1]) for n in range(len(parts))]
    excluded = False
    for negated, regex in rules:
        if any(regex.match(prefix) for prefix in prefixes):
            excluded = not negated
    return excluded


def _matrix_jobspec_fields(spec, legacy=False):
    cmdline = spec.cmdline
    if legacy:
        cmdline = _LEGACY_SNAPSHOT_CONTAINER_NAME_RE.sub("", cmdline)
    return (
        spec.shortname,
        cmdline,
        spec.environ,
        spec.cwd,
        spec.shell,
        spec.timeout_seconds,
        spec.flake_retries,
        spec.timeout_retries,
    )


# Languages added after the initial migration that do not exist on master.
_POST_MIGRATION_LANGUAGES = {"rust"}

# Test cases added after the initial migration that do not exist on master.
_POST_MIGRATION_TEST_CASES = set()


class MigrationParityTest(unittest.TestCase):
    """Temporary migration verification tests comparing the refactored harness against master."""

    @classmethod
    def setUpClass(cls):
        cls.master_rit = _load_master_run_interop_tests()
        cls.master_cm = _load_master_client_matrix()

    def test_parity_deleted_legacy_files(self):
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
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    _REPO_ROOT, "tools/interop_matrix/client_matrix.py"
                )
            )
        )
        for obsolete_rel in (
            "tools/run_tests/run_interop_tests.py",
            "tools/interop_matrix/create_matrix_images.py",
            "tools/interop_matrix/run_interop_matrix_tests.py",
            "tools/run_tests/dockerize/build_interop_image.sh",
            "tools/internal_ci/helper_scripts/prepare_build_interop_rc",
            "tools/internal_ci/helper_scripts/prepare_build_macos_interop_rc",
        ):
            self.assertFalse(
                os.path.exists(os.path.join(_REPO_ROOT, obsolete_rel)),
                f"Obsolete wrapper {obsolete_rel} should be deleted",
            )

    def test_parity_docker_base_stages_match_master(self):
        """Verify every Dockerfile `AS base` stage is byte-identical to master.

        The base stage is hashed by tools/dockerfile/push_testing_images.sh, so
        byte parity keeps every pinned .current_version image valid and the
        execution environment of every interop image identical to master.
        """
        pattern = os.path.join(
            _REPO_ROOT, "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile"
        )
        paths = sorted(glob.glob(pattern))
        self.assertTrue(paths)
        for path in paths:
            rel = os.path.relpath(path, _REPO_ROOT)
            safename = os.path.basename(os.path.dirname(path))[
                len("grpc_interop_") :
            ]
            if safename in _POST_MIGRATION_LANGUAGES:
                continue
            master_text = subprocess.check_output(
                ["git", "-C", _REPO_ROOT, "show", "master:%s" % rel], text=True
            )
            with open(path) as f:
                text = f.read()
            base = text.split("\n#=== INTEROP_BUILD_STAGE ===\n", 1)[0] + "\n"
            base_lines = [
                re.sub(r"^(FROM .*) AS base$", r"\1", line.rstrip("\n")) + "\n"
                for line in base.splitlines(keepends=True)
                if line != "ARG BASE_IMAGE=base\n"
            ]
            self.assertEqual("".join(base_lines), master_text, rel)

    def test_parity_kokoro_environment_preparation(self):
        """Verify the runner prepares a Kokoro worker as master's deleted rc did.

        master's grpc_run_interop_tests.sh sourced prepare_build_interop_rc,
        which cloned the sibling repositories and copied the prod service
        account key; the runner does both before Docker builds. The rc's
        `git submodule update --init` is still run by prepare_build_linux_rc.
        """
        rc = subprocess.check_output(
            [
                "git",
                "-C",
                _REPO_ROOT,
                "show",
                "master:tools/internal_ci/helper_scripts/prepare_build_interop_rc",
            ],
            text=True,
        )
        rc_clones = set(
            re.findall(r"git clone --recursive (\S+) \./\.\./(grpc-[\w-]+)", rc)
        )
        self.assertEqual(len(rc_clones), 5)
        rc_key = re.search(
            r'cp "\$\{KOKORO_KEYSTORE_DIR\}/(\S+)" ~/service_account/(\S+) ', rc
        )
        self.assertIsNotNone(rc_key)

        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        grpc_root = os.path.join(tmp, "grpc")
        keystore = os.path.join(tmp, "keystore")
        home = os.path.join(tmp, "home")
        for d in (grpc_root, keystore, home):
            os.makedirs(d)
        with open(os.path.join(keystore, rc_key.group(1)), "w") as f:
            f.write("{}")
        calls = []

        def fake_check_call(cmd, **unused_kwargs):
            calls.append(list(cmd))
            if cmd[:2] == ["git", "clone"]:
                os.makedirs(cmd[-1])
            return 0

        languages = [
            lang
            for name, lang in runner._LANGUAGES.items()
            if lang.docker_supported and name not in _POST_MIGRATION_LANGUAGES
        ]
        env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": home,
            "KOKORO_KEYSTORE_DIR": keystore,
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("subprocess.check_call", fake_check_call):
                runner.prepare_docker_build_environment(
                    languages, grpc_root=grpc_root
                )
        clones = {
            (c[3][: -len(".git")] if c[3].endswith(".git") else c[3], c[4])
            for c in calls
            if c[:3] == ["git", "clone", "--recursive"]
        }
        self.assertEqual(
            clones,
            {(url, os.path.join(tmp, dirname)) for url, dirname in rc_clones},
        )
        key_path = os.path.join(home, "service_account", rc_key.group(2))
        self.assertTrue(os.path.isfile(key_path))
        with open(
            os.path.join(
                _REPO_ROOT,
                "tools/internal_ci/helper_scripts/prepare_build_linux_rc",
            )
        ) as f:
            self.assertIn("\ngit submodule update --init\n", f.read())
        with open(
            os.path.join(
                _REPO_ROOT, "tools/internal_ci/linux/grpc_run_interop_tests.sh"
            )
        ) as f:
            self.assertIn(
                "\nsource tools/internal_ci/helper_scripts/prepare_build_linux_rc\n",
                f.read(),
            )

    def test_parity_dockerignore_extends_master(self):
        """Verify .dockerignore keeps every master pattern and only adds host build trees."""

        def patterns(text):
            return [
                line.strip()
                for line in text.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]

        master_patterns = patterns(
            subprocess.check_output(
                ["git", "-C", _REPO_ROOT, "show", "master:.dockerignore"],
                text=True,
            )
        )
        with open(os.path.join(_REPO_ROOT, ".dockerignore")) as f:
            ours = patterns(f.read())
        kept = [p for p in ours if p in master_patterns]
        self.assertEqual(kept, master_patterns)
        self.assertEqual(
            [p for p in ours if p not in master_patterns],
            ["/cmake/build/", "/py27/", "/py39/", "/py310/", "!third_party/**"],
        )
        # Why the third_party exception exists: master's patterns drop vendored
        # sources that the interop images compile.
        master_rules = _dockerignore_rules(
            subprocess.check_output(
                ["git", "-C", _REPO_ROOT, "show", "master:.dockerignore"],
                text=True,
            )
        )
        self.assertTrue(
            _dockerignore_excludes(
                master_rules,
                "third_party/boringssl-with-bazel/crypto/obj/obj.cc",
            )
        )

    def test_parity_languages_with_master(self):
        """Verify 100% parity of active _LANGUAGES commands, working directories, and eligibility against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = runner._LANGUAGES
        self.assertEqual(
            set(m_langs.keys()),
            set(c_langs.keys()) - _POST_MIGRATION_LANGUAGES,
        )

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
            self.assertEqual(ml.global_env(), cl.global_env())
            self.assertEqual(
                ml.client_cmd(sample_args), cl.client_cmd(sample_args)
            )
            if hasattr(ml, "server_cmd"):
                self.assertEqual(
                    getattr(ml, "server_cwd", None),
                    getattr(cl, "server_cwd", None),
                )
                self.assertEqual(
                    ml.server_cmd(sample_args), cl.server_cmd(sample_args)
                )
            else:
                self.assertFalse(
                    hasattr(cl, "server_cmd"),
                    f"{name} should not have server_cmd",
                )
            if hasattr(cl, "client_cmd_http2interop"):
                self.assertEqual(
                    getattr(ml, "http2_cwd", None),
                    getattr(cl, "http2_cwd", None),
                )
                self.assertEqual(
                    ml.client_cmd_http2interop(sample_args),
                    cl.client_cmd_http2interop(sample_args),
                )
            for tc in [
                t
                for t in runner._TEST_CASES
                if t not in _POST_MIGRATION_TEST_CASES
            ]:
                self.assertEqual(
                    tc not in ml.unimplemented_test_cases(),
                    cl.supports_client(tc, "tls"),
                    f"Client eligibility mismatch for {name}, {tc}",
                )
                if hasattr(ml, "server_cmd") and name != "pythonasyncio":
                    self.assertEqual(
                        tc not in ml.unimplemented_test_cases_server(),
                        cl.supports_server(tc, "tls"),
                        f"Server eligibility mismatch for {name}, {tc}",
                    )

    def test_parity_c2p_and_c2p_auth_jobspecs(self):
        """Verify 100% equivalence of cloud_to_prod and cloud_to_prod_auth JobSpecs against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = runner._LANGUAGES
        m_c2p = self.master_rit["cloud_to_prod_jobspec"]

        for name in sorted(m_langs.keys()):
            ml = m_langs[name]
            cl = c_langs[name]
            img = None if name == "objc" else "img:1"
            for tc in [
                t
                for t in runner._TEST_CASES
                if t not in _POST_MIGRATION_TEST_CASES
            ]:
                for cred_type in runner._CUSTOM_CREDENTIALS_TYPE_OPTIONS:
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
                        cj = runner.cloud_to_prod_jobspec(
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

            for tc in [
                t
                for t in runner._AUTH_TEST_CASES
                if t not in _POST_MIGRATION_TEST_CASES
            ]:
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
                            cj = runner.cloud_to_prod_jobspec(
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

    def test_parity_c2c_and_http2_jobspecs(self):
        """Verify 100% equivalence of cloud_to_cloud and HTTP/2 JobSpecs against master."""
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = runner._LANGUAGES
        m_c2c = self.master_rit["cloud_to_cloud_jobspec"]

        for sname in [
            s for s in runner._SERVERS if s not in _POST_MIGRATION_LANGUAGES
        ]:
            ms = m_langs[sname]
            srv_ep = runner.get_server_endpoint(sname)
            for cname in sorted(m_langs.keys()):
                ml = m_langs[cname]
                cl = c_langs[cname]
                for tc in [
                    t
                    for t in runner._TEST_CASES
                    if t not in _POST_MIGRATION_TEST_CASES
                ]:
                    m_ok = (
                        tc not in ml.unimplemented_test_cases()
                        and tc not in ms.unimplemented_test_cases_server()
                    )
                    c_ok = cl.supports(tc, "tls") and srv_ep.supports_server(
                        tc, "tls"
                    )
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
                        cj = runner.cloud_to_cloud_jobspec(
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

    def test_parity_dedicated_server_jobspecs(self):
        """Verify the MCS connection scaling server and client JobSpecs match master.

        master runs max_concurrent_streams_connection_scaling when "java" is a
        requested server: it starts an extra Java server with
        --max_concurrent_streams_limit=2, sleeps 30s, and runs every
        _LANGUAGES_FOR_MCS_TEST_CASE client against it as "java-mcs".
        """
        tc = "max_concurrent_streams_connection_scaling"
        spec = runner._DEDICATED_SERVER_TEST_CASE_SPECS[tc]
        self.assertEqual(spec["server"], "java")
        self.assertEqual(spec["server_name"], "java-mcs")
        self.assertEqual(spec["server_startup_seconds"], 30)
        m_langs = self.master_rit["_LANGUAGES"]
        c_langs = runner._LANGUAGES

        def norm(cmdline):
            return [
                "--name=NORM" if x.startswith("--name=") else x for x in cmdline
            ]

        def fields(job):
            return (
                norm(job.cmdline),
                job.shortname,
                job.environ,
                job.cwd,
                job.timeout_seconds,
                job.flake_retries,
                job.timeout_retries,
            )

        for ts in runner._TRANSPORT_SECURITY_OPTIONS:
            self.assertEqual(
                {
                    name
                    for name, lang in c_langs.items()
                    if name not in _POST_MIGRATION_LANGUAGES
                    and lang.supports(tc, ts)
                },
                set(self.master_rit["_LANGUAGES_FOR_MCS_TEST_CASE"]),
                ts,
            )
            ms = self.master_rit["server_jobspec"](
                m_langs[spec["server"]],
                "img:1",
                ts,
                max_concurrent_streams_limit=2,
            )
            cs = runner.server_jobspec(
                c_langs[spec["server"]],
                "img:1",
                ts,
                extra_server_flags=spec["server_flags"],
            )
            self.assertEqual(fields(ms), fields(cs), ts)
            for cname in self.master_rit["_LANGUAGES_FOR_MCS_TEST_CASE"]:
                mj = self.master_rit["cloud_to_cloud_jobspec"](
                    m_langs[cname],
                    tc,
                    "java-mcs",
                    "localhost",
                    8080,
                    docker_image="img:1",
                    transport_security=ts,
                )
                cj = runner.cloud_to_cloud_jobspec(
                    c_langs[cname],
                    tc,
                    spec["server_name"],
                    "localhost",
                    8080,
                    docker_image="img:1",
                    transport_security=ts,
                )
                self.assertEqual(fields(mj), fields(cj), (cname, ts))

    def test_parity_matrix_releases_and_jobspecs(self):
        """Verify all 555 matrix releases and 9,990 matrix JobSpecs match master.

        Each JobSpec is compared field by field against the JobSpec that
        master's run_interop_matrix_tests.py generates from master's
        testcases/* files, with --server_host pinned to master's old default.
        """
        self.assertEqual(
            {
                k: v
                for k, v in runner.LANG_RUNTIME_MATRIX.items()
                if k not in _POST_MIGRATION_LANGUAGES
            },
            self.master_cm["LANG_RUNTIME_MATRIX"],
        )
        self.assertEqual(
            {
                k: v
                for k, v in runner.LANG_RELEASE_MATRIX.items()
                if k not in _POST_MIGRATION_LANGUAGES
            },
            self.master_cm["LANG_RELEASE_MATRIX"],
        )

        tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_dir, ignore_errors=True)
        master_generate = _load_master_matrix_generator(self.master_cm, tmp_dir)

        total_releases = 0
        total_jobspecs = 0
        for lang in runner.LANG_RUNTIME_MATRIX:
            if lang in _POST_MIGRATION_LANGUAGES:
                for rt in runner.LANG_RUNTIME_MATRIX[lang]:
                    with mock.patch("builtins.print"):
                        specs = runner.generate_test_case_jobspecs(
                            lang, rt, "master", f"{lang}__{rt}_master"
                        )
                    self.assertEqual(len(specs), 18)
                continue
            self.assertEqual(
                runner.get_github_repo(lang),
                self.master_cm["get_github_repo"](lang),
                f"GitHub repo mismatch for {lang}",
            )
            for rel in runner.get_release_tags(lang):
                for rt in runner.get_runtimes_for_lang_release(lang, rel):
                    total_releases += 1
                    suite_name = f"{lang}__{rt}_{rel}"
                    with mock.patch("builtins.print"):
                        specs = runner.generate_test_case_jobspecs(
                            lang,
                            rt,
                            rel,
                            suite_name,
                            server_host=_LEGACY_MATRIX_SERVER_HOST,
                        )
                        legacy_specs = master_generate(
                            lang, rt, rel, suite_name
                        )
                    self.assertEqual(len(specs), 18)
                    self.assertEqual(
                        sorted(
                            (
                                _matrix_jobspec_fields(j, legacy=True)
                                for j in legacy_specs
                            ),
                            key=lambda f: f[0],
                        ),
                        sorted(
                            (_matrix_jobspec_fields(j) for j in specs),
                            key=lambda f: f[0],
                        ),
                        f"Matrix JobSpec mismatch for {suite_name}",
                    )
                    total_jobspecs += len(specs)
        self.assertEqual(total_releases, 562)
        self.assertEqual(total_jobspecs, 562 * 18)

    def test_parity_matrix_build_jobspecs(self):
        """Verify all 562 matrix-build image JobSpecs match master.

        Both harnesses build every matrix language/release/runtime from a
        simulated release checkout that ships interop-last's image builder (as
        every release cut before tools/interop/ existed does). Languages built
        from the release tag must get master's JobSpec exactly: the release's
        own builder, same environment. Languages built from HEAD Dockerfiles
        (separate repos) may differ only in the builder command.
        """
        master_cmi = _load_master_create_matrix_images(self.master_cm)
        tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_dir, ignore_errors=True)
        stack_base = os.path.join(tmp_dir, "checkout")
        builder = os.path.join(stack_base, runner._LEGACY_IMAGE_BUILDER)
        os.makedirs(os.path.dirname(builder))
        with open(builder, "w") as f:
            f.write("#!/bin/bash\n")
        parsed_args = runner.build_matrix_build_argparser().parse_args(
            ["--git_checkout", "--release", "all"]
        )
        captured = []

        def fake_run(jobs, *unused_args, **unused_kwargs):
            captured.append(list(jobs))
            return 0, {}

        def fields(job):
            return (
                list(job.cmdline),
                job.environ,
                job.shortname,
                job.timeout_seconds,
                job.tag,
            )

        total = 0
        master_cmi["checkout_grpc_stack"] = lambda lang, release: stack_base
        with contextlib.ExitStack() as stack:
            for patcher in (
                mock.patch.object(master_cmi["jobset"], "run", fake_run),
                mock.patch.object(runner.jobset, "run", fake_run),
                mock.patch.object(
                    runner, "checkout_grpc_stack", lambda **unused: stack_base
                ),
                mock.patch("builtins.print"),
                mock.patch.object(master_cmi["jobset"], "message"),
                mock.patch.object(runner.jobset, "message"),
            ):
                stack.enter_context(patcher)
            for lang in self.master_cm["LANG_RUNTIME_MATRIX"]:
                # master's build_image_jobspec reads the module-global `lang`.
                master_cmi["lang"] = lang
                from_tag = self.master_cm[
                    "should_build_docker_interop_image_from_release_tag"
                ](lang)
                for release in self.master_cm["get_release_tags"](lang):
                    captured.clear()
                    master_cmi["build_all_images_for_release"](lang, release)
                    runner.build_all_images_for_release(
                        lang, release, parsed_args, []
                    )
                    m_jobs, n_jobs = captured
                    self.assertEqual(len(m_jobs), len(n_jobs), (lang, release))
                    for m_job, n_job in zip(m_jobs, n_jobs):
                        if from_tag:
                            self.assertEqual(
                                fields(m_job), fields(n_job), (lang, release)
                            )
                        else:
                            self.assertEqual(
                                fields(m_job)[1:],
                                fields(n_job)[1:],
                                (lang, release),
                            )
                            self.assertEqual(
                                m_job.cmdline, [master_cmi["_IMAGE_BUILDER"]]
                            )
                            self.assertEqual(n_job.cmdline[-1], "build-image")
                        total += 1
        self.assertEqual(total, 562)

    def test_parity_http2_and_derived_language_sets(self):
        """Verify Http2Server/Http2Client parity, callable supports_server, and derived language set parity."""
        for name, c_inst in runner._LANGUAGES.items():
            self.assertTrue(
                callable(c_inst.supports_server),
                f"{name}.supports_server must be callable (not shadowed by bool)",
            )
        for cls_name in ("Http2Server", "Http2Client"):
            c_inst = getattr(runner, cls_name)()
            m_inst = self.master_rit[cls_name]()
            self.assertEqual(str(m_inst), str(c_inst))
            self.assertEqual(m_inst.safename, c_inst.safename)
            self.assertEqual(m_inst.global_env(), c_inst.global_env())
            if hasattr(m_inst, "server_cmd"):
                self.assertEqual(
                    m_inst.server_cmd(["--port=8080"]),
                    c_inst.server_cmd(["--port=8080"]),
                )
            if hasattr(m_inst, "client_cmd"):
                self.assertEqual(
                    m_inst.client_cmd(["--server_port=8080"]),
                    c_inst.client_cmd(["--server_port=8080"]),
                )
        self.assertEqual(
            set(runner._SERVERS) - _POST_MIGRATION_LANGUAGES,
            set(self.master_rit["_SERVERS"]) - {"pythonasyncio"},
        )


class InteropInvariantTest(unittest.TestCase):
    """Permanent architectural, schema, and behavioral invariant tests for the interop harness."""

    def test_invariant_static_two_stage_dockerfiles_and_contract(self):
        """Verify all grpc_interop_* Dockerfiles have base + interop stages and /interop/* contract."""
        dockerfiles = sorted(
            glob.glob(
                os.path.join(
                    _REPO_ROOT,
                    "tools/dockerfile/interoptest/grpc_interop_*/Dockerfile",
                )
            )
        )
        self.assertEqual(len(dockerfiles), 17 + len(_POST_MIGRATION_LANGUAGES))
        dockerfile_dirs = {df.split("/")[-2] for df in dockerfiles}
        for lang in runner._LANGUAGES.values():
            if lang.docker_supported:
                self.assertIn(f"grpc_interop_{lang.safename}", dockerfile_dirs)
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

    def test_invariant_zero_hardcoded_language_names_in_runner(self):
        """Verify AST of runner.py has zero hardcoded language literals and no mutable args global."""
        forbidden = (
            {
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
            | set(runner._LANGUAGES.keys())
            | {l.safename for l in runner._LANGUAGES.values()}
            | set(runner.LANG_RUNTIME_MATRIX.keys())
        )
        path = os.path.join(_REPO_ROOT, "tools/interop/runner.py")
        with open(path) as f:
            tree = ast.parse(f.read(), filename=path)
        hits = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value.strip().lower() in forbidden:
                    hits.append((node.lineno, node.value))
        self.assertEqual(
            hits, [], f"Forbidden language literal in {path}: {hits}"
        )
        self.assertFalse(
            hasattr(runner, "args"),
            "runner.py must not expose a mutable module-level args global",
        )

    def test_invariant_single_file_config_json_capabilities(self):
        """Verify tools/interop/config.json defines all 23 capabilities and active languages + http2."""
        with open(os.path.join(_REPO_ROOT, "tools/interop/config.json")) as f:
            cfg = json.load(f)
        self.assertEqual(len(cfg["capabilities"]), 23)
        self.assertEqual(len(runner.Capability), 23)
        self.assertEqual(
            len(cfg["languages"]), 14 + len(_POST_MIGRATION_LANGUAGES)
        )
        for lname, lcfg in cfg["languages"].items():
            self.assertNotIn(
                "docker_build",
                lcfg,
                f"Dead docker_build block should not be in {lname}",
            )

    def test_invariant_contract_entrypoints_mode(self):
        """Verify --contract-entrypoints switches client/server/http2 commands to /interop/*."""
        cxx = runner._LANGUAGES["c++"]
        job_c2c = runner.cloud_to_cloud_jobspec(
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
        job_srv = runner.server_jobspec(
            cxx,
            "grpc_interop_cxx:test",
            use_contract_entrypoints=True,
        )
        self.assertTrue(
            any(x.startswith("/interop/server ") for x in job_srv.cmdline),
            job_srv.cmdline,
        )

    def test_invariant_five_bug_fixes_and_adversarial_cases(self):
        """Verify all 5 bug fixes from master_plan_of_action.md and adversarial edge cases."""
        # Bug 1: commands.py use_asyncio branch in run_client & run_server + GatherProto fix
        with open(
            os.path.join(_REPO_ROOT, "src/python/grpcio_tests/commands.py")
        ) as f:
            cmds_src = f.read()
        self.assertIn("from tests_aio.interop import client", cmds_src)
        self.assertIn("client.test_interoperability()", cmds_src)
        self.assertIn(
            "server.interop_server_lib.parse_interop_server_arguments", cmds_src
        )
        self.assertNotIn("TODO(atash)", cmds_src)

        # Bug 2: pythonasyncio excluded from _SERVERS but accepted in -s CLI choices
        self.assertNotIn("pythonasyncio", runner._SERVERS)
        parsed = runner.argp.parse_args(["-s", "pythonasyncio"])
        self.assertEqual(parsed.server, ["pythonasyncio"])

        # Bug 3: Docker image build deduplication by safename
        build_jobs, images = runner.create_docker_build_jobs(
            [
                runner._LANGUAGES["java"],
                runner._LANGUAGES["javaokhttp"],
                runner.Http2Client(),
                runner.Http2Server(),
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
        self.assertIn("v1.20.0", runner.LANG_RELEASE_MATRIX["csharp"])

        # Divide-by-zero guard in aggregate_http2_results when all cases are skipped
        res = runner.aggregate_http2_results(
            '{"cases":[{"name":"c1","skipped":true,"passed":false}]}'
        )
        self.assertEqual(res["percent"], 0.0)
        self.assertEqual(res["skipped"], 1)

        # Shell token quoting in bash_cmdline
        quoted_cmd = runner.bash_cmdline(["bin/cmd", "arg with space", "a'b"])
        self.assertEqual(
            quoted_cmd,
            ["bash", "-c", "bin/cmd 'arg with space' 'a'\"'\"'b'"],
        )

        # pull_image_for_lang uses docker pull (not deprecated gcloud docker -- pull)
        pull_job = runner.pull_image_for_lang("cxx", "img:v1", "v1")
        self.assertIn("time docker pull img:v1", pull_job.cmdline[0])
        self.assertNotIn("gcloud docker", pull_job.cmdline[0])

    def test_invariant_override_server_and_ipv6(self):
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
                    runner.argp.parse_args(["--override_server", bad])

        parsed_ipv6 = runner.argp.parse_args(
            ["--override_server", "c++=[::1]:8080"]
        )
        self.assertEqual(parsed_ipv6.override_server, [["c++", "[::1]:8080"]])
        host, port = runner._split_host_port("[::1]:8080")
        self.assertEqual((host, port), ("[::1]", "8080"))

        ext_srv = runner.get_server_endpoint("external_custom_server")
        self.assertTrue(ext_srv.supports_server("large_unary", "tls"))

    def test_invariant_objc_manual_run_and_contract_entrypoints(self):
        """Verify objc works with ${SERVER_PORT}, preserves client_cwd, and never wraps in docker."""
        objc = runner._LANGUAGES["objc"]
        cmd_manual = objc.client_cmd(["--server_port=${SERVER_PORT}"])
        self.assertIsNotNone(cmd_manual)
        self.assertIn(
            "HOST_PORT_LOCALSSL=localhost:${SERVER_PORT}", cmd_manual[0]
        )

        c2c_job = runner.cloud_to_cloud_jobspec(
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

        c2p_job = runner.cloud_to_prod_jobspec(
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

    def test_invariant_contract_entrypoints_http2_and_matrix(self):
        """Verify Http2Server ignores args under --contract-entrypoints and matrix preserves shortnames."""
        h2_srv = runner.Http2Server(use_contract=True)
        srv_job = runner.server_jobspec(
            h2_srv,
            "grpc_interop_http2:test",
            use_contract_entrypoints=True,
        )
        self.assertEqual(
            srv_job.cmdline[-3:], ["bash", "-c", "/interop/server"]
        )

        # Default server_host=None preserves DNS hosts from config.json
        with mock.patch("builtins.print"):
            default_specs = runner.generate_test_case_jobspecs(
                "cxx",
                "cxx",
                "v1.76.0",
                "cxx__cxx_v1.76.0",
            )
        self.assertIn(
            "--server_host=grpc-test.sandbox.googleapis.com",
            default_specs[0].cmdline,
        )
        self.assertIn(
            "--server_host=grpc-test4.sandbox.googleapis.com",
            default_specs[-1].cmdline,
        )

        # Explicit server_host overrides --server_host and injects --server_host_override
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
                "--server_host=74.125.206.210",
                spec.cmdline,
            )
            self.assertIn(
                ".sandbox.googleapis.com",
                spec.cmdline,
            )

    def test_invariant_matrix_cli_flags_and_build_image_jobspec(self):
        """Verify matrix CLI flags (--docker_path/--gcr_path, --report_file, --release) and build_matrix_image_jobspec."""
        p_build = runner.build_matrix_build_argparser().parse_args(
            ["--docker_path", "gcr.io/custom", "-l", "all", "cxx"]
        )
        self.assertEqual(p_build.docker_path, "gcr.io/custom")

        p_test = runner.build_matrix_test_argparser().parse_args(
            ["--docker_path", "gcr.io/custom"]
        )
        self.assertEqual(p_test.docker_path, "gcr.io/custom")
        self.assertEqual(p_test.report_file, "report.xml")
        self.assertIsNone(p_test.server_host)
        with self.assertRaises(SystemExit):
            with mock.patch("sys.stderr", new=io.StringIO()):
                runner.build_matrix_test_argparser().parse_args(
                    ["--release", "master"]
                )

        expected_cmd = [
            sys.executable,
            os.path.abspath(runner.__file__),
            "build-image",
        ]
        job_go = runner.build_matrix_image_jobspec(
            "go1.8", {"GRPC_GO_ROOT": "/tmp/go"}, "v1.0.5", "/tmp/go", "go"
        )
        self.assertEqual(job_go.cmdline, expected_cmd)
        self.assertNotIn("INTEROP_DOCKERFILE_ROOT", job_go.environ)

        job_cxx = runner.build_matrix_image_jobspec(
            "cxx", {"GRPC_ROOT": "/tmp/cxx"}, "v1.0.1", "/tmp/cxx", "cxx"
        )
        self.assertEqual(job_cxx.cmdline, expected_cmd)
        self.assertEqual(
            job_cxx.environ.get("INTEROP_DOCKERFILE_ROOT"), "/tmp/cxx"
        )

    def test_invariant_docker_cleanup_and_healthcheck(self):
        """Verify wait_for_healthy quote stripping, add_files_to_image, and -s pythonasyncio without docker."""
        with mock.patch("subprocess.check_output", return_value=b'"healthy"\n'):
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
                self.assertEqual(
                    cmd[:4], ["docker", "build", "-t", "my_image:v1"]
                )
                self.assertIn("-f", cmd)
        finally:
            os.unlink(temp_commit)

        # -s pythonasyncio without --use_docker must exit 1
        with self.assertRaises(SystemExit) as ctx:
            with mock.patch("builtins.print"):
                runner.main(["-s", "pythonasyncio", "-l", "c++"])
        self.assertEqual(ctx.exception.code, 1)

    def test_invariant_node_dockerfiles_and_legacy_fallback(self):
        """Verify Node Dockerfiles use bash -l and runner.py build-image dry-run works for modern + legacy."""
        for node_dir in ("grpc_interop_node", "grpc_interop_nodepurejs"):
            df_path = os.path.join(
                _REPO_ROOT,
                "tools/dockerfile/interoptest",
                node_dir,
                "Dockerfile",
            )
            with open(df_path) as f:
                df_text = f.read()
            self.assertIn(
                '/bin/bash -l -c "cd /home/appuser/grpc-node', df_text
            )
            self.assertIn("#!/bin/bash -l\\n", df_text)

        with open(os.path.join(_REPO_ROOT, ".dockerignore")) as f:
            ignore_patterns = {
                line.strip().rstrip("/")
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            }
        # Host build trees must not leak into the 'grpc' build context.
        for host_tree in ("/cmake/build", "/py27", "/py39", "/py310"):
            self.assertIn(host_tree, ignore_patterns)

        cmd_modern = runner.build_docker_image(
            "grpc_interop_cxx:dryrun", "grpc_interop_cxx", dry_run=True
        )
        self.assertEqual(cmd_modern[:3], ["docker", "buildx", "build"])
        self.assertIn("--load", cmd_modern)
        # Build contexts are never mounted inside one another.
        self.assertFalse(any("grpc_git" in arg for arg in cmd_modern))

        # A checkout without #=== INTEROP_BUILD_STAGE === (any release cut
        # before tools/interop/ existed) is never built by build-image ...
        temp_root = tempfile.mkdtemp()
        try:
            legacy_dir = os.path.join(
                temp_root,
                "tools/dockerfile/interoptest/grpc_interop_csharp",
            )
            os.makedirs(legacy_dir)
            with open(os.path.join(legacy_dir, "Dockerfile"), "w") as f:
                f.write("FROM debian:stretch\nRUN echo base\n")
            with self.assertRaises(ValueError):
                runner.build_docker_image(
                    "grpc_interop_csharp:v1.10.0",
                    "grpc_interop_csharp",
                    environ={
                        "GRPC_ROOT": temp_root,
                        "INTEROP_DOCKERFILE_ROOT": temp_root,
                    },
                    dry_run=True,
                )
            # ... matrix-build runs that release's own interop-last builder,
            # while checkouts without one use build-image.
            root_env = {"GRPC_ROOT": temp_root}
            job = runner.build_matrix_image_jobspec(
                "csharp", root_env, "v1.10.0", temp_root, "csharp"
            )
            self.assertEqual(job.cmdline[-1], "build-image")
            self.assertEqual(job.environ["INTEROP_DOCKERFILE_ROOT"], temp_root)
            builder = os.path.join(temp_root, runner._LEGACY_IMAGE_BUILDER)
            os.makedirs(os.path.dirname(builder))
            with open(builder, "w") as f:
                f.write("#!/bin/bash\n")
            job = runner.build_matrix_image_jobspec(
                "csharp", root_env, "v1.10.0", temp_root, "csharp"
            )
            self.assertEqual(job.cmdline, [builder])
            self.assertNotIn("INTEROP_DOCKERFILE_ROOT", job.environ)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    def test_invariant_docker_context_keeps_tracked_files(self):
        """Verify .dockerignore keeps every tracked file in the 'grpc' build context.

        Interop images copy the repo root, including checked-out submodules,
        and build gRPC from it, so a pattern matching a tracked directory (for
        example '**/obj/' and BoringSSL's crypto/obj/) breaks those builds.
        Covers third_party only when submodules are checked out.
        """
        with open(os.path.join(_REPO_ROOT, ".dockerignore")) as f:
            rules = _dockerignore_rules(f.read())
        for path in (
            "third_party/boringssl-with-bazel/crypto/obj/obj.cc",
            "third_party/protobuf/python/dist/setup.py",
            "src/core/lib/surface/call.cc",
        ):
            self.assertFalse(_dockerignore_excludes(rules, path), path)
        tracked = subprocess.check_output(
            ["git", "-C", _REPO_ROOT, "ls-files", "--recurse-submodules"],
            text=True,
        ).splitlines()
        excluded = [
            path
            for path in tracked
            if not path.startswith(".github/")  # CI metadata, never built
            and _dockerignore_excludes(rules, path)
        ]
        self.assertEqual(excluded, [])

    def test_invariant_config_schema_and_ergonomics_validation(self):
        """Verify config.schema.json completeness, config.json compactness (<380 lines), and negative schema validation."""
        cfg_path = os.path.join(_REPO_ROOT, "tools/interop/config.json")
        schema_path = os.path.join(
            _REPO_ROOT, "tools/interop/config.schema.json"
        )
        with open(cfg_path) as f:
            cfg_lines = f.readlines()
            cfg = json.loads("".join(cfg_lines))
        with open(schema_path) as f:
            schema = json.load(f)

        self.assertLess(len(cfg_lines), 380, "config.json must be < 380 lines")
        self.assertEqual(cfg.get("$schema"), "./config.schema.json")
        self.assertEqual(
            schema.get("$schema"),
            "https://json-schema.org/draft/2020-12/schema",
        )
        top_desc = schema.get("description", "")
        self.assertIn("HOW TO ADD A LANGUAGE", top_desc)
        self.assertIn("HOW TO ADD A TEST CASE", top_desc)
        self.assertIn("HOW TO ADD A MATRIX RELEASE", top_desc)
        self.assertIn("SYNC RULE", top_desc)
        for prop_name, prop_spec in schema["properties"].items():
            self.assertTrue(
                "description" in prop_spec or "$ref" in prop_spec,
                f"Top-level property {prop_name} missing description/$ref",
            )
        for def_name, def_spec in schema["$defs"].items():
            self.assertIn(
                "description",
                def_spec,
                f"$defs.{def_name} missing description",
            )
            for p_name, p_spec in def_spec.get("properties", {}).items():
                self.assertIn(
                    "description",
                    p_spec,
                    f"$defs.{def_name}.properties.{p_name} missing description",
                )

        runner.validate_config(cfg, schema)

        def assert_invalid(mutator, msg):
            bad = copy.deepcopy(cfg)
            mutator(bad)
            with self.assertRaises(ValueError, msg=msg):
                runner.validate_config(bad, schema)

        assert_invalid(
            lambda d: d.pop("test_cases"),
            "Missing required top-level key must fail",
        )
        assert_invalid(
            lambda d: d.update({"unknown_top_key": 123}),
            "Unknown top-level property must fail",
        )
        assert_invalid(
            lambda d: d["languages"]["dart"].update({"bogus_field": True}),
            "Unknown language property must fail",
        )
        assert_invalid(
            lambda d: d["languages"]["dart"]["client_capabilities"].append(
                "NOT_A_CAPABILITY"
            ),
            "Invalid capability enum must fail",
        )
        assert_invalid(
            lambda d: d["capabilities"].append("CORE_INTEROP"),
            "Duplicate capability in uniqueItems array must fail",
        )
        assert_invalid(
            lambda d: d["languages"]["objc"].update(
                {"docker_supported": "yes"}
            ),
            "Wrong type for boolean field must fail",
        )
        assert_invalid(
            lambda d: d["languages"]["objc"].pop("repo"),
            "Missing required repo on language entry must fail",
        )
        assert_invalid(
            lambda d: d["languages"]["objc"].pop("client_capabilities"),
            "Missing required client_capabilities must fail",
        )
        assert_invalid(
            lambda d: d["languages"]["objc"].update(
                {"add_client_capabilities": ["ORCA"]}
            ),
            "Disallowed add_client_capabilities property must fail",
        )
        assert_invalid(
            lambda d: d["test_cases"]["standard"]["large_unary"][
                "capabilities"
            ].append("UNKNOWN_CAP"),
            "Unknown capability in test_cases must fail",
        )
        assert_invalid(
            lambda d: d["matrix"]["languages"]["cxx"][
                "release_overrides"
            ].update({"v1.19.0..v1.0.1": {"testcases_file": "cxx__v1.0.1"}}),
            "Reversed release range in matrix release_overrides must fail",
        )
        assert_invalid(
            lambda d: d["matrix"]["languages"]["cxx"][
                "release_overrides"
            ].update({"v1.19.0..v1.20.0": {"testcases_file": "cxx__v1.0.1"}}),
            "Overlapping release range in matrix release_overrides must fail",
        )
        assert_invalid(
            lambda d: d["matrix"]["languages"]["cxx"]["release_overrides"][
                "v1.0.1..v1.19.0"
            ].update({"testcases_file": "cxx__nonexistent"}),
            "Unknown testcases_file in matrix release_overrides must fail",
        )

    def test_invariant_config_extensibility_and_developer_ergonomics(self):
        """Verify adding a new language or test case requires minimal delta lines and validates against config.schema.json."""
        cfg_path = os.path.join(_REPO_ROOT, "tools/interop/config.json")
        schema_path = os.path.join(
            _REPO_ROOT, "tools/interop/config.schema.json"
        )
        with open(cfg_path) as f:
            cfg = json.load(f)
        with open(schema_path) as f:
            schema = json.load(f)

        extended = copy.deepcopy(cfg)
        extended["languages"]["rust"] = {
            "repo": "https://github.com/grpc/grpc-rust.git",
            "cwd": "../grpc-rust",
            "client_cmd": ["target/release/interop_client"],
            "server_cmd": ["target/release/interop_server"],
            "client_capabilities": ["CORE_INTEROP", "ORCA", "TLS", "INSECURE"],
            "server_capabilities": ["CORE_INTEROP", "ORCA", "TLS", "INSECURE"],
        }
        extended["test_cases"]["standard"]["custom_orca_check"] = {
            "capabilities": ["CORE_INTEROP", "ORCA"]
        }
        runner.validate_config(extended, schema)

        resolved_rust = runner._resolve_language_config(
            "rust", extended["languages"]["rust"], extended["language_defaults"]
        )
        rust_lang = runner.ConfigurableLanguage("rust", resolved_rust)
        self.assertEqual(rust_lang.safename, "rust")
        self.assertTrue(rust_lang.is_server_supported)
        self.assertEqual(rust_lang.client_cwd, "../grpc-rust")
        self.assertEqual(rust_lang.server_cwd, "../grpc-rust")
        self.assertIsNone(rust_lang.http2_cwd)
        self.assertIn(
            runner.Capability.CORE_INTEROP, rust_lang.client_capabilities
        )
        self.assertIn(runner.Capability.ORCA, rust_lang.client_capabilities)
        self.assertEqual(
            rust_lang.client_cmd(["--server_port=8080"]),
            ["target/release/interop_client", "--server_port=8080"],
        )
        derived_ctxs = runner._derive_build_contexts(extended)
        self.assertEqual(
            derived_ctxs["grpc_rust"],
            {"env_var": "GRPC_RUST_ROOT", "default_rel_path": "../grpc-rust"},
        )

        with tempfile.TemporaryDirectory() as tmp_root:
            with mock.patch("subprocess.check_call") as mock_call:
                runner.prepare_docker_build_environment(
                    [rust_lang], grpc_root=os.path.join(tmp_root, "grpc")
                )
                expected_target = os.path.join(tmp_root, "grpc-rust")
                mock_call.assert_any_call(
                    [
                        "git",
                        "clone",
                        "--recursive",
                        "https://github.com/grpc/grpc-rust.git",
                        expected_target,
                    ]
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
