#!/usr/bin/env python3
# Copyright 2024 gRPC authors.
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
"""Unit tests for Solution 4: Buildx Bake + Container-Network / Compose Interop Orchestrator."""

from collections import OrderedDict
import os
import setuptools
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(TEST_DIR, "../.."))
sys.path.insert(0, TEST_DIR)
sys.path.insert(0, os.path.join(REPO_ROOT, "tools"))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools/interop_matrix"))
sys.path.insert(0, os.path.join(REPO_ROOT, "src/python/grpcio_tests"))

import client_matrix
import create_matrix_images
from interop_bake import bake_manifest
from interop_bake import compose_manifest
import python_utils.dockerjob as dockerjob
import python_utils.jobset as jobset
import python_utils.report_utils as report_utils
import run_interop_matrix_tests
import run_interop_tests


class TestBakeManifestAndDeduplication(unittest.TestCase):
    """Tests declarative Buildx Bake HCL generation and image deduplication."""

    def test_java_and_javaokhttp_deduplicated_to_single_image(self):
        langs = [
            run_interop_tests._LANGUAGES["java"],
            run_interop_tests._LANGUAGES["javaokhttp"],
            run_interop_tests._LANGUAGES["c++"],
            run_interop_tests._LANGUAGES["objc"],
        ]
        safename_to_tag, lang_to_tag, safename_to_langs = (
            bake_manifest.deduplicate_language_builds(
                langs, tag_generator=lambda s: "grpc_interop_%s:testtag" % s
            )
        )
        self.assertEqual(list(safename_to_tag.keys()), ["cxx", "java"])
        self.assertNotIn("objc", safename_to_tag)
        self.assertNotIn("objc", lang_to_tag)
        self.assertEqual(
            lang_to_tag["java"], "grpc_interop_java:testtag"
        )
        self.assertEqual(
            lang_to_tag["javaokhttp"], "grpc_interop_java:testtag"
        )
        self.assertEqual(
            safename_to_langs["java"], ["java", "javaokhttp"]
        )

    def test_http2_client_and_server_deduplicated_to_single_image(self):
        items = [
            run_interop_tests.Http2Client(),
            run_interop_tests.Http2Server(),
            run_interop_tests._LANGUAGES["go"],
        ]
        safename_to_tag, lang_to_tag, safename_to_langs = (
            bake_manifest.deduplicate_language_builds(
                items, tag_generator=lambda s: "grpc_interop_%s:shared" % s
            )
        )
        self.assertEqual(list(safename_to_tag.keys()), ["go", "http2"])
        self.assertEqual(
            safename_to_tag["http2"], "grpc_interop_http2:shared"
        )
        self.assertEqual(lang_to_tag["http2"], "grpc_interop_http2:shared")
        self.assertEqual(safename_to_langs["http2"], ["http2"])

    def test_read_current_version_for_all_17_interop_images(self):
        for base_name in bake_manifest.DEFAULT_BASE_IMAGES:
            resolved = bake_manifest.read_current_version(REPO_ROOT, base_name)
            self.assertIn(
                "us-docker.pkg.dev/grpc-testing/testing-images-public/%s:"
                % base_name,
                resolved,
            )
            self.assertIn("@sha256:", resolved)

    def test_generate_bake_hcl_structure_and_contexts(self):
        safename_to_tag = OrderedDict(
            [
                ("cxx", "grpc_interop_cxx:tag1"),
                ("go1.19", "grpc_interop_go1.19:tag2"),
                ("java", "grpc_interop_java:tag3"),
            ]
        )
        hcl = bake_manifest.generate_bake_hcl(
            safename_to_tag,
            grpc_root=REPO_ROOT,
            release="v1.62.0",
            extra_tags_by_safename={
                "cxx": ["grpc_interop_cxx:infrastructure-public-image-tag1"]
            },
        )
        self.assertIn('group "default"', hcl)
        self.assertIn('targets = ["cxx", "go1_19", "java"]', hcl)
        self.assertIn('target "_common"', hcl)
        self.assertIn('dockerfile = "tools/interop_bake/Interop.Dockerfile"', hcl)
        self.assertIn('grpc            = "dir:${GRPC_ROOT}"', hcl)
        self.assertIn('grpc_java       = "dir:${GRPC_JAVA_ROOT}"', hcl)
        self.assertIn('grpc_go         = "dir:${GRPC_GO_ROOT}"', hcl)
        self.assertIn('target "cxx"', hcl)
        self.assertIn('target "go1_19"', hcl)
        self.assertIn('target "java"', hcl)
        self.assertIn(
            'tags = ["grpc_interop_cxx:tag1", "grpc_interop_cxx:infrastructure-public-image-tag1"]',
            hcl,
        )
        self.assertIn('default = "v1.62.0"', hcl)

    def test_static_bake_hcl_and_dockerfile_exist_and_valid(self):
        bake_path = os.path.join(
            REPO_ROOT, "tools", "interop_bake", "docker-bake.hcl"
        )
        dockerfile_path = os.path.join(
            REPO_ROOT, "tools", "interop_bake", "Interop.Dockerfile"
        )
        self.assertTrue(os.path.isfile(bake_path))
        self.assertTrue(os.path.isfile(dockerfile_path))
        with open(bake_path, "r") as f:
            bake_text = f.read()
        for target in (
            "cxx",
            "aspnetcore",
            "dart",
            "go",
            "go1_8",
            "go1_11",
            "go1_16",
            "go1_19",
            "go1_x",
            "http2",
            "java",
            "node",
            "nodepurejs",
            "php7",
            "python",
            "pythonasyncio",
            "ruby",
        ):
            self.assertIn('target "%s"' % target, bake_text)
        with open(dockerfile_path, "r") as f:
            df_text = f.read()
        self.assertIn("--mount=type=bind,from=grpc,target=/var/local/jenkins/grpc,ro", df_text)
        self.assertIn("LABEL release=", df_text)

    def test_is_buildx_bake_available_env_overrides(self):
        self.assertFalse(
            bake_manifest.is_buildx_bake_available(
                {"GRPC_INTEROP_DISABLE_BAKE": "1"}
            )
        )
        self.assertTrue(
            bake_manifest.is_buildx_bake_available(
                {"GRPC_INTEROP_FORCE_BAKE": "1"}
            )
        )


class TestContainerNetworkAndComposeOrchestrator(unittest.TestCase):
    """Tests container-network addressing, healthchecks, and Compose YAML generation."""

    def test_deterministic_service_hostnames_and_healthchecks(self):
        self.assertEqual(
            compose_manifest.server_service_name("c++"), "interop-server-cxx"
        )
        self.assertEqual(
            compose_manifest.server_service_name(
                run_interop_tests._LANGUAGES["go"]
            ),
            "interop-server-go",
        )
        self.assertEqual(
            compose_manifest.server_service_name(
                run_interop_tests.Http2Server()
            ),
            "interop-server-http2",
        )
        self.assertIn(
            "http2_server_health_check.py",
            compose_manifest.server_healthcheck_cmd("http2", 8080),
        )
        self.assertIn(
            "/dev/tcp/127.0.0.1/8080",
            compose_manifest.server_healthcheck_cmd("c++", 8080),
        )

    def test_server_and_client_jobspec_bridge_network_vs_host(self):
        cxx = run_interop_tests._LANGUAGES["c++"]
        go = run_interop_tests._LANGUAGES["go"]

        # Bridge network server jobspec
        srv_bridge = run_interop_tests.server_jobspec(
            cxx,
            "grpc_interop_cxx:t1",
            transport_security="tls",
            docker_network="interop-net",
        )
        self.assertIn("--network=interop-net", srv_bridge.cmdline)
        self.assertIn(
            "--network-alias=interop-server-cxx", srv_bridge.cmdline
        )
        self.assertTrue(
            any(a.startswith("--health-cmd=") for a in srv_bridge.cmdline)
        )

        # Legacy host server jobspec (no docker_network)
        srv_host = run_interop_tests.server_jobspec(
            cxx,
            "grpc_interop_cxx:t1",
            transport_security="tls",
        )
        self.assertNotIn("--network=interop-net", srv_host.cmdline)
        self.assertIn("-p", srv_host.cmdline)

        # Bridge network client jobspec
        cli_bridge = run_interop_tests.cloud_to_cloud_jobspec(
            go,
            "large_unary",
            "c++",
            "interop-server-cxx",
            8080,
            docker_image="grpc_interop_go:t1",
            transport_security="tls",
            docker_network="interop-net",
        )
        self.assertIn("--network=interop-net", cli_bridge.cmdline)
        self.assertNotIn("--net=host", cli_bridge.cmdline)
        self.assertIn(
            "--server_host=interop-server-cxx", " ".join(cli_bridge.cmdline)
        )
        self.assertEqual(
            cli_bridge.shortname,
            "cloud_to_cloud:go:c++_server:large_unary:tls",
        )

        # Legacy host client jobspec
        cli_host = run_interop_tests.cloud_to_cloud_jobspec(
            go,
            "large_unary",
            "c++",
            "localhost",
            32768,
            docker_image="grpc_interop_go:t1",
            transport_security="tls",
        )
        self.assertIn("--net=host", cli_host.cmdline)
        self.assertIn("--server_host=localhost", " ".join(cli_host.cmdline))

    def test_generate_compose_yaml_with_servers_and_clients(self):
        cxx = run_interop_tests._LANGUAGES["c++"]
        http2_srv = run_interop_tests.Http2Server()
        srv_specs = OrderedDict(
            [
                (
                    "c++",
                    run_interop_tests.server_jobspec(
                        cxx,
                        "grpc_interop_cxx:v1",
                        docker_network="interop-net",
                    ),
                ),
                (
                    "http2",
                    run_interop_tests.server_jobspec(
                        http2_srv,
                        "grpc_interop_http2:v1",
                        docker_network="interop-net",
                    ),
                ),
            ]
        )
        client_job = run_interop_tests.cloud_to_cloud_jobspec(
            run_interop_tests._LANGUAGES["java"],
            "ping_pong",
            "c++",
            "interop-server-cxx",
            8080,
            docker_image="grpc_interop_java:v1",
            docker_network="interop-net",
        )
        compose_dict = compose_manifest.generate_compose_dict(
            srv_specs,
            [client_job],
            docker_images={
                "c++": "grpc_interop_cxx:v1",
                "http2": "grpc_interop_http2:v1",
                "java": "grpc_interop_java:v1",
            },
            network_name="interop-net",
        )
        self.assertIn("interop-server-cxx", compose_dict["services"])
        self.assertIn("interop-server-http2", compose_dict["services"])
        self.assertEqual(
            len(compose_dict["services"]["interop-server-http2"]["ports"]), 8
        )
        client_svc = compose_dict["services"][
            "cloud-to-cloud-java-cxx-server-ping-pong-tls"
        ]
        self.assertEqual(
            client_svc["depends_on"]["interop-server-cxx"]["condition"],
            "service_healthy",
        )

        yaml_str = compose_manifest.generate_compose_yaml(
            srv_specs, [client_job], network_name="interop-net"
        )
        self.assertIn("name: grpc-interop", yaml_str)
        self.assertIn("interop-server-cxx:", yaml_str)
        self.assertIn("condition: service_healthy", yaml_str)


class TestBugFixesAndIdleServerPrevention(unittest.TestCase):
    """Tests the pythonasyncio client dispatch fix and idle server filtering."""

    def test_pythonasyncio_client_dispatches_to_tests_aio(self):
        fake_setuptools = types.ModuleType("setuptools")
        fake_setuptools.Command = object
        fake_errors = types.ModuleType("setuptools.errors")
        fake_errors.OptionError = Exception
        fake_setuptools.errors = fake_errors
        fake_cmd_pkg = types.ModuleType("setuptools.command")
        for sub_name, attr_name in (
            ("build_ext", "build_ext"),
            ("build_py", "build_py"),
            ("easy_install", "easy_install"),
            ("install", "install"),
            ("test", "test"),
        ):
            mod = types.ModuleType("setuptools.command.%s" % sub_name)
            setattr(mod, attr_name, object)
            setattr(fake_cmd_pkg, sub_name, mod)

        called = {"aio": False, "sync": False}

        async def fake_aio_test_interoperability():
            called["aio"] = True
            called["argv"] = list(sys.argv[1:])

        fake_aio_client = types.ModuleType("tests_aio.interop.client")
        fake_aio_client.test_interoperability = fake_aio_test_interoperability
        fake_aio_pkg = types.ModuleType("tests_aio")
        fake_aio_interop = types.ModuleType("tests_aio.interop")
        fake_aio_interop.client = fake_aio_client

        fake_sync_client = types.ModuleType("tests.interop.client")
        fake_sync_client.parse_interop_client_args = lambda argv: argv
        fake_sync_client.test_interoperability = (
            lambda parsed: called.update({"sync": True})
        )
        fake_sync_pkg = types.ModuleType("tests")
        fake_sync_interop = types.ModuleType("tests.interop")
        fake_sync_interop.client = fake_sync_client

        with mock.patch.dict(
            sys.modules,
            {
                "setuptools": fake_setuptools,
                "setuptools.errors": fake_errors,
                "setuptools.command": fake_cmd_pkg,
                "setuptools.command.build_ext": fake_cmd_pkg.build_ext,
                "setuptools.command.build_py": fake_cmd_pkg.build_py,
                "setuptools.command.easy_install": fake_cmd_pkg.easy_install,
                "setuptools.command.install": fake_cmd_pkg.install,
                "setuptools.command.test": fake_cmd_pkg.test,
                "tests_aio": fake_aio_pkg,
                "tests_aio.interop": fake_aio_interop,
                "tests_aio.interop.client": fake_aio_client,
                "tests": fake_sync_pkg,
                "tests.interop": fake_sync_interop,
                "tests.interop.client": fake_sync_client,
            },
        ):
            if "commands" in sys.modules:
                del sys.modules["commands"]
            import commands

            cmd = commands.RunInterop()
            cmd.initialize_options()
            cmd.client = True
            cmd.use_asyncio = True
            cmd.args = "--server_host=localhost --server_port=8080 --test_case=large_unary"
            cmd.run_client()

            self.assertTrue(called["aio"])
            self.assertFalse(called["sync"])
            self.assertIn("--test_case=large_unary", called["argv"])

            # Also verify synchronous client path when use_asyncio=False
            cmd.use_asyncio = False
            cmd.run_client()
            self.assertTrue(called["sync"])

    def test_pythonasyncio_server_filtered_out_with_zero_runnable_tests(self):
        all_clients = [
            run_interop_tests._LANGUAGES[k]
            for k in run_interop_tests._LANGUAGES
            if k != "objc"
        ]
        active = run_interop_tests.filter_runnable_servers(
            set(run_interop_tests._SERVERS),
            all_clients,
            http2_interop=True,
        )
        self.assertNotIn("pythonasyncio", active)
        self.assertIn("c++", active)
        self.assertIn("go", active)
        self.assertIn("java", active)

    def test_go_server_filtered_out_when_only_http2_interop_and_no_clients(self):
        active = run_interop_tests.filter_runnable_servers(
            {"go", "c++"},
            client_languages=[],
            http2_interop=True,
        )
        self.assertEqual(active, {"c++"})

    def test_job_kill_handler_does_not_sleep_and_removes_container(self):
        fake_job = mock.MagicMock()
        fake_job._spec.container_name = "test_container_123"
        with mock.patch.object(
            dockerjob.subprocess, "call", return_value=0
        ) as mock_call, mock.patch("time.sleep") as mock_sleep:
            run_interop_tests._job_kill_handler(fake_job)
            mock_sleep.assert_not_called()
            calls = [c[0][0] for c in mock_call.call_args_list]
            self.assertIn(["docker", "kill", "test_container_123"], calls)
            self.assertIn(["docker", "rm", "-f", "test_container_123"], calls)


class TestInteropSpecialCasesAndDryRunCLI(unittest.TestCase):
    """Tests ORCA, ALTS, Auth, HTTP/2 bad server/client, and CLI --emit-bake / --emit-compose."""

    def test_orca_test_cases_inject_service_config_json(self):
        cxx = run_interop_tests._LANGUAGES["c++"]
        for orca_case in ("orca_per_rpc", "orca_oob"):
            job = run_interop_tests.cloud_to_cloud_jobspec(
                cxx,
                orca_case,
                "c++",
                "interop-server-cxx",
                8080,
                docker_image="grpc_interop_cxx:test",
                docker_network="interop-net",
            )
            joined = " ".join(job.cmdline)
            self.assertIn("test_backend_metrics_load_balancer", joined)

    def test_auth_options_env_vs_flag_and_cxx_gce_requirement(self):
        # Node/Python/Ruby/PHP7/AspNetCore use GOOGLE_APPLICATION_CREDENTIALS
        for lang_name in (
            "aspnetcore",
            "node",
            "nodepurejs",
            "php7",
            "python",
            "ruby",
        ):
            lang = run_interop_tests._LANGUAGES[lang_name]
            cmdargs, env = run_interop_tests.auth_options(
                lang,
                "jwt_token_creds",
                google_default_creds_use_key_file=False,
                service_account_key_file="/path/key.json",
                default_service_account="sa@example.com",
            )
            self.assertEqual(
                env.get("GOOGLE_APPLICATION_CREDENTIALS"), "/path/key.json"
            )
            self.assertNotIn("--service_account_key_file=/path/key.json", cmdargs)

        # C++ oauth2_auth_token requires GCE creds and passes --default_service_account
        cxx = run_interop_tests._LANGUAGES["c++"]
        self.assertTrue(
            run_interop_tests.compute_engine_creds_required(
                cxx, "oauth2_auth_token"
            )
        )
        cmdargs, env = run_interop_tests.auth_options(
            cxx,
            "oauth2_auth_token",
            google_default_creds_use_key_file=False,
            service_account_key_file="/path/key.json",
            default_service_account="sa@example.com",
        )
        self.assertIn("--service_account_key_file=/path/key.json", cmdargs)
        self.assertIn("--default_service_account=sa@example.com", cmdargs)
        self.assertIn(
            "--oauth_scope=https://www.googleapis.com/auth/xapi.zoo", cmdargs
        )

    def test_http2_server_interop_framing_cases_map_to_large_unary(self):
        java = run_interop_tests._LANGUAGES["java"]
        job = run_interop_tests.cloud_to_cloud_jobspec(
            java,
            "data_frame_padding",
            "http2",
            "interop-server-http2",
            8080,
            docker_image="grpc_interop_java:test",
            transport_security="insecure",
            docker_network="interop-net",
        )
        joined = " ".join(job.cmdline)
        self.assertIn("./run-test-client.sh", joined)
        self.assertIn("--test_case=large_unary", joined)
        self.assertIn("--use_tls=false", joined)
        self.assertEqual(
            job.shortname,
            "cloud_to_cloud:java:http2_server:data_frame_padding:insecure",
        )

    def test_dry_run_emits_bake_and_compose_and_manual_cmdlogs(self):
        temp_dir = tempfile.mkdtemp()
        try:
            bake_file = os.path.join(temp_dir, "docker-bake.hcl")
            compose_file = os.path.join(temp_dir, "docker-compose.yaml")
            rc = run_interop_tests.main(
                [
                    "-l",
                    "c++",
                    "java",
                    "javaokhttp",
                    "-s",
                    "c++",
                    "go",
                    "pythonasyncio",
                    "--use_docker",
                    "--http2_interop",
                    "--http2_server_interop",
                    "--emit-bake",
                    bake_file,
                    "--emit-compose",
                    compose_file,
                    "--dry-run",
                    "--travis",
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue(os.path.isfile(bake_file))
            self.assertTrue(os.path.isfile(compose_file))

            with open(bake_file, "r") as f:
                bake_content = f.read()
            # java and javaokhttp deduplicated to single "java" target;
            # pythonasyncio server filtered out!
            self.assertIn('targets = ["cxx", "go", "http2", "java"]', bake_content)
            self.assertNotIn("pythonasyncio", bake_content)

            with open(compose_file, "r") as f:
                compose_content = f.read()
            self.assertIn("interop-server-cxx:", compose_content)
            self.assertIn("interop-server-go:", compose_content)
            self.assertIn("interop-server-http2:", compose_content)
            self.assertNotIn("interop-server-pythonasyncio:", compose_content)
        finally:
            shutil.rmtree(temp_dir)

    def test_manual_run_generates_client_cmds_matching_matrix_format(self):
        temp_dir = tempfile.mkdtemp()
        try:
            res = jobset.JobResult()
            res.state = "PASSED"
            res.returncode = 0
            res.elapsed_time = 0.1
            res.message = ""
            with mock.patch.dict(
                os.environ, {"GRPC_TEST_REPORT_BASE_DIR": temp_dir}
            ), mock.patch.object(
                bake_manifest, "is_buildx_bake_available", return_value=True
            ), mock.patch.object(
                jobset,
                "run",
                side_effect=lambda jobs, **kwargs: (
                    0,
                    {j.shortname: [res] for j in jobs},
                ),
            ), mock.patch.object(
                dockerjob, "remove_image", return_value=True
            ):
                with self.assertRaises(SystemExit) as cm:
                    run_interop_tests.main(
                        [
                            "-l",
                            "c++",
                            "--use_docker",
                            "--cloud_to_prod",
                            "--prod_servers",
                            "default",
                            "gateway_v4",
                            "--manual_run",
                            "--custom_credentials_type",
                            "tls",
                            "--travis",
                        ]
                    )
                self.assertEqual(cm.exception.code, 0)
            cmds_file = os.path.join(REPO_ROOT, "interop_client_cmds.sh")
            self.assertTrue(os.path.isfile(cmds_file))
            with open(cmds_file, "r") as f:
                cmds_text = f.read()
            os.unlink(cmds_file)
            self.assertIn('echo "Testing ${docker_image:=grpc_interop_cxx:', cmds_text)
            self.assertIn(
                'docker run -i --rm=true -w /var/local/git/grpc --net=host $docker_image bash -c "cmake/build/interop_client --server_host=grpc-test.sandbox.googleapis.com --server_port=443 --test_case=large_unary --use_tls=true"',
                cmds_text,
            )
        finally:
            shutil.rmtree(temp_dir)

    def test_bake_build_fallback_to_legacy_on_bake_failure(self):
        temp_dir = tempfile.mkdtemp()
        try:
            ok_res = jobset.JobResult()
            ok_res.state = "PASSED"
            ok_res.returncode = 0
            ok_res.elapsed_time = 0.1
            ok_res.message = ""

            fail_res = jobset.JobResult()
            fail_res.state = "FAILED"
            fail_res.returncode = 1
            fail_res.elapsed_time = 0.1
            fail_res.message = "bake error"

            run_calls = []

            def fake_jobset_run(jobs, **kwargs):
                job_list = list(jobs)
                run_calls.append([j.shortname for j in job_list])
                if len(job_list) == 1 and job_list[0].shortname == "build_docker_bake":
                    return (1, {"build_docker_bake": [fail_res]})
                return (0, {j.shortname: [ok_res] for j in job_list})

            fake_docker_job = mock.MagicMock()
            fake_docker_job.is_running.return_value = True
            fake_docker_job.mapped_port.return_value = 32768

            with mock.patch.dict(
                os.environ, {"GRPC_TEST_REPORT_BASE_DIR": temp_dir}
            ), mock.patch.object(
                bake_manifest, "is_buildx_bake_available", return_value=True
            ), mock.patch.object(
                jobset, "run", side_effect=fake_jobset_run
            ), mock.patch.object(
                dockerjob, "DockerJob", return_value=fake_docker_job
            ), mock.patch.object(
                dockerjob, "create_network", return_value="grpc_interop_net_1"
            ), mock.patch.object(
                dockerjob, "remove_network", return_value=True
            ), mock.patch.object(
                dockerjob, "finish_jobs"
            ), mock.patch.object(
                dockerjob, "remove_image", return_value=True
            ):
                with self.assertRaises(SystemExit) as cm:
                    run_interop_tests.main(
                        [
                            "-l",
                            "java",
                            "javaokhttp",
                            "-s",
                            "c++",
                            "--use_docker",
                            "--travis",
                        ]
                    )
                self.assertEqual(cm.exception.code, 0)

            # First call was single `build_docker_bake`, second call fell back to
            # deduplicated `build_docker_c++` and `build_docker_java` ( only 2 images, not 3!)
            self.assertEqual(run_calls[0], ["build_docker_bake"])
            self.assertEqual(
                sorted(run_calls[1]), ["build_docker_c++", "build_docker_java"]
            )
        finally:
            shutil.rmtree(temp_dir)

    def test_aggregate_http2_results_parsing(self):
        sample_stdout = (
            'some log output\n{"cases": ['
            '{"name": "case1", "passed": true}, '
            '{"name": "case2", "passed": false}, '
            '{"name": "case3", "skipped": true}'
            "]}\ntrailing"
        )
        agg = run_interop_tests.aggregate_http2_results(sample_stdout)
        self.assertIsNotNone(agg)
        self.assertEqual(agg["passed"], 1)
        self.assertEqual(agg["failed"], 1)
        self.assertEqual(agg["skipped"], 1)
        self.assertEqual(agg["failed_cases"], "case2")
        self.assertAlmostEqual(agg["percent"], 0.5)


class TestMatrixScriptsAndReports(unittest.TestCase):
    """Tests create_matrix_images.py, run_interop_matrix_tests.py, and XML/BQ reporting."""

    def test_matrix_bake_manifest_and_jobspec_generation(self):
        hcl = create_matrix_images.generate_matrix_bake_for_release(
            "go",
            "v1.52.3",
            docker_path="us-docker.pkg.dev/grpc-testing/testing-images-public",
            grpc_root=REPO_ROOT,
        )
        self.assertIn('target "go1_19"', hcl)
        self.assertIn(
            "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.19:v1.52.3",
            hcl,
        )
        self.assertIn(
            "us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_go1.19:infrastructure-public-image-v1.52.3",
            hcl,
        )

    def test_run_interop_matrix_tests_jobspecs_and_xml_report(self):
        specs = run_interop_matrix_tests._generate_test_case_jobspecs(
            "cxx", "cxx", "v1.32.0", "cxx__cxx_v1.32.0"
        )
        self.assertTrue(len(specs) > 0)
        first = specs[0]
        self.assertTrue(first.shortname.startswith("cxx__cxx_v1.32.0:cxx:"))
        self.assertIn("--server_host=74.125.206.210", first.cmdline)
        self.assertIn("--server_host_override=", first.cmdline)

        # Verify JUnit XML report respects GRPC_TEST_REPORT_BASE_DIR
        temp_dir = tempfile.mkdtemp()
        try:
            res = jobset.JobResult()
            res.state = "PASSED"
            res.returncode = 0
            res.elapsed_time = 0.5
            res.message = "ok"
            with mock.patch.dict(
                os.environ, {"GRPC_TEST_REPORT_BASE_DIR": temp_dir}
            ):
                report_utils.render_junit_xml_report(
                    {first.shortname: [res]},
                    run_interop_tests._TESTS_XML_REPORT,
                )
            xml_path = os.path.join(
                temp_dir, run_interop_tests._TESTS_XML_REPORT
            )
            self.assertTrue(os.path.isfile(xml_path))
            tree = ET.parse(xml_path)
            testcases = tree.getroot().findall(".//testcase")
            self.assertEqual(len(testcases), 1)
            self.assertEqual(testcases[0].attrib["name"], first.shortname)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    import setuptools

    unittest.main(verbosity=2)
