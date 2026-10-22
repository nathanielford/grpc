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
"""Self-contained gRPC interop and backwards-compatibility matrix test harness.

All language definitions, commands, environment variables, test suites, skip
matrices, and historical release matrix configurations are loaded from
`tools/interop/interop.toml` using Python's standard library `tomllib`.
"""

from __future__ import annotations

import argparse
import atexit
from collections import OrderedDict
import itertools
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import uuid

_THIS_DIR = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
_RUN_TESTS_DIR = os.path.join(ROOT, "tools/run_tests")
_PYTHON_UTILS_DIR = os.path.join(_RUN_TESTS_DIR, "python_utils")

for _p in (_RUN_TESTS_DIR, _PYTHON_UTILS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import python_utils.dockerjob as dockerjob
import python_utils.jobset as jobset
import python_utils.report_utils as report_utils

try:
    import python_utils.upload_test_results as upload_test_results
    from python_utils.upload_test_results import upload_interop_results_to_bq
except ImportError as _import_err:
    upload_test_results = None
    upload_interop_results_to_bq = None


_TOML_PATH = os.path.join(_THIS_DIR, "interop.toml")


def load_config(path: str = _TOML_PATH) -> Dict[str, Any]:
    """Loads the static TOML configuration file using stdlib tomllib."""
    with open(path, "rb") as f:
        return tomllib.load(f)


CONFIG: Dict[str, Any] = load_config()
_DEFAULTS: Dict[str, Any] = CONFIG["defaults"]
_SUITES: Dict[str, Any] = CONFIG["suites"]
_TEST_CASES_META: Dict[str, Any] = CONFIG.get("test_cases", {})
_TRANSPORT_SECURITY_CFG: Dict[str, Any] = CONFIG["transport_security"]
_CUSTOM_CREDS_CFG: Dict[str, Any] = CONFIG["custom_credentials_types"]
_MATRIX_CFG: Dict[str, Any] = CONFIG["matrix"]
_MATRIX_DEFAULTS: Dict[str, Any] = _MATRIX_CFG["defaults"]

_DEFAULT_SERVER_PORT: int = int(_DEFAULTS["default_server_port"])
_TEST_TIMEOUT: int = int(_DEFAULTS["test_timeout_seconds"])
_DOCKER_BUILD_XML_REPORT: str = str(_DEFAULTS["docker_build_xml_report"])
_TESTS_XML_REPORT: str = str(_DEFAULTS["tests_xml_report"])
DOCKER_WORKDIR_ROOT: str = str(_DEFAULTS["docker_workdir_root"])
_IMAGE_BUILDER: str = str(_DEFAULTS["build_script"])
_BUILD_INFO: str = str(_MATRIX_DEFAULTS["build_info_dir"])
_TEST_TIMEOUT_SECONDS: int = int(_MATRIX_DEFAULTS["test_timeout_seconds"])
_PULL_IMAGE_TIMEOUT_SECONDS: int = int(
    _MATRIX_DEFAULTS["pull_image_timeout_seconds"]
)
_MAX_PARALLEL_DOWNLOADS: int = int(_MATRIX_DEFAULTS["max_parallel_downloads"])

prod_servers: Dict[str, str] = dict(CONFIG["prod_servers"])

_TEST_CASES: List[str] = list(_SUITES["test_cases"])
_CLOUD_TO_PROD_TEST_CASES: List[str] = list(_SUITES["cloud_to_prod_test_cases"])
_AUTH_TEST_CASES: List[str] = list(_SUITES["auth_test_cases"])
_HTTP2_TEST_CASES: List[str] = list(_SUITES["http2_test_cases"])
_HTTP2_SERVER_TEST_CASES: List[str] = list(_SUITES["http2_server_test_cases"])
_ORCA_TEST_CASES: List[str] = list(_SUITES["orca_test_cases"])
_GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES: Dict[str, str] = dict(
    _SUITES["http2_server_grpc_client_map"]
)
_HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS: List[str] = list(
    _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES.keys()
)
_TRANSPORT_SECURITY_OPTIONS: List[str] = list(
    _SUITES["transport_security_options"]
)
_CUSTOM_CREDENTIALS_TYPE_OPTIONS: List[str] = list(
    _SUITES["custom_credentials_type_options"]
)

_GOOGLE_DEFAULT_CREDS_TEST_CASE: str = "google_default_credentials"
_COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE: str = (
    "compute_engine_channel_credentials"
)


class InteropLanguage:
    """Data-driven language / endpoint descriptor configured from interop.toml."""

    _DEFAULT_NAME: str = ""
    _DEFAULT_CFG: Dict[str, Any] = {}

    def __init__(
        self,
        name: Optional[str] = None,
        cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        resolved_name = name if name is not None else self._DEFAULT_NAME
        resolved_cfg = (
            cfg
            if cfg is not None
            else (
                self._DEFAULT_CFG
                or CONFIG["languages"].get(resolved_name, {})
            )
        )
        self._name: str = str(resolved_cfg.get("name", resolved_name))
        self.safename: str = str(resolved_cfg.get("safename", self._name))
        self.client_cwd: Optional[str] = resolved_cfg.get("client_cwd")
        self.server_cwd: Optional[str] = resolved_cfg.get("server_cwd")
        self.http2_cwd: Optional[str] = resolved_cfg.get("http2_cwd")
        self._client_cmd_prefix: List[str] = list(
            resolved_cfg.get("client_cmd", [])
        )
        self._server_cmd_prefix: List[str] = list(
            resolved_cfg.get("server_cmd", [])
        )
        self._http2_cmd_prefix: List[str] = list(
            resolved_cfg.get("http2_client_cmd", [])
        )
        self._args_mode: str = str(resolved_cfg.get("args_mode", "list"))
        self._xcode_cmd_template: str = str(
            resolved_cfg.get("xcode_cmd_template", "")
        )
        self._global_env: Dict[str, str] = dict(
            resolved_cfg.get("global_env", {})
        )
        self._cloud_to_prod_env: Dict[str, str] = dict(
            resolved_cfg.get("cloud_to_prod_env", {})
        )
        self._unimplemented_client_cases: List[str] = list(
            resolved_cfg.get("unimplemented_client_test_cases", [])
        )
        self._unimplemented_server_cases: List[str] = list(
            resolved_cfg.get("unimplemented_server_test_cases", [])
        )
        self.client_test_cases: List[str] = list(
            resolved_cfg.get("client_test_cases", [])
        )
        self.server_test_cases: List[str] = list(
            resolved_cfg.get("server_test_cases", [])
        )
        self.runs_in_docker: bool = bool(
            resolved_cfg.get("runs_in_docker", True)
        )
        self.included_in_all: bool = bool(
            resolved_cfg.get("included_in_all", True)
        )
        self.is_server: bool = bool(resolved_cfg.get("is_server", False))
        self.server_choosable: bool = bool(
            resolved_cfg.get("server_choosable", self.is_server)
        )
        self.supports_alts: bool = bool(
            resolved_cfg.get("supports_alts", False)
        )
        self.supports_http2_server_interop: bool = bool(
            resolved_cfg.get("supports_http2_server_interop", False)
        )
        self.supports_http2_badclient_interop: bool = bool(
            resolved_cfg.get("supports_http2_badclient_interop", True)
        )
        self.auth_creds_mode: str = str(
            resolved_cfg.get("auth_creds_mode", "flag")
        )
        self.oauth2_uses_gce_creds: bool = bool(
            resolved_cfg.get("oauth2_uses_gce_creds", False)
        )
        self.supported_custom_credentials_types: List[str] = list(
            resolved_cfg.get("supported_custom_credentials_types", ["tls"])
        )
        self.server_port_count: int = int(
            resolved_cfg.get("server_port_count", 1)
        )
        self.server_health_cmd: str = str(
            resolved_cfg.get("server_health_cmd", "")
        )
        self.server_health_interval: str = str(
            resolved_cfg.get("server_health_interval", "1s")
        )
        self.server_health_retries: str = str(
            resolved_cfg.get("server_health_retries", "5")
        )
        self.server_health_timeout: str = str(
            resolved_cfg.get("server_health_timeout", "10s")
        )

    def _format_cmd(
        self, prefix: List[str], cmd_args: Sequence[str]
    ) -> Optional[List[str]]:
        if self._args_mode == "objc_xcode":
            for arg in cmd_args:
                port = re.search(r"--server_port=(\d+)", str(arg))
                if port:
                    return [self._xcode_cmd_template % port.group(1)]
            return None
        if self._args_mode == "ignore":
            return list(prefix)
        if self._args_mode == "setup_py_quoted":
            return list(prefix) + ['--args="{}"'.format(" ".join(cmd_args))]
        return list(prefix) + list(cmd_args)

    def client_cmd(self, cmd_args: Sequence[str]) -> Optional[List[str]]:
        return self._format_cmd(self._client_cmd_prefix, cmd_args)

    def server_cmd(self, cmd_args: Sequence[str]) -> List[str]:
        res = self._format_cmd(self._server_cmd_prefix, cmd_args)
        return res if res is not None else list(self._server_cmd_prefix)

    def client_cmd_http2interop(self, cmd_args: Sequence[str]) -> List[str]:
        return list(self._http2_cmd_prefix) + list(cmd_args)

    def cloud_to_prod_env(self) -> Dict[str, str]:
        return dict(self._cloud_to_prod_env)

    def global_env(self) -> Dict[str, str]:
        return dict(self._global_env)

    def unimplemented_test_cases(self) -> List[str]:
        return list(self._unimplemented_client_cases)

    def unimplemented_test_cases_server(self) -> List[str]:
        return list(self._unimplemented_server_cases)

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._name}>"


def _register_languages() -> Tuple[
    Dict[str, InteropLanguage],
    List[str],
    List[str],
    List[str],
    List[str],
    List[str],
]:
    languages_map: Dict[str, InteropLanguage] = {}
    servers_list: List[str] = []
    choosable_servers: List[str] = []
    http2_clients: List[str] = []
    alts_clients: List[str] = []
    alts_servers: List[str] = []

    for lang_key, lang_cfg in CONFIG["languages"].items():
        cls_name = str(lang_cfg.get("class_name", "InteropLanguage"))
        lang_cls = type(
            cls_name,
            (InteropLanguage,),
            {"_DEFAULT_NAME": lang_key, "_DEFAULT_CFG": lang_cfg},
        )
        globals()[cls_name] = lang_cls
        for alias in lang_cfg.get("class_aliases", []):
            globals()[str(alias)] = lang_cls

        instance = lang_cls(lang_key, lang_cfg)
        if lang_cfg.get("included_in_languages", True):
            languages_map[lang_key] = instance
        if instance.is_server:
            servers_list.append(lang_key)
        if instance.server_choosable:
            choosable_servers.append(lang_key)
        if instance.supports_http2_server_interop:
            http2_clients.append(lang_key)
        if instance.supports_alts:
            alts_clients.append(lang_key)
            if instance.is_server:
                alts_servers.append(lang_key)

    for ep_cfg in CONFIG.get("http2_endpoints", {}).values():
        ep_name = str(ep_cfg["name"])
        ep_cls_name = str(ep_cfg["class_name"])
        ep_cls = type(
            ep_cls_name,
            (InteropLanguage,),
            {"_DEFAULT_NAME": ep_name, "_DEFAULT_CFG": ep_cfg},
        )
        globals()[ep_cls_name] = ep_cls

    return (
        languages_map,
        servers_list,
        choosable_servers,
        http2_clients,
        alts_clients,
        alts_servers,
    )


(
    _LANGUAGES,
    _SERVERS,
    _CHOOSABLE_SERVERS,
    _LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES,
    _LANGUAGES_FOR_ALTS_TEST_CASES,
    _SERVERS_FOR_ALTS_TEST_CASES,
) = _register_languages()


def _resolve_language(language: Any) -> InteropLanguage:
    if isinstance(language, InteropLanguage):
        return language
    key = str(language)
    if key in _LANGUAGES:
        return _LANGUAGES[key]
    for ep_cfg in CONFIG.get("http2_endpoints", {}).values():
        if ep_cfg.get("name") == key or ep_cfg.get("safename") == key:
            return InteropLanguage(key, ep_cfg)
    raise KeyError(f"Unknown interop language: {language}")


def _server_has_implemented_test_cases(
    server_lang_name: str, http2_interop: bool = False
) -> bool:
    """Returns True if the server language implements at least one active test case."""
    lang = _LANGUAGES.get(str(server_lang_name))
    if not lang:
        return True
    unimplemented = set(lang.unimplemented_test_cases_server())
    if any(tc not in unimplemented for tc in _TEST_CASES):
        return True
    if http2_interop and any(
        tc not in unimplemented for tc in _HTTP2_TEST_CASES
    ):
        return True
    return False


def docker_run_cmdline(
    cmdline: List[str],
    image: str,
    docker_args: Sequence[str] = (),
    cwd: Optional[str] = None,
    environ: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Wraps given cmdline array to create 'docker run' cmdline from it."""
    docker_cmdline = ["docker", "run", "-i", "--rm=true"]
    if environ:
        for k, v in list(environ.items()):
            docker_cmdline += ["-e", "%s=%s" % (k, v)]
    workdir = DOCKER_WORKDIR_ROOT
    if cwd:
        workdir = os.path.join(workdir, cwd)
    docker_cmdline += ["-w", workdir]
    docker_cmdline += list(docker_args) + [image] + list(cmdline)
    return docker_cmdline


def manual_cmdline(docker_cmdline: Sequence[str], docker_image: str) -> str:
    """Returns docker cmdline adjusted for manual invocation."""
    print_cmdline = []
    for item in docker_cmdline:
        if item.startswith("--name="):
            continue
        if item == docker_image:
            item = "$docker_image"
        item = item.replace('"', '\\"')
        if any(character.isspace() for character in item):
            item = '"%s"' % item
        print_cmdline.append(item)
    return " ".join(print_cmdline)


def write_cmdlog_maybe(
    cmdlog: Optional[Sequence[str]], filename: str
) -> None:
    """Writes manual command log file if non-empty."""
    if cmdlog:
        with open(filename, "w") as logfile:
            logfile.write("#!/bin/bash\n")
            logfile.write("# DO NOT MODIFY\n")
            logfile.write(
                "# This file is generated by"
                " run_interop_tests.py/create_testcases.sh\n"
            )
            logfile.writelines("%s\n" % line for line in cmdlog)
        print("Command log written to file %s" % filename)


def bash_cmdline(cmdline: Sequence[str]) -> List[str]:
    """Creates bash -c cmdline from args list."""
    return ["bash", "-c", " ".join(cmdline)]


def compute_engine_creds_required(language: Any, test_case: str) -> bool:
    """Returns True if given test requires access to compute engine creds."""
    lang_obj = _LANGUAGES.get(str(language))
    tc_meta = _TEST_CASES_META.get(test_case, {})
    if tc_meta.get("requires_compute_engine_creds", False):
        return True
    if (
        test_case == "oauth2_auth_token"
        and lang_obj is not None
        and lang_obj.oauth2_uses_gce_creds
    ):
        return True
    return False


def auth_options(
    language: Any,
    test_case: str,
    google_default_creds_use_key_file: bool = False,
    service_account_key_file: Optional[str] = None,
    default_service_account: Optional[str] = None,
) -> Tuple[List[str], Dict[str, str]]:
    """Returns (cmdline, env) tuple with cloud_to_prod_auth test options."""
    lang_obj = _LANGUAGES.get(str(language))
    if service_account_key_file is None:
        service_account_key_file = str(
            _DEFAULTS["default_service_account_key_file"]
        )
    if default_service_account is None:
        default_service_account = str(_DEFAULTS["default_service_account"])

    cmdargs: List[str] = []
    env: Dict[str, str] = {}

    oauth_scope_arg = (
        f"--oauth_scope={_DEFAULTS['default_oauth_scope']}"
    )
    key_file_arg = f"--service_account_key_file={service_account_key_file}"
    default_account_arg = (
        f"--default_service_account={default_service_account}"
    )

    tc_meta = _TEST_CASES_META.get(test_case, {})
    if tc_meta.get("uses_key_file", False):
        if lang_obj is not None and lang_obj.auth_creds_mode == "env":
            env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
        else:
            cmdargs.append(key_file_arg)

    if tc_meta.get("uses_oauth_scope", False):
        cmdargs.append(oauth_scope_arg)

    if (
        test_case == "oauth2_auth_token"
        and lang_obj is not None
        and lang_obj.oauth2_uses_gce_creds
    ):
        cmdargs.append(default_account_arg)

    if (
        tc_meta.get("uses_key_file_when_flag_enabled", False)
        and google_default_creds_use_key_file
    ):
        env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file

    if tc_meta.get("uses_default_service_account", False):
        cmdargs.append(default_account_arg)

    return (cmdargs, env)


def _job_kill_handler(job: Any) -> None:
    if getattr(job._spec, "container_name", None):
        dockerjob.docker_kill(job._spec.container_name)


def cloud_to_prod_jobspec(
    language: Any,
    test_case: str,
    server_host_nickname: str,
    server_host: str,
    google_default_creds_use_key_file: bool = False,
    docker_image: Optional[str] = None,
    auth: bool = False,
    manual_cmd_log: Optional[List[str]] = None,
    service_account_key_file: Optional[str] = None,
    default_service_account: Optional[str] = None,
    transport_security: str = "tls",
) -> jobset.JobSpec:
    """Creates jobspec for cloud-to-prod interop test."""
    lang_obj = (
        language
        if isinstance(language, InteropLanguage)
        else _resolve_language(language)
    )
    container_name = None
    cmdargs = [
        "--server_host=%s" % server_host,
        "--server_port=%s" % _DEFAULTS["prod_port"],
        "--test_case=%s" % test_case,
    ]
    supported_creds = getattr(
        lang_obj, "supported_custom_credentials_types", ["tls"]
    )
    if (
        transport_security in _CUSTOM_CREDS_CFG
        and transport_security in supported_creds
    ):
        transport_security_options = list(
            _CUSTOM_CREDS_CFG[transport_security]["flags"]
        )
    else:
        print(
            "Invalid transport security option %s in cloud_to_prod_jobspec."
            " Lang: %s" % (str(language), transport_security)
        )
        sys.exit(1)

    cmdargs = cmdargs + transport_security_options
    environ = dict(lang_obj.cloud_to_prod_env(), **lang_obj.global_env())
    if auth:
        auth_cmdargs, auth_env = auth_options(
            lang_obj,
            test_case,
            google_default_creds_use_key_file,
            service_account_key_file,
            default_service_account,
        )
        cmdargs += auth_cmdargs
        environ.update(auth_env)

    client_cmdline = lang_obj.client_cmd(cmdargs)
    cmdline = bash_cmdline(client_cmdline or [])
    cwd = lang_obj.client_cwd

    if docker_image:
        container_name = dockerjob.random_name(
            "interop_client_%s" % lang_obj.safename
        )
        cmdline = docker_run_cmdline(
            cmdline,
            image=docker_image,
            cwd=cwd,
            environ=environ,
            docker_args=["--net=host", "--name=%s" % container_name],
        )
        if manual_cmd_log is not None:
            if manual_cmd_log == []:
                manual_cmd_log.append(
                    'echo "Testing ${docker_image:=%s}"' % docker_image
                )
            manual_cmd_log.append(manual_cmdline(cmdline, docker_image))
        cwd = None
        environ = None

    suite_name = "cloud_to_prod_auth" if auth else "cloud_to_prod"
    allow_flakes = bool(args and getattr(args, "allow_flakes", False))
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="%s:%s:%s:%s:%s"
        % (
            suite_name,
            lang_obj,
            server_host_nickname,
            test_case,
            transport_security,
        ),
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=4 if allow_flakes else 0,
        timeout_retries=2 if allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
        test_job.container_name = container_name
    return test_job


def cloud_to_cloud_jobspec(
    language: Any,
    test_case: str,
    server_name: str,
    server_host: str,
    server_port: Any,
    docker_image: Optional[str] = None,
    transport_security: str = "tls",
    manual_cmd_log: Optional[List[str]] = None,
) -> jobset.JobSpec:
    """Creates jobspec for cloud-to-cloud interop test."""
    lang_obj = (
        language
        if isinstance(language, InteropLanguage)
        else _resolve_language(language)
    )
    interop_only_options = [
        f"--server_host_override={_DEFAULTS['server_host_override']}",
        "--use_test_ca=true",
    ]
    if transport_security in _TRANSPORT_SECURITY_CFG:
        interop_only_options += list(
            _TRANSPORT_SECURITY_CFG[transport_security]["flags"]
        )
    else:
        print(
            "Invalid transport security option %s in cloud_to_cloud_jobspec."
            % transport_security
        )
        sys.exit(1)

    client_test_case = test_case
    if test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
        client_test_case = _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES[
            test_case
        ]
    if client_test_case in lang_obj.unimplemented_test_cases():
        print(
            "asking client %s to run unimplemented test case %s"
            % (repr(lang_obj), client_test_case)
        )
        sys.exit(1)

    if test_case in _ORCA_TEST_CASES:
        interop_only_options += [
            f"--service_config_json='{_DEFAULTS['orca_service_config_json']}'"
        ]

    common_options = [
        "--test_case=%s" % client_test_case,
        "--server_host=%s" % server_host,
        "--server_port=%s" % server_port,
    ]

    if test_case in _HTTP2_SERVER_TEST_CASES:
        if test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
            client_options = interop_only_options + common_options
            cmdline = bash_cmdline(lang_obj.client_cmd(client_options) or [])
            cwd = lang_obj.client_cwd
        else:
            cmdline = bash_cmdline(
                lang_obj.client_cmd_http2interop(common_options)
            )
            cwd = lang_obj.http2_cwd
    else:
        cmdline = bash_cmdline(
            lang_obj.client_cmd(common_options + interop_only_options) or []
        )
        cwd = lang_obj.client_cwd

    environ = lang_obj.global_env()
    container_name = None
    runs_in_docker = getattr(lang_obj, "runs_in_docker", True)
    if docker_image and runs_in_docker:
        container_name = dockerjob.random_name(
            "interop_client_%s" % lang_obj.safename
        )
        cmdline = docker_run_cmdline(
            cmdline,
            image=docker_image,
            environ=environ,
            cwd=cwd,
            docker_args=["--net=host", "--name=%s" % container_name],
        )
        if manual_cmd_log is not None:
            if manual_cmd_log == []:
                manual_cmd_log.append(
                    'echo "Testing ${docker_image:=%s}"' % docker_image
                )
            manual_cmd_log.append(manual_cmdline(cmdline, docker_image))
        cwd = None

    allow_flakes = bool(args and getattr(args, "allow_flakes", False))
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="cloud_to_cloud:%s:%s_server:%s:%s"
        % (lang_obj, server_name, test_case, transport_security),
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=4 if allow_flakes else 0,
        timeout_retries=2 if allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
        test_job.container_name = container_name
    return test_job


def server_jobspec(
    language: Any,
    docker_image: Optional[str],
    transport_security: str = "tls",
    manual_cmd_log: Optional[List[str]] = None,
) -> jobset.JobSpec:
    """Create jobspec for running a server."""
    lang_obj = (
        language
        if isinstance(language, InteropLanguage)
        else _resolve_language(language)
    )
    container_name = dockerjob.random_name(
        "interop_server_%s" % lang_obj.safename
    )
    server_cmd = ["--port=%s" % _DEFAULT_SERVER_PORT]
    if transport_security in _TRANSPORT_SECURITY_CFG:
        server_cmd += list(_TRANSPORT_SECURITY_CFG[transport_security]["flags"])
    else:
        print(
            "Invalid transport security option %s in server_jobspec."
            % transport_security
        )
        sys.exit(1)

    cmdline = bash_cmdline(lang_obj.server_cmd(server_cmd))
    environ = lang_obj.global_env()
    docker_args = ["--name=%s" % container_name]
    port_count = getattr(lang_obj, "server_port_count", 1)
    if port_count > 1:
        docker_args += list(
            itertools.chain.from_iterable(
                ("-p", str(_DEFAULT_SERVER_PORT + i))
                for i in range(len(_HTTP2_SERVER_TEST_CASES))
            )
        )
    else:
        docker_args += ["-p", str(_DEFAULT_SERVER_PORT)]

    health_cmd = getattr(lang_obj, "server_health_cmd", "")
    if health_cmd:
        docker_args += [
            f"--health-cmd={health_cmd}",
            f"--health-interval={getattr(lang_obj, 'server_health_interval', '1s')}",
            f"--health-retries={getattr(lang_obj, 'server_health_retries', '5')}",
            f"--health-timeout={getattr(lang_obj, 'server_health_timeout', '10s')}",
        ]

    docker_cmdline = docker_run_cmdline(
        cmdline,
        image=docker_image or "",
        cwd=lang_obj.server_cwd,
        environ=environ,
        docker_args=docker_args,
    )
    if manual_cmd_log is not None:
        if manual_cmd_log == []:
            manual_cmd_log.append(
                'echo "Testing ${docker_image:=%s}"' % docker_image
            )
        manual_cmd_log.append(manual_cmdline(docker_cmdline, docker_image or ""))

    server_job = jobset.JobSpec(
        cmdline=docker_cmdline,
        environ=environ,
        shortname="interop_server_%s" % lang_obj,
        timeout_seconds=int(_DEFAULTS["server_timeout_seconds"]),
    )
    server_job.container_name = container_name
    return server_job


def build_interop_image_jobspec(
    language: Any, tag: Optional[str] = None
) -> jobset.JobSpec:
    """Creates jobspec for building interop docker image for a language."""
    lang_obj = (
        language
        if isinstance(language, InteropLanguage)
        else _resolve_language(language)
    )
    if not tag:
        tag = "grpc_interop_%s:%s" % (lang_obj.safename, uuid.uuid4())
    env = {
        "INTEROP_IMAGE": tag,
        "BASE_NAME": "grpc_interop_%s" % lang_obj.safename,
    }
    build_job = jobset.JobSpec(
        cmdline=[_IMAGE_BUILDER],
        environ=env,
        shortname="build_docker_%s" % lang_obj,
        timeout_seconds=int(_DEFAULTS["image_build_timeout_seconds"]),
    )
    build_job.tag = tag
    return build_job


def compute_docker_build_jobs(
    languages: Iterable[Any],
    servers: Iterable[Any],
    http2_interop: bool = False,
    http2_server_interop: bool = False,
    http2_server_clients: Optional[Iterable[Any]] = None,
) -> Tuple[List[jobset.JobSpec], Dict[str, str]]:
    """Computes deduplicated Docker build JobSpecs keyed by safename."""
    languages_to_build: List[InteropLanguage] = []
    seen_names = set()

    def _add_candidate(item: Any) -> None:
        obj = (
            item
            if isinstance(item, InteropLanguage)
            else _resolve_language(item)
        )
        key = str(obj)
        if key not in seen_names:
            seen_names.add(key)
            languages_to_build.append(obj)

    for l in languages:
        _add_candidate(l)
    for s in servers:
        _add_candidate(s)
    if http2_server_clients:
        for c in http2_server_clients:
            _add_candidate(c)
    if http2_interop:
        _add_candidate(globals()["Http2Client"]())
    if http2_server_interop:
        _add_candidate(globals()["Http2Server"]())

    build_jobs: List[jobset.JobSpec] = []
    docker_images: Dict[str, str] = {}
    tag_by_safename: Dict[str, str] = {}

    for lang_obj in sorted(languages_to_build, key=lambda x: str(x)):
        if not getattr(lang_obj, "runs_in_docker", True):
            continue
        if lang_obj.safename not in tag_by_safename:
            job = build_interop_image_jobspec(lang_obj)
            tag_by_safename[lang_obj.safename] = job.tag
            build_jobs.append(job)
        docker_images[str(lang_obj)] = tag_by_safename[lang_obj.safename]

    return build_jobs, docker_images


def _parse_override_server(kv: str) -> List[str]:
    if "=" not in kv:
        raise argparse.ArgumentTypeError(
            f"Invalid --override_server value '{kv}': expected servername=HOST:PORT"
        )
    name, addr = kv.split("=", 1)
    if ":" not in addr or not name or not addr:
        raise argparse.ArgumentTypeError(
            f"Invalid --override_server value '{kv}': expected servername=HOST:PORT"
        )
    return [name, addr]


def build_interop_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run interop tests.")
    parser.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(_LANGUAGES),
        nargs="+",
        default=["all"],
        help="Clients to run.",
    )
    parser.add_argument(
        "-j", "--jobs", default=multiprocessing.cpu_count(), type=int
    )
    parser.add_argument(
        "--cloud_to_prod",
        default=False,
        action="store_const",
        const=True,
        help="Run cloud_to_prod tests.",
    )
    parser.add_argument(
        "--cloud_to_prod_auth",
        default=False,
        action="store_const",
        const=True,
        help="Run cloud_to_prod_auth tests.",
    )
    parser.add_argument(
        "--google_default_creds_use_key_file",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Whether or not we should use a key file for the "
            "google_default_credentials test case, e.g. by "
            "setting env var GOOGLE_APPLICATION_CREDENTIALS."
        ),
    )
    parser.add_argument(
        "--prod_servers",
        choices=list(prod_servers.keys()),
        default=["default"],
        nargs="+",
        help=(
            "The servers to run cloud_to_prod and cloud_to_prod_auth tests"
            " against."
        ),
    )
    parser.add_argument(
        "-s",
        "--server",
        choices=["all"] + sorted(_CHOOSABLE_SERVERS),
        nargs="+",
        help=(
            "Run cloud_to_cloud servers in a separate docker image. Servers can"
            " only be started automatically if --use_docker option is enabled."
        ),
        default=[],
    )
    parser.add_argument(
        "--override_server",
        action="append",
        type=_parse_override_server,
        help="Use servername=HOST:PORT to explicitly specify a server.",
        default=[],
    )
    parser.add_argument(
        "--service_account_key_file",
        type=str,
        help="The service account key file to use for some auth interop tests.",
        default=str(_DEFAULTS["default_service_account_key_file"]),
    )
    parser.add_argument(
        "--default_service_account",
        type=str,
        help=(
            "Default GCE service account email to use for some auth interop"
            " tests."
        ),
        default=str(_DEFAULTS["default_service_account"]),
    )
    parser.add_argument(
        "-t",
        "--travis",
        default=False,
        action="store_const",
        const=True,
        help="When set, indicates that the script is running on CI.",
    )
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_const", const=True
    )
    parser.add_argument(
        "--use_docker",
        default=False,
        action="store_const",
        const=True,
        help="Run all the interop tests under docker.",
    )
    parser.add_argument(
        "--allow_flakes",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Allow flaky tests to show as passing (re-runs failed tests up to"
            " five times)"
        ),
    )
    parser.add_argument(
        "--manual_run",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Prepare things for running interop tests manually. Preserve docker"
            " images after building them and skip actually running the tests."
        ),
    )
    parser.add_argument(
        "--http2_interop",
        default=False,
        action="store_const",
        const=True,
        help="Enable HTTP/2 client edge case testing. (Bad client, good server)",
    )
    parser.add_argument(
        "--http2_server_interop",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Enable HTTP/2 server edge case testing. (Includes positive and"
            " negative tests"
        ),
    )
    parser.add_argument(
        "--transport_security",
        choices=_TRANSPORT_SECURITY_OPTIONS,
        default="tls",
        type=str,
        nargs="?",
        const=True,
        help="Which transport security mechanism to use.",
    )
    parser.add_argument(
        "--custom_credentials_type",
        choices=_CUSTOM_CREDENTIALS_TYPE_OPTIONS,
        default=_CUSTOM_CREDENTIALS_TYPE_OPTIONS,
        nargs="+",
        help=(
            "Credential types to test in the cloud_to_prod setup. Default is to"
            " test with all creds types possible."
        ),
    )
    parser.add_argument(
        "--skip_compute_engine_creds",
        default=False,
        action="store_const",
        const=True,
        help="Skip auth tests requiring access to compute engine credentials.",
    )
    parser.add_argument(
        "--bq_result_table",
        default="",
        type=str,
        nargs="?",
        help="Upload test results to a specified BQ table.",
    )
    return parser


argp = build_interop_arg_parser()
args = argp.parse_args([])


def parse_interop_args(
    argv: Optional[Sequence[str]] = None,
) -> argparse.Namespace:
    global args
    args = argp.parse_args(argv)
    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for tools/run_tests/run_interop_tests.py."""
    global args
    args = argp.parse_args(argv)
    os.chdir(ROOT)

    raw_servers = set(
        s
        for s in itertools.chain.from_iterable(
            _SERVERS if x == "all" else [x] for x in args.server
        )
    )
    if args.transport_security == "alts":
        raw_servers = raw_servers.intersection(_SERVERS_FOR_ALTS_TEST_CASES)

    servers = {
        s
        for s in raw_servers
        if _server_has_implemented_test_cases(
            s, http2_interop=bool(args.http2_interop)
        )
    }

    if args.use_docker:
        if not args.travis:
            print("Seen --use_docker flag, will run interop tests under docker.")
            print("")
            print(
                "IMPORTANT: The changes you are testing need to be locally"
                " committed"
            )
            print(
                "because only the committed changes in the current branch will"
                " be"
            )
            print("copied to the docker environment.")
            time.sleep(5)

    if args.manual_run and not args.use_docker:
        print(
            "--manual_run is only supported with --use_docker option enabled."
        )
        sys.exit(1)

    if not args.use_docker and raw_servers:
        print(
            "Running interop servers is only supported with --use_docker option"
            " enabled."
        )
        sys.exit(1)

    all_languages = {
        k for k, v in _LANGUAGES.items() if getattr(v, "included_in_all", True)
    }
    languages = set(
        _LANGUAGES[l]
        for l in itertools.chain.from_iterable(
            all_languages if x == "all" else [x] for x in args.language
        )
    )
    if args.transport_security == "alts":
        alts_languages = set(
            _LANGUAGES[l] for l in _LANGUAGES_FOR_ALTS_TEST_CASES
        )
        languages = languages.intersection(alts_languages)

    languages_http2_clients_for_http2_server_interop = set()
    if args.http2_server_interop:
        languages_http2_clients_for_http2_server_interop = set(
            _LANGUAGES[l]
            for l in _LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES
            if "all" in args.language or l in args.language
        )

    http2Interop = globals()["Http2Client"]() if args.http2_interop else None
    http2InteropServer = (
        globals()["Http2Server"]() if args.http2_server_interop else None
    )

    docker_images: Dict[str, str] = {}
    if args.use_docker:
        build_jobs, docker_images = compute_docker_build_jobs(
            languages=languages,
            servers=servers,
            http2_interop=bool(args.http2_interop),
            http2_server_interop=bool(args.http2_server_interop),
            http2_server_clients=languages_http2_clients_for_http2_server_interop,
        )

        if build_jobs:
            jobset.message(
                "START", "Building interop docker images.", do_newline=True
            )
            if args.verbose:
                print(
                    "Jobs to run: \n%s\n"
                    % "\n".join(str(j) for j in build_jobs)
                )

            num_failures, build_resultset = jobset.run(
                build_jobs, newline_on_success=True, maxjobs=args.jobs
            )

            report_utils.render_junit_xml_report(
                build_resultset, _DOCKER_BUILD_XML_REPORT
            )

            if num_failures == 0:
                jobset.message(
                    "SUCCESS",
                    "All docker images built successfully.",
                    do_newline=True,
                )
            else:
                jobset.message(
                    "FAILED",
                    "Failed to build interop docker images.",
                    do_newline=True,
                )
                for image in sorted(set(docker_images.values())):
                    dockerjob.remove_image(image, skip_nonexistent=True)
                sys.exit(1)

    server_manual_cmd_log = [] if args.manual_run else None
    client_manual_cmd_log = [] if args.manual_run else None

    server_jobs: Dict[str, Any] = {}
    server_addresses: Dict[str, Tuple[str, Any]] = {}
    try:
        for s in sorted(servers):
            lang = str(s)
            spec = server_jobspec(
                _LANGUAGES[lang],
                docker_images.get(lang),
                args.transport_security,
                manual_cmd_log=server_manual_cmd_log,
            )
            if not args.manual_run:
                job = dockerjob.DockerJob(spec)
                server_jobs[lang] = job
                server_addresses[lang] = (
                    "localhost",
                    job.mapped_port(_DEFAULT_SERVER_PORT),
                )
            else:
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")

        http2_server_job = None
        if args.http2_server_interop:
            lang = str(http2InteropServer)
            spec = server_jobspec(
                http2InteropServer,
                docker_images.get(lang),
                manual_cmd_log=server_manual_cmd_log,
            )
            if not args.manual_run:
                http2_server_job = dockerjob.DockerJob(spec)
                server_jobs[lang] = http2_server_job
            else:
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")

        jobs: List[jobset.JobSpec] = []
        if args.cloud_to_prod:
            if not _TRANSPORT_SECURITY_CFG.get(
                args.transport_security, {}
            ).get("supports_cloud_to_prod", False):
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                for language in sorted(languages, key=lambda x: str(x)):
                    for test_case in _CLOUD_TO_PROD_TEST_CASES:
                        if test_case not in language.unimplemented_test_cases():
                            for (
                                transport_security
                            ) in args.custom_credentials_type:
                                if transport_security not in getattr(
                                    language,
                                    "supported_custom_credentials_types",
                                    ["tls"],
                                ):
                                    continue
                                test_job = cloud_to_prod_jobspec(
                                    language,
                                    test_case,
                                    server_host_nickname,
                                    prod_servers[server_host_nickname],
                                    google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                    docker_image=docker_images.get(
                                        str(language)
                                    ),
                                    manual_cmd_log=client_manual_cmd_log,
                                    service_account_key_file=args.service_account_key_file,
                                    default_service_account=args.default_service_account,
                                    transport_security=transport_security,
                                )
                                jobs.append(test_job)
                if args.http2_interop:
                    for test_case in _HTTP2_TEST_CASES:
                        test_job = cloud_to_prod_jobspec(
                            http2Interop,
                            test_case,
                            server_host_nickname,
                            prod_servers[server_host_nickname],
                            google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                            docker_image=docker_images.get(str(http2Interop)),
                            manual_cmd_log=client_manual_cmd_log,
                            service_account_key_file=args.service_account_key_file,
                            default_service_account=args.default_service_account,
                            transport_security=args.transport_security,
                        )
                        jobs.append(test_job)

        if args.cloud_to_prod_auth:
            if not _TRANSPORT_SECURITY_CFG.get(
                args.transport_security, {}
            ).get("supports_cloud_to_prod", False):
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                for language in sorted(languages, key=lambda x: str(x)):
                    for test_case in _AUTH_TEST_CASES:
                        if (
                            not args.skip_compute_engine_creds
                            or not compute_engine_creds_required(
                                language, test_case
                            )
                        ):
                            if (
                                test_case
                                not in language.unimplemented_test_cases()
                            ):
                                tc_meta = _TEST_CASES_META.get(test_case, {})
                                transport_security = str(
                                    tc_meta.get("transport_security", "tls")
                                )
                                if (
                                    transport_security
                                    not in args.custom_credentials_type
                                ):
                                    continue
                                test_job = cloud_to_prod_jobspec(
                                    language,
                                    test_case,
                                    server_host_nickname,
                                    prod_servers[server_host_nickname],
                                    google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                    docker_image=docker_images.get(
                                        str(language)
                                    ),
                                    auth=True,
                                    manual_cmd_log=client_manual_cmd_log,
                                    service_account_key_file=args.service_account_key_file,
                                    default_service_account=args.default_service_account,
                                    transport_security=transport_security,
                                )
                                jobs.append(test_job)

        for server in args.override_server:
            if len(server) != 2 or ":" not in server[1]:
                print(f"Invalid --override_server entry: {server}")
                sys.exit(1)
            server_name = server[0]
            (server_host, server_port) = server[1].split(":", 1)
            server_addresses[server_name] = (server_host, server_port)

        for server_name, server_address in list(server_addresses.items()):
            (server_host, server_port) = server_address
            server_language = _LANGUAGES.get(server_name, None)
            skip_server = []
            if server_language:
                skip_server = server_language.unimplemented_test_cases_server()
            for language in sorted(languages, key=lambda x: str(x)):
                for test_case in _TEST_CASES:
                    if test_case not in language.unimplemented_test_cases():
                        if test_case not in skip_server:
                            test_job = cloud_to_cloud_jobspec(
                                language,
                                test_case,
                                server_name,
                                server_host,
                                server_port,
                                docker_image=docker_images.get(str(language)),
                                transport_security=args.transport_security,
                                manual_cmd_log=client_manual_cmd_log,
                            )
                            jobs.append(test_job)

            if args.http2_interop:
                for test_case in _HTTP2_TEST_CASES:
                    if server_language and not getattr(
                        server_language,
                        "supports_http2_badclient_interop",
                        True,
                    ):
                        continue
                    test_job = cloud_to_cloud_jobspec(
                        http2Interop,
                        test_case,
                        server_name,
                        server_host,
                        server_port,
                        docker_image=docker_images.get(str(http2Interop)),
                        transport_security=args.transport_security,
                        manual_cmd_log=client_manual_cmd_log,
                    )
                    jobs.append(test_job)

        if args.http2_server_interop:
            if not args.manual_run:
                http2_server_job.wait_for_healthy(
                    timeout_seconds=int(
                        _DEFAULTS["server_healthy_timeout_seconds"]
                    )
                )
            for language in sorted(
                languages_http2_clients_for_http2_server_interop,
                key=lambda x: str(x),
            ):
                for test_case in sorted(
                    set(_HTTP2_SERVER_TEST_CASES)
                    - set(_HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS)
                ):
                    offset = sorted(_HTTP2_SERVER_TEST_CASES).index(test_case)
                    server_port = _DEFAULT_SERVER_PORT + offset
                    if not args.manual_run:
                        server_port = http2_server_job.mapped_port(server_port)
                    test_job = cloud_to_cloud_jobspec(
                        language,
                        test_case,
                        str(http2InteropServer),
                        "localhost",
                        server_port,
                        docker_image=docker_images.get(str(language)),
                        manual_cmd_log=client_manual_cmd_log,
                    )
                    jobs.append(test_job)
            for language in sorted(languages, key=lambda x: str(x)):
                for test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
                    if test_case not in language.unimplemented_test_cases():
                        offset = sorted(_HTTP2_SERVER_TEST_CASES).index(
                            test_case
                        )
                        server_port = _DEFAULT_SERVER_PORT + offset
                        if not args.manual_run:
                            server_port = http2_server_job.mapped_port(
                                server_port
                            )
                        if args.transport_security != "insecure":
                            print(
                                "Creating grpc client to http2 server test case"
                                " with insecure connection, even though"
                                " args.transport_security is not insecure."
                                " Http2 test server only supports insecure"
                                " connections."
                            )
                        test_job = cloud_to_cloud_jobspec(
                            language,
                            test_case,
                            str(http2InteropServer),
                            "localhost",
                            server_port,
                            docker_image=docker_images.get(str(language)),
                            transport_security="insecure",
                            manual_cmd_log=client_manual_cmd_log,
                        )
                        jobs.append(test_job)

        if not jobs:
            print("No jobs to run.")
            for image in sorted(set(docker_images.values())):
                dockerjob.remove_image(image, skip_nonexistent=True)
            sys.exit(1)

        if args.manual_run:
            print("All tests will skipped --manual_run option is active.")

        if args.verbose:
            print("Jobs to run: \n%s\n" % "\n".join(str(job) for job in jobs))

        num_failures, resultset = jobset.run(
            jobs,
            newline_on_success=True,
            maxjobs=args.jobs,
            skip_jobs=args.manual_run,
        )
        if (
            args.bq_result_table
            and resultset
            and upload_interop_results_to_bq is not None
        ):
            upload_interop_results_to_bq(resultset, args.bq_result_table)
        if num_failures:
            jobset.message("FAILED", "Some tests failed", do_newline=True)
        else:
            jobset.message("SUCCESS", "All tests passed", do_newline=True)

        write_cmdlog_maybe(
            server_manual_cmd_log, str(_DEFAULTS["server_manual_cmd_log_file"])
        )
        write_cmdlog_maybe(
            client_manual_cmd_log, str(_DEFAULTS["client_manual_cmd_log_file"])
        )

        report_utils.render_junit_xml_report(resultset, _TESTS_XML_REPORT)

        if num_failures:
            sys.exit(1)
        else:
            sys.exit(0)
    finally:
        for server, job in list(server_jobs.items()):
            if not job.is_running():
                print('Server "%s" has exited prematurely.' % server)

        dockerjob.finish_jobs(list(server_jobs.values()))

        for image in sorted(set(docker_images.values())):
            if not args.manual_run:
                print("Removing docker image %s" % image)
                dockerjob.remove_image(image)
            else:
                print("Preserving docker image: %s" % image)


# =============================================================================
# Backwards-Compatibility Matrix (client_matrix / create_matrix_images /
# run_interop_matrix_tests)
# =============================================================================


class ReleaseInfo:
    """Info about a single release of a language."""

    def __init__(
        self,
        patch: Optional[List[str]] = None,
        runtimes: Optional[List[str]] = None,
        testcases_file: Optional[str] = None,
    ) -> None:
        self.patch: List[str] = list(patch) if patch else []
        self.runtimes: List[str] = list(runtimes) if runtimes else []
        self.testcases_file: Optional[str] = testcases_file


def _load_matrix_tables() -> Tuple[
    Dict[str, List[str]], Dict[str, "OrderedDict[str, ReleaseInfo]"]
]:
    runtime_matrix: Dict[str, List[str]] = {}
    release_matrix: Dict[str, "OrderedDict[str, ReleaseInfo]"] = {}
    for lang, lcfg in _MATRIX_CFG["languages"].items():
        runtime_matrix[lang] = list(lcfg["default_runtimes"])
        od: "OrderedDict[str, ReleaseInfo]" = OrderedDict()
        for entry in lcfg.get("releases", []):
            tag = str(entry["tag"])
            od[tag] = ReleaseInfo(
                patch=entry.get("patch", []),
                runtimes=entry.get("runtimes", []),
                testcases_file=entry.get("testcases_file"),
            )
        release_matrix[lang] = od
    return runtime_matrix, release_matrix


LANG_RUNTIME_MATRIX, LANG_RELEASE_MATRIX = _load_matrix_tables()
MATRIX_LANGUAGES: List[str] = list(LANG_RUNTIME_MATRIX.keys())
_RELEASES: List[str] = sorted(
    list(
        set(
            release
            for release_dict in list(LANG_RELEASE_MATRIX.values())
            for release in list(release_dict.keys())
        )
    )
)


def get_github_repo(lang: str) -> str:
    lcfg = _MATRIX_CFG["languages"].get(lang)
    if lcfg and "repo" in lcfg:
        return str(lcfg["repo"])
    extra = _MATRIX_CFG.get("extra_repos", {})
    if lang in extra:
        return str(extra[lang])
    return str(_MATRIX_DEFAULTS["default_repo"])


def get_release_tags(lang: str) -> List[str]:
    """Returns list of known releases for given language."""
    return list(LANG_RELEASE_MATRIX[lang].keys())


def get_runtimes_for_lang_release(lang: str, release: str) -> List[str]:
    """Get list of valid runtimes for given release of lang."""
    runtimes = list(LANG_RUNTIME_MATRIX[lang])
    release_info = LANG_RELEASE_MATRIX[lang].get(release)
    if release_info and release_info.runtimes:
        runtimes = list(release_info.runtimes)
    return runtimes


def should_build_docker_interop_image_from_release_tag(lang: str) -> bool:
    lcfg = _MATRIX_CFG["languages"].get(lang, {})
    return bool(lcfg.get("build_from_release_tag", True))


def build_create_matrix_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run interop tests.")
    parser.add_argument(
        "--docker_path",
        default=str(_MATRIX_DEFAULTS["docker_path"]),
        help="Path of docker images",
    )
    parser.add_argument(
        "--release",
        default="master",
        choices=["all", "master"] + _RELEASES,
        help="github commit tag to checkout.",
    )
    parser.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(MATRIX_LANGUAGES),
        nargs="+",
        default=["all"],
        help="Test languages to build docker images for.",
    )
    parser.add_argument(
        "--git_checkout",
        action="store_true",
        help="Use a separate git clone tree for building grpc stack.",
    )
    parser.add_argument(
        "--git_checkout_root",
        default=str(_MATRIX_DEFAULTS["git_checkout_root"]),
        help="Directory under which language repositories will be cloned.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep the created local images after uploading to GCR",
    )
    parser.add_argument(
        "--reuse_git_root",
        default=False,
        action="store_const",
        const=True,
        help="reuse the repo dir.",
    )
    parser.add_argument(
        "--upload_images",
        action="store_true",
        help="If set, images will be uploaded to container registry.",
    )
    return parser


def build_run_matrix_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run interop tests.")
    parser.add_argument(
        "-j", "--jobs", default=multiprocessing.cpu_count(), type=int
    )
    parser.add_argument(
        "--docker_path",
        default=str(_MATRIX_DEFAULTS["docker_path"]),
        help="Path of docker images",
    )
    parser.add_argument(
        "--release",
        default="all",
        choices=["all"] + _RELEASES,
        help="Release tags to test.",
    )
    parser.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(MATRIX_LANGUAGES),
        nargs="+",
        default=["all"],
        help="Languages to test",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep the created local images after finishing the tests.",
    )
    parser.add_argument(
        "--report_file",
        default=str(_MATRIX_DEFAULTS["report_file"]),
        help="The result file to create.",
    )
    parser.add_argument(
        "--allow_flakes",
        default=False,
        action="store_const",
        const=True,
        help="Allow flaky tests to show as passing",
    )
    parser.add_argument(
        "--bq_result_table",
        default="",
        type=str,
        nargs="?",
        help="Upload test results to a specified BQ table.",
    )
    parser.add_argument(
        "--server_host",
        default=str(_MATRIX_DEFAULTS["server_host"]),
        type=str,
        nargs="?",
        help="The gateway to backend services.",
    )
    return parser


docker_images_cleanup: List[str] = []


def cleanup() -> None:
    if not getattr(args, "keep", False):
        for image in docker_images_cleanup:
            dockerjob.remove_image(image, skip_nonexistent=True)


atexit.register(cleanup)


def build_image_jobspec(
    runtime: str,
    env: Dict[str, str],
    gcr_tag: str,
    stack_base: str,
    lang: Optional[str] = None,
) -> jobset.JobSpec:
    """Build interop docker image for a language with runtime."""
    basename = "grpc_interop_%s" % runtime
    docker_path = getattr(args, "docker_path", _MATRIX_DEFAULTS["docker_path"])
    tag = "%s/%s:%s" % (docker_path, basename, gcr_tag)
    build_env = {"INTEROP_IMAGE": tag, "BASE_NAME": basename}
    build_env.update(env)
    if gcr_tag != "master":
        build_env["RELEASE_LABEL"] = gcr_tag
        if stack_base:
            build_env["BUILD_INFO_FILE"] = os.path.join(
                stack_base, "commit_log"
            )
    if (
        lang
        and stack_base
        and should_build_docker_interop_image_from_release_tag(lang)
    ):
        build_env["INTEROP_DOCKERFILE_ROOT"] = stack_base

    build_job = jobset.JobSpec(
        cmdline=[_IMAGE_BUILDER],
        environ=build_env,
        shortname="build_docker_%s" % runtime,
        timeout_seconds=int(_MATRIX_DEFAULTS["build_timeout_seconds"]),
    )
    build_job.tag = tag
    return build_job


def maybe_apply_patches_on_git_tag(
    stack_base: str, lang: str, release: str
) -> None:
    files_to_patch = []
    release_info = LANG_RELEASE_MATRIX[lang].get(release)
    if release_info:
        files_to_patch = release_info.patch
    if not files_to_patch:
        return
    patch_file = os.path.abspath(
        os.path.join(
            ROOT, "tools/interop_matrix/patches", f"{lang}_{release}", "git_repo.patch"
        )
    )
    if not os.path.exists(patch_file):
        jobset.message(
            "FAILED", "expected patch file |%s| to exist" % patch_file
        )
        sys.exit(1)
    subprocess.check_output(
        ["git", "apply", patch_file], cwd=stack_base, stderr=subprocess.STDOUT
    )
    for repo_relative_path in files_to_patch:
        subprocess.check_output(
            ["git", "add", repo_relative_path],
            cwd=stack_base,
            stderr=subprocess.STDOUT,
        )
    subprocess.check_output(
        [
            "git",
            "commit",
            "-m",
            "Hack performed on top of %s git "
            "tag in order to build and run the %s "
            "interop tests on that tag." % (lang, release),
        ],
        cwd=stack_base,
        stderr=subprocess.STDOUT,
    )


def checkout_grpc_stack(lang: str, release: str) -> str:
    """Invokes 'git checkout' for the lang/release and returns directory created."""
    assert args.git_checkout and args.git_checkout_root

    if not os.path.exists(args.git_checkout_root):
        os.makedirs(args.git_checkout_root)

    repo = get_github_repo(lang)
    repo_dir = os.path.splitext(os.path.basename(repo))[0]
    stack_base = os.path.join(args.git_checkout_root, repo_dir)

    if not args.reuse_git_root and os.path.exists(stack_base):
        jobset.message("START", "Removing git checkout root.", do_newline=True)
        shutil.rmtree(stack_base)

    if not os.path.exists(stack_base):
        subprocess.check_call(
            ["git", "clone", "--recursive", repo],
            cwd=os.path.dirname(stack_base),
        )

    jobset.message(
        "START",
        "git checkout %s from %s" % (release, stack_base),
        do_newline=True,
    )
    assert not os.path.dirname(__file__).startswith(stack_base)
    output = subprocess.check_output(
        ["git", "checkout", release], cwd=stack_base, stderr=subprocess.STDOUT
    )
    maybe_apply_patches_on_git_tag(stack_base, lang, release)
    commit_log = subprocess.check_output(["git", "log", "-1"], cwd=stack_base)
    jobset.message(
        "SUCCESS",
        "git checkout",
        "%s: %s" % (str(output), commit_log),
        do_newline=True,
    )

    jobset.message(
        "START",
        "git submodule update --init at %s from %s" % (release, stack_base),
        do_newline=True,
    )
    subprocess.check_call(
        ["git", "submodule", "update", "--init"],
        cwd=stack_base,
        stderr=subprocess.STDOUT,
    )
    jobset.message(
        "SUCCESS",
        "git submodule update --init",
        "%s: %s" % (str(output), commit_log),
        do_newline=True,
    )

    with open(os.path.join(stack_base, "commit_log"), "wb") as f:
        f.write(commit_log)
    return stack_base


def build_all_images_for_release(lang: str, release: str) -> List[str]:
    """Build all docker images for a release across all runtimes."""
    docker_images = []
    build_jobs = []

    env: Dict[str, str] = {}
    stack_base = ""
    if getattr(args, "git_checkout", False):
        stack_base = checkout_grpc_stack(lang, release)
        lcfg = _MATRIX_CFG["languages"].get(lang, {})
        var = str(
            lcfg.get("root_env_var", _MATRIX_DEFAULTS["default_root_env_var"])
        )
        env[var] = stack_base

    for runtime in get_runtimes_for_lang_release(lang, release):
        job = build_image_jobspec(runtime, env, release, stack_base, lang=lang)
        docker_images.append(job.tag)
        build_jobs.append(job)

    jobset.message("START", "Building interop docker images.", do_newline=True)
    print("Jobs to run: \n%s\n" % "\n".join(str(j) for j in build_jobs))

    num_failures, _ = jobset.run(
        build_jobs, newline_on_success=True, maxjobs=multiprocessing.cpu_count()
    )
    if num_failures:
        jobset.message(
            "FAILED", "Failed to build interop docker images.", do_newline=True
        )
        docker_images_cleanup.extend(docker_images)
        sys.exit(1)

    jobset.message(
        "SUCCESS",
        "All docker images built for %s at %s." % (lang, release),
        do_newline=True,
    )
    return docker_images


def build_all_images_for_lang(lang: str) -> List[str]:
    """Build all docker images for a language across releases and runtimes."""
    if not getattr(args, "git_checkout", False):
        if args.release != "master":
            print(
                "Cannot use --release without also enabling --git_checkout.\n"
            )
            sys.exit(1)
        releases = [args.release]
    else:
        if args.release == "all":
            releases = get_release_tags(lang)
        else:
            if args.release not in ["master"] + get_release_tags(lang):
                jobset.message(
                    "SKIPPED",
                    "%s for %s is not defined" % (args.release, lang),
                    do_newline=True,
                )
                return []
            releases = [args.release]

    images = []
    for release in releases:
        images += build_all_images_for_release(lang, release)
    jobset.message(
        "SUCCESS",
        "All docker images built for %s at %s." % (lang, releases),
        do_newline=True,
    )
    return images


def create_matrix_images_main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for tools/interop_matrix/create_matrix_images.py."""
    global args
    parser = build_create_matrix_arg_parser()
    args = parser.parse_args(argv)
    languages = args.language if args.language != ["all"] else MATRIX_LANGUAGES
    for lang in languages:
        docker_images = build_all_images_for_lang(lang)
        for image in docker_images:
            if args.upload_images:
                jobset.message("START", "Uploading %s" % image, do_newline=True)
                assert (
                    image.startswith(args.docker_path) and image.find(":") != -1
                )
                (image_name, tag_name) = image.rsplit(":", 1)
                alternate_image = (
                    f"{image_name}:infrastructure-public-image-{tag_name}"
                )
                subprocess.call(
                    ["docker", "image", "tag", image, alternate_image]
                )
                subprocess.call(["gcloud", "docker", "--", "push", image])
                subprocess.call(
                    ["gcloud", "docker", "--", "push", alternate_image]
                )
            else:
                print(
                    "Not uploading image %s, run with --upload_images to"
                    " upload." % image
                )


def _get_test_images_for_lang(
    lang: str, release_arg: str, image_path_prefix: str
) -> Dict[str, List[Tuple[str, str]]]:
    """Find docker images for a language across releases and runtimes."""
    if release_arg == "all":
        releases = get_release_tags(lang)
    else:
        if release_arg not in get_release_tags(lang):
            jobset.message(
                "SKIPPED",
                "release %s for %s is not defined" % (release_arg, lang),
                do_newline=True,
            )
            return {}
        releases = [release_arg]

    images: Dict[str, List[Tuple[str, str]]] = {}
    for tag in releases:
        for runtime in get_runtimes_for_lang_release(lang, tag):
            image_name = "%s/grpc_interop_%s:%s" % (
                image_path_prefix,
                runtime,
                tag,
            )
            images.setdefault(runtime, []).append((tag, image_name))
    return images


def _resolve_matrix_profile_name(
    lang: str, runtime: str, release: str
) -> str:
    lcfg = _MATRIX_CFG["languages"].get(lang, {})
    release_info = LANG_RELEASE_MATRIX.get(lang, {}).get(release)
    profile_name = release_info.testcases_file if release_info else None
    if not profile_name:
        profile_name = str(lcfg.get("default_profile", f"{lang}__master"))
    replacements = lcfg.get("runtime_profile_replacements", {})
    if runtime in replacements:
        src_prefix, dst_prefix = replacements[runtime]
        profile_name = profile_name.replace(str(src_prefix), str(dst_prefix))
    return profile_name


def _format_profile_line(
    profile: Dict[str, Any], server_host: str, test_case: str
) -> str:
    args_style = str(profile["args_style"])
    if args_style == "modern":
        inner_args = (
            f"--server_host={server_host} --server_port=443"
            f" --test_case={test_case} --use_tls=true"
        )
        inner_cmd = f'{profile["cmd_prefix"]} {inner_args}'
    elif args_style == "legacy_override":
        inner_args = (
            f"--server_host={server_host}"
            f" --server_host_override={server_host} --server_port=443"
            f" --use_tls=true --test_case={test_case}"
        )
        inner_cmd = f'{profile["cmd_prefix"]} {inner_args}'
    elif args_style == "python_modern":
        inner_args = (
            f"--server_host={server_host} --server_port=443"
            f" --test_case={test_case} --use_tls=true"
        )
        inner_cmd = f'{profile["cmd_prefix"]} --args=\\"{inner_args}\\"'
    elif args_style == "python_legacy_override":
        inner_args = (
            f"--server_host={server_host}"
            f" --server_host_override={server_host} --server_port=443"
            f" --use_tls=true --test_case={test_case}"
        )
        inner_cmd = f'{profile["cmd_prefix"]} --args=\\"{inner_args}\\"'
    else:
        raise ValueError(f"Unknown matrix profile args_style: {args_style}")

    parts = ["docker", "run", "-i", "--rm=true"]
    for env_kv in profile.get("docker_env", []):
        parts.extend(["-e", str(env_kv)])
    parts.extend(["-w", str(profile["workdir"]), "--net=host", "$docker_image"])
    parts.append(str(profile["shell"]))
    parts.append(f'"{inner_cmd}"')
    return " ".join(parts)


def _read_test_cases_file(lang: str, runtime: str, release: str) -> List[str]:
    """Synthesizes the 18 matrix test case commands from interop.toml profiles."""
    profile_name = _resolve_matrix_profile_name(lang, runtime, release)
    profile = _MATRIX_CFG["profiles"][profile_name]
    prod_hosts = _MATRIX_DEFAULTS["prod_hosts"]
    lines: List[str] = []
    for host in prod_hosts:
        for test_case in _CLOUD_TO_PROD_TEST_CASES:
            lines.append(_format_profile_line(profile, str(host), test_case))
    return lines


def _cleanup_docker_image(image: str) -> None:
    jobset.message("START", "Cleanup docker image %s" % image, do_newline=True)
    dockerjob.remove_image(image, skip_nonexistent=True)


def _generate_test_case_jobspecs(
    lang: str, runtime: str, release: str, suite_name: str
) -> List[jobset.JobSpec]:
    """Returns the list of test cases from testcase profiles per lang/release."""
    testcase_lines = _read_test_cases_file(lang, runtime, release)
    target_server_host = getattr(
        args, "server_host", str(_MATRIX_DEFAULTS["server_host"])
    )
    allow_flakes = bool(getattr(args, "allow_flakes", False))

    job_spec_list: List[jobset.JobSpec] = []
    for line in testcase_lines:
        print("Creating jobspec with cmdline '{}'".format(line))
        m = re.search(r"--test_case=(\w+)", line)
        testcase_name = m.group(1) if m else "unknown_test"

        if "--server_host_override=" in line:
            m = re.search(
                r"--server_host_override=((.*).sandbox.googleapis.com)", line
            )
        else:
            m = re.search(r"--server_host=((.*).sandbox.googleapis.com)", line)
        server = m.group(1) if m else "unknown_server"
        server_short = m.group(2) if m else "unknown_server"

        assert "--server_host=" in line
        line = re.sub(
            r"--server_host=[^ ]*",
            r"--server_host=%s" % target_server_host,
            line,
        )

        if (
            target_server_host != server
            and "--server_host_override=" not in line
        ):
            line = re.sub(
                r"(--server_host=[^ ]*)",
                r"\1 --server_host_override=%s" % server,
                line,
            )

        spec = jobset.JobSpec(
            cmdline=line,
            shortname="%s:%s:%s:%s"
            % (suite_name, lang, server_short, testcase_name),
            timeout_seconds=_TEST_TIMEOUT_SECONDS,
            shell=True,
            flake_retries=5 if allow_flakes else 0,
        )
        job_spec_list.append(spec)
    return job_spec_list


def _pull_image_for_lang(
    lang: str, image: str, release: Optional[str] = None
) -> jobset.JobSpec:
    """Pull an image for a given language from the image registry."""
    cmdline = [
        "time gcloud docker -- pull %s && time docker run --rm=true %s"
        " /bin/true" % (image, image)
    ]
    return jobset.JobSpec(
        cmdline=cmdline,
        shortname="pull_image_{}".format(image),
        timeout_seconds=_PULL_IMAGE_TIMEOUT_SECONDS,
        shell=True,
        flake_retries=2,
    )


def _test_release(
    lang: str,
    runtime: str,
    release: str,
    image: str,
    xml_report_tree: Any,
    skip_tests: bool,
) -> int:
    total_num_failures = 0
    suite_name = "%s__%s_%s" % (lang, runtime, release)
    job_spec_list = _generate_test_case_jobspecs(
        lang, runtime, release, suite_name
    )

    if not job_spec_list:
        jobset.message("FAILED", "No test cases were found.", do_newline=True)
        total_num_failures += 1
    else:
        maxjobs = getattr(args, "jobs", multiprocessing.cpu_count())
        num_failures, resultset = jobset.run(
            job_spec_list,
            newline_on_success=True,
            add_env={"docker_image": image},
            maxjobs=maxjobs,
            skip_jobs=skip_tests,
        )
        bq_table = getattr(args, "bq_result_table", "")
        if bq_table and resultset and upload_test_results is not None:
            upload_test_results.upload_interop_results_to_bq(
                resultset, bq_table
            )
        if skip_tests:
            jobset.message("FAILED", "Tests were skipped", do_newline=True)
            total_num_failures += 1
        if num_failures:
            total_num_failures += num_failures

        report_utils.append_junit_xml_results(
            xml_report_tree,
            resultset,
            str(_MATRIX_DEFAULTS["junit_suite_package"]),
            suite_name,
            str(uuid.uuid4()),
        )
    return total_num_failures


def _run_tests_for_lang(
    lang: str,
    runtime: str,
    images: Sequence[Tuple[str, str]],
    xml_report_tree: Any,
) -> int:
    """Find and run all test cases for a language."""
    skip_tests = False
    total_num_failures = 0

    maxjobs = getattr(args, "jobs", multiprocessing.cpu_count())
    max_pull_jobs = min(maxjobs, _MAX_PARALLEL_DOWNLOADS)
    max_chunk_size = max_pull_jobs
    chunk_count = (len(images) + max_chunk_size) // max_chunk_size

    for chunk_index in range(chunk_count):
        chunk_start = chunk_index * max_chunk_size
        chunk_size = min(max_chunk_size, len(images) - chunk_start)
        chunk_end = chunk_start + chunk_size
        pull_specs = []
        if not skip_tests:
            for release, image in images[chunk_start:chunk_end]:
                pull_specs.append(_pull_image_for_lang(lang, image, release))

        pull_failures, _ = jobset.run(
            pull_specs, newline_on_success=True, maxjobs=max_pull_jobs
        )
        if pull_failures:
            jobset.message(
                "FAILED",
                'Image download failed. Skipping tests for language "%s"'
                % lang,
                do_newline=True,
            )
            skip_tests = True
        for release, image in images[chunk_start:chunk_end]:
            total_num_failures += _test_release(
                lang, runtime, release, image, xml_report_tree, skip_tests
            )
        if not getattr(args, "keep", False):
            for _, image in images[chunk_start:chunk_end]:
                _cleanup_docker_image(image)
    if not total_num_failures:
        jobset.message(
            "SUCCESS", "All {} tests passed".format(lang), do_newline=True
        )
    else:
        jobset.message(
            "FAILED", "Some {} tests failed".format(lang), do_newline=True
        )

    return total_num_failures


def run_interop_matrix_tests_main(
    argv: Optional[Sequence[str]] = None,
) -> None:
    """Entry point for tools/interop_matrix/run_interop_matrix_tests.py."""
    global args
    parser = build_run_matrix_arg_parser()
    args = parser.parse_args(argv)
    languages = args.language if args.language != ["all"] else MATRIX_LANGUAGES
    total_num_failures = 0
    _xml_report_tree = report_utils.new_junit_xml_tree()
    for lang in languages:
        docker_images = _get_test_images_for_lang(
            lang, args.release, args.docker_path
        )
        for runtime in sorted(docker_images.keys()):
            total_num_failures += _run_tests_for_lang(
                lang, runtime, docker_images[runtime], _xml_report_tree
            )

    report_utils.create_xml_report_file(_xml_report_tree, args.report_file)

    if total_num_failures:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
