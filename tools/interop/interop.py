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
"""Thin Python loader and compatibility harness backed by tools/interop/config.json and interop_runner."""

import argparse
import atexit
from collections import OrderedDict
import copy
from dataclasses import dataclass, field
import itertools
import json
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import uuid

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
DEFAULT_CONFIG_PATH = os.path.join(_THIS_DIR, "config.json")
RUST_SOURCE_PATH = os.path.join(_THIS_DIR, "interop_runner.rs")
RUST_BINARY_PATH = os.path.join(_THIS_DIR, "interop_runner")

_PYTHON_UTILS_DIR = os.path.abspath(
    os.path.join(_THIS_DIR, "../run_tests/python_utils")
)
if _PYTHON_UTILS_DIR not in sys.path:
    sys.path.append(_PYTHON_UTILS_DIR)

import dockerjob  # noqa: E402
import jobset  # noqa: E402
import report_utils  # noqa: E402

try:
    import upload_test_results
    from upload_test_results import upload_interop_results_to_bq
except ImportError:
    upload_test_results = None
    upload_interop_results_to_bq = None


VALID_ARGS_MODES = frozenset(
    {"append", "joined_flag", "ignore_args", "extract_port_template"}
)
VALID_AUTH_CRED_MODES = frozenset({"env", "flag"})


def ensure_rust_runner_binary() -> str:
    """Ensures tools/interop/interop_runner is compiled and up-to-date."""
    if os.path.exists(RUST_BINARY_PATH) and os.access(RUST_BINARY_PATH, os.X_OK):
        if not os.path.exists(RUST_SOURCE_PATH) or (
            os.path.getmtime(RUST_BINARY_PATH) >= os.path.getmtime(RUST_SOURCE_PATH)
        ):
            return RUST_BINARY_PATH
    subprocess.check_call(
        ["rustc", "-O", RUST_SOURCE_PATH, "-o", RUST_BINARY_PATH]
    )
    return RUST_BINARY_PATH


def run_rust_runner(args: Sequence[str], check: bool = True) -> subprocess.CompletedProcess:
    """Invokes the compiled Rust interop_runner binary with the given arguments."""
    binary = ensure_rust_runner_binary()
    return subprocess.run(
        [binary] + list(args),
        check=check,
        text=True,
        capture_output=True,
    )


class SpecValidationError(ValueError):
    """Raised when config.json fails schema or cross-reference validation."""


@dataclass
class CommandSpec:
    """Declarative command construction specification."""

    command: List[str]
    args_mode: str = "append"
    arg_template: Optional[str] = None
    port_regex: Optional[str] = None

    def render(self, args: Sequence[str]) -> Optional[List[str]]:
        args_list = list(args)
        if self.args_mode == "append":
            return list(self.command) + args_list
        if self.args_mode == "joined_flag":
            joined = " ".join(args_list)
            formatted = (self.arg_template or '--args="{args}"').format(
                args=joined
            )
            return list(self.command) + [formatted]
        if self.args_mode == "ignore_args":
            return list(self.command)
        if self.args_mode == "extract_port_template":
            pattern = self.port_regex or r"--server_port=(\d+)"
            for arg in args_list:
                match = re.search(pattern, arg)
                if match:
                    portnum = match.group(1)
                    cmdline = (self.arg_template or "").format(port=portnum)
                    return [cmdline]
            return None
        raise SpecValidationError(f"Unsupported args_mode: {self.args_mode}")


@dataclass
class DockerBuildSpec:
    """Declarative Docker build configuration for a language or runtime."""

    base_name: str
    current_version_file: str
    dockerfile_dir: str
    build_script: str
    repo_mount: str = "grpc"
    workdir: str = "/var/local/git/grpc"
    env: Dict[str, str] = field(default_factory=dict)
    build_commands: List[str] = field(default_factory=list)


@dataclass
class HealthcheckSpec:
    """Docker container healthcheck specification."""

    cmd_template: str
    interval: str = "1s"
    retries: int = 5
    timeout: str = "10s"
    wait_timeout_seconds: int = 600


@dataclass
class ServerDockerConfig:
    """Special server Docker container configuration."""

    port_count_from_suite: Optional[str] = None
    port_count: int = 1
    healthcheck: Optional[HealthcheckSpec] = None


class LanguageSpec:
    """Declarative language/target definition compatible with legacy Language classes."""

    def __init__(
        self,
        name: str,
        safename: str,
        repo: str,
        included_in_all_clients: bool,
        supports_server: bool,
        docker_supported: bool,
        auth_credentials_mode: str,
        client_cwd: Optional[str],
        server_cwd: Optional[str],
        http2_cwd: Optional[str],
        global_env: Dict[str, str],
        cloud_to_prod_env: Dict[str, str],
        client_cmd_spec: Optional[CommandSpec],
        server_cmd_spec: Optional[CommandSpec],
        http2_client_cmd_spec: Optional[CommandSpec],
        unimplemented_client_cases: List[str],
        unimplemented_server_cases: List[str],
        client_test_cases: Optional[List[str]] = None,
        server_test_cases: Optional[List[str]] = None,
        docker_build: Optional[DockerBuildSpec] = None,
        server_docker_config: Optional[ServerDockerConfig] = None,
        is_http2_target: bool = False,
        server_unimplemented_client_cases: Optional[List[str]] = None,
        legacy_factory: Optional[str] = None,
        legacy_aliases: Optional[List[str]] = None,
        server_legacy_factory: Optional[str] = None,
        oauth2_uses_gce_creds: bool = False,
        skip_http2_bad_client: bool = False,
        oci_labels: Optional[Dict[str, str]] = None,
        container_entrypoints: Optional[Dict[str, Optional[str]]] = None,
        client_capabilities: Optional[List[str]] = None,
        server_capabilities: Optional[List[str]] = None,
    ):
        self.name = name
        self.safename = safename
        self.repo = repo
        self.included_in_all_clients = included_in_all_clients
        self.supports_server = supports_server
        self.docker_supported = docker_supported
        self.auth_credentials_mode = auth_credentials_mode
        self.client_cwd = client_cwd
        self.server_cwd = server_cwd
        self.http2_cwd = http2_cwd
        self._global_env = dict(global_env)
        self._cloud_to_prod_env = dict(cloud_to_prod_env)
        self.client_cmd_spec = client_cmd_spec
        self.server_cmd_spec = server_cmd_spec
        self.http2_client_cmd_spec = http2_client_cmd_spec
        self._unimplemented_client_cases = list(unimplemented_client_cases)
        self._unimplemented_server_cases = list(unimplemented_server_cases)
        self.client_test_cases = list(client_test_cases or [])
        self.server_test_cases = list(server_test_cases or [])
        self._server_unimplemented_client_cases = (
            list(server_unimplemented_client_cases)
            if server_unimplemented_client_cases is not None
            else list(unimplemented_client_cases)
        )
        self.docker_build = docker_build
        self.server_docker_config = server_docker_config
        self.is_http2_target = is_http2_target
        self.legacy_factory = legacy_factory
        self.legacy_aliases = list(legacy_aliases or [])
        self.server_legacy_factory = server_legacy_factory
        self.oauth2_uses_gce_creds = oauth2_uses_gce_creds
        self.skip_http2_bad_client = skip_http2_bad_client
        self.oci_labels = dict(oci_labels or {})
        self.container_entrypoints = dict(container_entrypoints or {})
        self.client_capabilities = list(client_capabilities or [])
        self.server_capabilities = list(server_capabilities or [])

    def client_cmd(self, args: Sequence[str]) -> Optional[List[str]]:
        if not self.client_cmd_spec:
            raise SpecValidationError(
                f"Language '{self.name}' does not define a client_cmd"
            )
        return self.client_cmd_spec.render(args)

    def server_cmd(self, args: Sequence[str]) -> List[str]:
        if not self.server_cmd_spec:
            raise SpecValidationError(
                f"Language '{self.name}' does not define a server_cmd"
            )
        rendered = self.server_cmd_spec.render(args)
        return rendered if rendered is not None else []

    def client_cmd_http2interop(self, args: Sequence[str]) -> List[str]:
        if not self.http2_client_cmd_spec:
            raise SpecValidationError(
                f"Language '{self.name}' does not define an http2_client_cmd"
            )
        rendered = self.http2_client_cmd_spec.render(args)
        return rendered if rendered is not None else []

    def global_env(self) -> Dict[str, str]:
        return dict(self._global_env)

    def cloud_to_prod_env(self) -> Dict[str, str]:
        return dict(self._cloud_to_prod_env)

    def unimplemented_test_cases(self) -> List[str]:
        return list(self._unimplemented_client_cases)

    def unimplemented_test_cases_server(self) -> List[str]:
        return list(self._unimplemented_server_cases)

    def has_active_server_cases(self) -> bool:
        return bool(self.server_test_cases)

    def as_http2_server_target(self) -> "LanguageSpec":
        clone = copy.copy(self)
        clone._unimplemented_client_cases = list(
            self._server_unimplemented_client_cases
        )
        return clone

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"LanguageSpec({self.name!r})"


@dataclass
class StandardTestCaseSpec:
    name: str
    cloud_to_prod_eligible: bool = True
    extra_cloud_to_cloud_flags: List[str] = field(default_factory=list)


@dataclass
class AuthTestCaseSpec:
    name: str
    transport_security: str = "tls"
    requires_compute_engine_creds: bool = False
    requires_compute_engine_creds_languages: List[str] = field(
        default_factory=list
    )
    uses_service_account_key: bool = False
    uses_service_account_key_when_flag: Optional[str] = None
    uses_oauth_scope: bool = False
    uses_default_service_account: bool = False
    uses_default_service_account_languages: List[str] = field(
        default_factory=list
    )

    def needs_compute_engine_creds(
        self, language_name: str, lang_spec: Optional[LanguageSpec] = None
    ) -> bool:
        if self.requires_compute_engine_creds:
            return True
        if lang_spec and lang_spec.oauth2_uses_gce_creds and self.name == "oauth2_auth_token":
            return True
        return str(language_name) in self.requires_compute_engine_creds_languages

    def needs_default_service_account(
        self, language_name: str, lang_spec: Optional[LanguageSpec] = None
    ) -> bool:
        if self.uses_default_service_account:
            return True
        if lang_spec and lang_spec.oauth2_uses_gce_creds and self.name == "oauth2_auth_token":
            return True
        return str(language_name) in self.uses_default_service_account_languages


@dataclass
class Http2ClientTestCaseSpec:
    name: str


@dataclass
class Http2ServerTestCaseSpec:
    name: str
    client_kind: str = "http2_interop"
    grpc_client_test_case: Optional[str] = None
    transport_security_override: Optional[str] = None


class ReleaseInfo:
    """Info about a single release of a language (compatible with client_matrix.ReleaseInfo)."""

    def __init__(self, patch=None, runtimes=None, testcases_file=None):
        self.patch = list(patch) if patch else []
        self.runtimes = list(runtimes) if runtimes else []
        self.testcases_file = testcases_file

    def __eq__(self, other):
        if not isinstance(other, ReleaseInfo):
            return False
        return (
            self.patch == other.patch
            and self.runtimes == other.runtimes
            and self.testcases_file == other.testcases_file
        )

    def __repr__(self):
        return (
            f"ReleaseInfo(patch={self.patch!r}, runtimes={self.runtimes!r}, "
            f"testcases_file={self.testcases_file!r})"
        )


@dataclass
class MatrixTestcaseProfile:
    name: str
    default_image: Optional[str]
    workdir: str
    inner_command_template: str
    env: Dict[str, str] = field(default_factory=dict)
    bash_login: bool = False
    container_names: List[str] = field(default_factory=list)

    def render_commands(
        self, servers: Sequence[str], test_cases: Sequence[str]
    ) -> List[str]:
        env_parts = [f"-e {k}={v}" for k, v in self.env.items()]
        env_str = (" " + " ".join(env_parts)) if env_parts else ""
        bash_cmd = "bash -l -c" if self.bash_login else "bash -c"
        lines = []
        idx = 0
        for srv in servers:
            for tc in test_cases:
                name_str = ""
                if self.container_names and idx < len(self.container_names):
                    name_str = f" --name {self.container_names[idx]}"
                inner = self.inner_command_template.format(
                    server_host=srv, test_case=tc
                )
                line = (
                    f"docker run -i --rm=true{env_str} -w {self.workdir}"
                    f' --net=host{name_str} $docker_image {bash_cmd} "{inner}"'
                )
                lines.append(line)
                idx += 1
        return lines


def validate_spec_data(raw: Dict[str, Any]) -> None:
    """Validates schema structure and cross-references of the raw config dictionary."""
    if not isinstance(raw, dict):
        raise SpecValidationError("Root of config.json must be a mapping")

    required_top_keys = [
        "schema_version",
        "defaults",
        "prod_servers",
        "transport_security",
        "custom_credentials_types",
        "test_case_groups",
        "suites",
        "test_cases",
        "languages",
        "matrix",
    ]
    for key in required_top_keys:
        if key not in raw:
            raise SpecValidationError(
                f"Missing required top-level key: '{key}'"
            )

    if str(raw["schema_version"]) not in ("1", "1.0", "2", "2.0"):
        raise SpecValidationError(
            f"Unsupported schema_version: {raw['schema_version']!r}"
        )
    if "capabilities" in raw:
        caps = raw["capabilities"]
        if not isinstance(caps, dict) or not caps.get("all_capabilities"):
            raise SpecValidationError(
                "capabilities.all_capabilities must be a non-empty list"
            )

    test_cases_sec = raw["test_cases"]
    for tc_cat in ["standard", "auth", "http2_client", "http2_server"]:
        if tc_cat not in test_cases_sec or not isinstance(
            test_cases_sec[tc_cat], list
        ):
            raise SpecValidationError(
                f"test_cases.{tc_cat} must be a non-empty list"
            )

    standard_names = [c["name"] for c in test_cases_sec["standard"]]
    auth_names = [c["name"] for c in test_cases_sec["auth"]]
    http2_client_names = [c["name"] for c in test_cases_sec["http2_client"]]
    http2_server_names = [c["name"] for c in test_cases_sec["http2_server"]]
    all_known_cases = set(
        standard_names + auth_names + http2_client_names + http2_server_names
    )

    groups = dict(raw["test_case_groups"])
    builtin_groups = {
        "standard_test_cases": standard_names,
        "standard_test_cases_except_first": standard_names[1:],
        "auth_test_cases": auth_names,
        "http2_client_test_cases": http2_client_names,
        "http2_server_test_cases": http2_server_names,
    }
    for gname, gitems in groups.items():
        if not isinstance(gitems, list):
            raise SpecValidationError(
                f"test_case_groups.{gname} must be a list"
            )
        for item in gitems:
            if item not in all_known_cases:
                raise SpecValidationError(
                    f"Unknown test case '{item}' in test_case_groups.{gname}"
                )

    languages_sec = raw["languages"]
    if not isinstance(languages_sec, dict) or not languages_sec:
        raise SpecValidationError("languages must be a non-empty mapping")

    for lang_name, ldata in languages_sec.items():
        if not isinstance(ldata, dict):
            raise SpecValidationError(
                f"Language '{lang_name}' definition must be a mapping"
            )
        if not ldata.get("safename"):
            raise SpecValidationError(
                f"Language '{lang_name}' missing 'safename'"
            )
        auth_mode = ldata.get("auth_credentials_mode")
        if auth_mode not in VALID_AUTH_CRED_MODES:
            raise SpecValidationError(
                f"Language '{lang_name}' has invalid auth_credentials_mode: {auth_mode!r}"
            )
        for cmd_key in ["client_cmd", "server_cmd", "http2_client_cmd"]:
            cmd_val = ldata.get(cmd_key)
            if cmd_val is not None:
                mode = cmd_val.get("args_mode", "append")
                if mode not in VALID_ARGS_MODES:
                    raise SpecValidationError(
                        f"Language '{lang_name}'.{cmd_key} has invalid args_mode: {mode!r}"
                    )
        if ldata.get("supports_server") and not ldata.get("server_cmd"):
            raise SpecValidationError(
                f"Language '{lang_name}' sets supports_server=True but has no server_cmd"
            )
        for list_key in [
            "unimplemented_client_cases",
            "unimplemented_server_cases",
            "server_unimplemented_client_cases",
        ]:
            for ref in ldata.get(list_key) or []:
                if ref.startswith("@"):
                    gref = ref[1:]
                    if gref not in groups and gref not in builtin_groups:
                        raise SpecValidationError(
                            f"Language '{lang_name}'.{list_key} references unknown group '{ref}'"
                        )
                elif ref not in all_known_cases:
                    raise SpecValidationError(
                        f"Language '{lang_name}'.{list_key} references unknown test case '{ref}'"
                    )

    for sec_name, sec_data in raw["transport_security"].items():
        for role_key in ["supported_clients", "supported_servers"]:
            allowed = sec_data.get(role_key, "all")
            if allowed != "all":
                for l in allowed:
                    if l not in languages_sec:
                        raise SpecValidationError(
                            f"transport_security.{sec_name}.{role_key} references unknown language '{l}'"
                        )

    for cred_name, cred_data in raw["custom_credentials_types"].items():
        allowed = cred_data.get("supported_clients", "all")
        if allowed != "all":
            for l in allowed:
                if l not in languages_sec:
                    raise SpecValidationError(
                        f"custom_credentials_types.{cred_name} references unknown language '{l}'"
                    )

    matrix_sec = raw["matrix"]
    profiles_sec = matrix_sec.get("testcase_profiles", {})
    runtimes_sec = matrix_sec.get("runtimes", {})
    for mlang, mdata in matrix_sec.get("languages", {}).items():
        for r in mdata.get("default_runtimes", []):
            if r not in runtimes_sec:
                raise SpecValidationError(
                    f"matrix.languages.{mlang}.default_runtimes references unknown runtime '{r}'"
                )
        for rel_tag, rinfo in mdata.get("releases", {}).items():
            for r in rinfo.get("runtimes", []):
                if r not in runtimes_sec:
                    raise SpecValidationError(
                        f"matrix.languages.{mlang}.releases.{rel_tag} references unknown runtime '{r}'"
                    )
            tc_file = rinfo.get("testcases_file")
            if tc_file and tc_file not in profiles_sec:
                raise SpecValidationError(
                    f"matrix.languages.{mlang}.releases.{rel_tag} references unknown testcase_profile '{tc_file}'"
                )


class InteropSpec:
    """Parsed, validated Single Source of Truth specification from tools/interop/config.json."""

    def __init__(self, raw: Dict[str, Any], spec_path: str = DEFAULT_CONFIG_PATH):
        validate_spec_data(raw)
        self.raw = raw
        self.spec_path = spec_path
        self.schema_version: str = str(raw["schema_version"])
        self.defaults: Dict[str, Any] = dict(raw["defaults"])
        self.prod_servers: Dict[str, str] = dict(raw["prod_servers"])
        self.transport_security: Dict[str, Dict[str, Any]] = dict(
            raw["transport_security"]
        )
        self.custom_credentials_types: Dict[str, Dict[str, Any]] = dict(
            raw["custom_credentials_types"]
        )
        self.test_case_groups: Dict[str, List[str]] = {
            k: list(v) for k, v in raw["test_case_groups"].items()
        }
        self.suites: Dict[str, Dict[str, Any]] = dict(raw["suites"])

        self.standard_test_cases_spec: List[StandardTestCaseSpec] = [
            StandardTestCaseSpec(
                name=c["name"],
                cloud_to_prod_eligible=bool(
                    c.get("cloud_to_prod_eligible", True)
                ),
                extra_cloud_to_cloud_flags=list(
                    c.get("extra_cloud_to_cloud_flags") or []
                ),
            )
            for c in raw["test_cases"]["standard"]
        ]
        self.standard_test_cases: List[str] = [
            c.name for c in self.standard_test_cases_spec
        ]
        self.standard_test_case_map: Dict[str, StandardTestCaseSpec] = {
            c.name: c for c in self.standard_test_cases_spec
        }

        self.auth_test_cases_spec: List[AuthTestCaseSpec] = [
            AuthTestCaseSpec(
                name=c["name"],
                transport_security=c.get("transport_security", "tls"),
                requires_compute_engine_creds=bool(
                    c.get("requires_compute_engine_creds", False)
                ),
                requires_compute_engine_creds_languages=list(
                    c.get("requires_compute_engine_creds_languages") or []
                ),
                uses_service_account_key=bool(
                    c.get("uses_service_account_key", False)
                ),
                uses_service_account_key_when_flag=c.get(
                    "uses_service_account_key_when_flag"
                ),
                uses_oauth_scope=bool(c.get("uses_oauth_scope", False)),
                uses_default_service_account=bool(
                    c.get("uses_default_service_account", False)
                ),
                uses_default_service_account_languages=list(
                    c.get("uses_default_service_account_languages") or []
                ),
            )
            for c in raw["test_cases"]["auth"]
        ]
        self.auth_test_cases: List[str] = [
            c.name for c in self.auth_test_cases_spec
        ]
        self.auth_test_case_map: Dict[str, AuthTestCaseSpec] = {
            c.name: c for c in self.auth_test_cases_spec
        }

        self.http2_client_test_cases_spec: List[Http2ClientTestCaseSpec] = [
            Http2ClientTestCaseSpec(name=c["name"])
            for c in raw["test_cases"]["http2_client"]
        ]
        self.http2_client_test_cases: List[str] = [
            c.name for c in self.http2_client_test_cases_spec
        ]

        self.http2_server_test_cases_spec: List[Http2ServerTestCaseSpec] = [
            Http2ServerTestCaseSpec(
                name=c["name"],
                client_kind=c.get("client_kind", "http2_interop"),
                grpc_client_test_case=c.get("grpc_client_test_case"),
                transport_security_override=c.get(
                    "transport_security_override"
                ),
            )
            for c in raw["test_cases"]["http2_server"]
        ]
        self.http2_server_test_cases: List[str] = [
            c.name for c in self.http2_server_test_cases_spec
        ]
        self.http2_server_test_case_map: Dict[str, Http2ServerTestCaseSpec] = {
            c.name: c for c in self.http2_server_test_cases_spec
        }

        self._builtin_groups: Dict[str, List[str]] = {
            "standard_test_cases": list(self.standard_test_cases),
            "standard_test_cases_except_first": list(
                self.standard_test_cases[1:]
            ),
            "auth_test_cases": list(self.auth_test_cases),
            "http2_client_test_cases": list(self.http2_client_test_cases),
            "http2_server_test_cases": list(self.http2_server_test_cases),
        }

        self.all_targets: Dict[str, LanguageSpec] = {}
        self.languages: Dict[str, LanguageSpec] = {}
        http2_target: Optional[LanguageSpec] = None
        for lang_name, ldata in raw["languages"].items():
            lang_spec = self._parse_language(lang_name, ldata)
            self.all_targets[lang_name] = lang_spec
            if lang_spec.is_http2_target:
                http2_target = lang_spec
            else:
                self.languages[lang_name] = lang_spec

        if http2_target is None:
            http2_target = next(iter(self.all_targets.values()))
        self.http2_client_target: LanguageSpec = http2_target
        self.http2_server_target: LanguageSpec = (
            http2_target.as_http2_server_target()
        )

        self.servers: List[str] = [
            name
            for name, lspec in self.languages.items()
            if lspec.supports_server
        ]

        self.grpc_client_test_cases_for_http2_server_test_cases: Dict[
            str, str
        ] = {
            c.name: c.grpc_client_test_case
            for c in self.http2_server_test_cases_spec
            if c.client_kind == "grpc_client" and c.grpc_client_test_case
        }
        self.http2_server_test_cases_that_use_grpc_clients: List[str] = list(
            self.grpc_client_test_cases_for_http2_server_test_cases.keys()
        )
        self.languages_with_http2_clients_for_http2_server_test_cases: List[
            str
        ] = list(self.suites["http2_server"].get("supported_http2_clients", []))
        self.languages_for_alts_test_cases: List[str] = list(
            self.transport_security["alts"].get("supported_clients", [])
        )
        self.servers_for_alts_test_cases: List[str] = list(
            self.transport_security["alts"].get("supported_servers", [])
        )
        self.transport_security_options: List[str] = list(
            self.transport_security.keys()
        )
        self.custom_credentials_type_options: List[str] = list(
            self.custom_credentials_types.keys()
        )

        self._parse_matrix(raw["matrix"])

    def expand_case_list(self, refs: Sequence[str]) -> List[str]:
        result: List[str] = []
        for ref in refs:
            if ref.startswith("@"):
                gname = ref[1:]
                if gname in self.test_case_groups:
                    result.extend(self.test_case_groups[gname])
                elif gname in self._builtin_groups:
                    result.extend(self._builtin_groups[gname])
                else:
                    raise SpecValidationError(f"Unknown group '@{gname}'")
            else:
                result.append(ref)
        return result

    def _parse_cmd(
        self, cmd_dict: Optional[Dict[str, Any]]
    ) -> Optional[CommandSpec]:
        if not cmd_dict:
            return None
        return CommandSpec(
            command=list(cmd_dict.get("command") or []),
            args_mode=cmd_dict.get("args_mode", "append"),
            arg_template=cmd_dict.get("arg_template"),
            port_regex=cmd_dict.get("port_regex"),
        )

    def _parse_language(
        self, name: str, ldata: Dict[str, Any]
    ) -> LanguageSpec:
        db_raw = ldata.get("docker_build")
        docker_build = None
        if db_raw:
            docker_build = DockerBuildSpec(
                base_name=db_raw["base_name"],
                current_version_file=db_raw["current_version_file"],
                dockerfile_dir=db_raw["dockerfile_dir"],
                build_script=db_raw["build_script"],
                repo_mount=db_raw.get("repo_mount", "grpc"),
                workdir=db_raw.get(
                    "workdir", self.defaults["docker_workdir_root"]
                ),
                env=dict(db_raw.get("env") or {}),
                build_commands=list(db_raw.get("build_commands") or []),
            )

        sdc_raw = ldata.get("server_docker_config")
        server_docker_config = None
        if sdc_raw:
            port_count = int(sdc_raw.get("port_count", 1))
            if sdc_raw.get("port_count_from_suite") == "http2_server":
                port_count = len(self.http2_server_test_cases)
            hc_raw = sdc_raw.get("healthcheck")
            hc = None
            if hc_raw:
                hc = HealthcheckSpec(
                    cmd_template=hc_raw["cmd_template"],
                    interval=hc_raw.get("interval", "1s"),
                    retries=int(hc_raw.get("retries", 5)),
                    timeout=hc_raw.get("timeout", "10s"),
                    wait_timeout_seconds=int(
                        hc_raw.get("wait_timeout_seconds", 600)
                    ),
                )
            server_docker_config = ServerDockerConfig(
                port_count_from_suite=sdc_raw.get("port_count_from_suite"),
                port_count=port_count,
                healthcheck=hc,
            )

        unimpl_client = self.expand_case_list(
            ldata.get("unimplemented_client_cases") or []
        )
        unimpl_server = self.expand_case_list(
            ldata.get("unimplemented_server_cases") or []
        )
        server_unimpl_client = None
        if "server_unimplemented_client_cases" in ldata:
            server_unimpl_client = self.expand_case_list(
                ldata.get("server_unimplemented_client_cases") or []
            )

        if "client_test_cases" in ldata:
            c_cases = list(ldata["client_test_cases"])
        elif ldata.get("is_http2_target"):
            c_cases = list(self.http2_client_test_cases)
        else:
            c_cases = [
                tc
                for tc in (self.standard_test_cases + self.auth_test_cases)
                if tc not in unimpl_client
            ]

        if "server_test_cases" in ldata:
            s_cases = list(ldata["server_test_cases"])
        elif ldata.get("is_http2_target"):
            s_cases = list(self.http2_server_test_cases)
        elif ldata.get("supports_server"):
            s_cases = [
                tc for tc in self.standard_test_cases if tc not in unimpl_server
            ]
        else:
            s_cases = []

        return LanguageSpec(
            name=name,
            safename=ldata["safename"],
            repo=ldata.get("repo", "grpc/grpc"),
            included_in_all_clients=bool(
                ldata.get("included_in_all_clients", True)
            ),
            supports_server=bool(ldata.get("supports_server", False)),
            docker_supported=bool(ldata.get("docker_supported", True)),
            auth_credentials_mode=ldata.get("auth_credentials_mode", "flag"),
            client_cwd=ldata.get("client_cwd"),
            server_cwd=ldata.get("server_cwd"),
            http2_cwd=ldata.get("http2_cwd"),
            global_env=dict(ldata.get("global_env") or {}),
            cloud_to_prod_env=dict(ldata.get("cloud_to_prod_env") or {}),
            client_cmd_spec=self._parse_cmd(ldata.get("client_cmd")),
            server_cmd_spec=self._parse_cmd(ldata.get("server_cmd")),
            http2_client_cmd_spec=self._parse_cmd(
                ldata.get("http2_client_cmd")
            ),
            unimplemented_client_cases=unimpl_client,
            unimplemented_server_cases=unimpl_server,
            client_test_cases=c_cases,
            server_test_cases=s_cases,
            docker_build=docker_build,
            server_docker_config=server_docker_config,
            is_http2_target=bool(ldata.get("is_http2_target", False)),
            server_unimplemented_client_cases=server_unimpl_client,
            legacy_factory=ldata.get("legacy_factory"),
            legacy_aliases=list(ldata.get("legacy_aliases") or []),
            server_legacy_factory=ldata.get("server_legacy_factory"),
            oauth2_uses_gce_creds=bool(ldata.get("oauth2_uses_gce_creds", False)),
            skip_http2_bad_client=bool(ldata.get("skip_http2_bad_client", False)),
            oci_labels=ldata.get("oci_labels"),
            container_entrypoints=ldata.get("container_entrypoints"),
            client_capabilities=ldata.get("client_capabilities"),
            server_capabilities=ldata.get("server_capabilities"),
        )

    def _parse_matrix(self, mraw: Dict[str, Any]) -> None:
        self.matrix_config = mraw
        self.lang_runtime_matrix: Dict[str, List[str]] = {}
        self.lang_release_matrix: Dict[str, "OrderedDict[str, ReleaseInfo]"] = (
            {}
        )
        self.matrix_repos: Dict[str, str] = dict(
            mraw.get("extra_repos") or {}
        )
        self.matrix_build_from_release_tag: Dict[str, bool] = {}
        self.matrix_root_env_var: Dict[str, str] = {}

        for mlang, mdata in mraw.get("languages", {}).items():
            self.lang_runtime_matrix[mlang] = list(
                mdata.get("default_runtimes") or []
            )
            self.matrix_repos[mlang] = mdata.get(
                "repo", "https://github.com/grpc/grpc.git"
            )
            self.matrix_build_from_release_tag[mlang] = bool(
                mdata.get("build_from_release_tag", True)
            )
            self.matrix_root_env_var[mlang] = mdata.get(
                "root_env_var", "GRPC_ROOT"
            )
            rel_odict: "OrderedDict[str, ReleaseInfo]" = OrderedDict()
            for rtag, rinfo in (mdata.get("releases") or {}).items():
                rel_odict[rtag] = ReleaseInfo(
                    patch=rinfo.get("patch") or [],
                    runtimes=rinfo.get("runtimes") or [],
                    testcases_file=rinfo.get("testcases_file"),
                )
            self.lang_release_matrix[mlang] = rel_odict

        self.matrix_runtimes: Dict[str, DockerBuildSpec] = {}
        for rname, rdata in (mraw.get("runtimes") or {}).items():
            self.matrix_runtimes[rname] = DockerBuildSpec(
                base_name=rdata["base_name"],
                current_version_file=rdata["current_version_file"],
                dockerfile_dir=rdata["dockerfile_dir"],
                build_script=rdata["build_script"],
            )

        self.matrix_testcase_profiles: Dict[str, MatrixTestcaseProfile] = {}
        for pname, pdata in (mraw.get("testcase_profiles") or {}).items():
            self.matrix_testcase_profiles[pname] = MatrixTestcaseProfile(
                name=pname,
                default_image=pdata.get("default_image"),
                workdir=pdata["workdir"],
                inner_command_template=pdata["inner_command_template"],
                env=dict(pdata.get("env") or {}),
                bash_login=bool(pdata.get("bash_login", False)),
                container_names=list(pdata.get("container_names") or []),
            )

    @classmethod
    def load(cls, path: str = DEFAULT_CONFIG_PATH) -> "InteropSpec":
        return load_spec(path)

    def get_language(self, name: str) -> LanguageSpec:
        return self.all_targets[name]

    def get_matrix_testcase_commands(self, profile_name: str) -> List[str]:
        profile = self.matrix_testcase_profiles[profile_name]
        servers = self.matrix_config.get(
            "servers",
            [
                "grpc-test.sandbox.googleapis.com",
                "grpc-test4.sandbox.googleapis.com",
            ],
        )
        test_cases = self.matrix_config.get(
            "test_cases",
            [
                tc.name
                for tc in self.standard_test_cases_spec
                if tc.cloud_to_prod_eligible
            ],
        )
        return profile.render_commands(servers, test_cases)

    def is_client_supported_for_transport_security(
        self, language_name: str, transport_security: str
    ) -> bool:
        sec = self.transport_security.get(transport_security)
        if not sec:
            return False
        allowed = sec.get("supported_clients", "all")
        return allowed == "all" or str(language_name) in allowed

    def is_server_supported_for_transport_security(
        self, server_name: str, transport_security: str
    ) -> bool:
        sec = self.transport_security.get(transport_security)
        if not sec:
            return False
        allowed = sec.get("supported_servers", "all")
        return allowed == "all" or str(server_name) in allowed

    def is_client_supported_for_custom_credentials(
        self, language_name: str, custom_credentials_type: str
    ) -> bool:
        cred = self.custom_credentials_types.get(custom_credentials_type)
        if not cred:
            return False
        allowed = cred.get("supported_clients", "all")
        return allowed == "all" or str(language_name) in allowed

    def explain_cloud_to_cloud_matrix(
        self,
        client_langs: Sequence[str],
        server_langs: Sequence[str],
        transport_security: str = "tls",
    ) -> List[Dict[str, Any]]:
        records = []
        for s_name in server_langs:
            s_spec = self.languages.get(s_name)
            if not self.is_server_supported_for_transport_security(
                s_name, transport_security
            ):
                records.append(
                    {
                        "suite": "cloud_to_cloud",
                        "client": "*",
                        "server": s_name,
                        "test_case": "*",
                        "status": "SKIPPED",
                        "reason": f"server '{s_name}' does not support transport_security='{transport_security}'",
                    }
                )
                continue
            skip_server = (
                s_spec.unimplemented_test_cases_server() if s_spec else []
            )
            for c_name in client_langs:
                c_spec = self.languages[c_name]
                if not self.is_client_supported_for_transport_security(
                    c_name, transport_security
                ):
                    records.append(
                        {
                            "suite": "cloud_to_cloud",
                            "client": c_name,
                            "server": s_name,
                            "test_case": "*",
                            "status": "SKIPPED",
                            "reason": f"client '{c_name}' does not support transport_security='{transport_security}'",
                        }
                    )
                    continue
                skip_client = c_spec.unimplemented_test_cases()
                for tc in self.standard_test_cases:
                    if tc in skip_client:
                        records.append(
                            {
                                "suite": "cloud_to_cloud",
                                "client": c_name,
                                "server": s_name,
                                "test_case": tc,
                                "status": "SKIPPED",
                                "reason": f"unimplemented by client '{c_name}'",
                            }
                        )
                    elif tc in skip_server:
                        records.append(
                            {
                                "suite": "cloud_to_cloud",
                                "client": c_name,
                                "server": s_name,
                                "test_case": tc,
                                "status": "SKIPPED",
                                "reason": f"unimplemented by server '{s_name}'",
                            }
                        )
                    else:
                        records.append(
                            {
                                "suite": "cloud_to_cloud",
                                "client": c_name,
                                "server": s_name,
                                "test_case": tc,
                                "status": "INCLUDED",
                                "reason": "supported by both client and server",
                            }
                        )
        return records


_CACHED_SPEC: Optional[InteropSpec] = None


def load_spec(path: str = DEFAULT_CONFIG_PATH) -> InteropSpec:
    """Loads and validates InteropSpec from tools/interop/config.json."""
    with open(path, "r") as f:
        raw = json.load(f)
    return InteropSpec(raw, spec_path=path)


def get_spec() -> InteropSpec:
    """Returns the cached singleton InteropSpec."""
    global _CACHED_SPEC
    if _CACHED_SPEC is None:
        _CACHED_SPEC = load_spec(DEFAULT_CONFIG_PATH)
    return _CACHED_SPEC


# ============================================================================
# Docker Image Builder (Deduplicating by safename)
# ============================================================================


class DockerImageBuilder:
    """Builds gRPC interop Docker images from declarative specifications."""

    def __init__(
        self, spec: Optional[InteropSpec] = None, repo_root: str = REPO_ROOT
    ):
        self.spec = spec or get_spec()
        self.repo_root = repo_root

    def resolve_base_image(self, build_spec: DockerBuildSpec) -> str:
        version_path = os.path.join(
            self.repo_root, build_spec.current_version_file
        )
        if os.path.exists(version_path):
            with open(version_path, "r") as f:
                content = f.read().strip()
            if content:
                return content
        registry = self.spec.matrix_config.get(
            "default_docker_path",
            "us-docker.pkg.dev/grpc-testing/testing-images-public",
        )
        return f"{registry}/{build_spec.base_name}:latest"

    def resolve_build_spec(
        self, target: Union[str, LanguageSpec, DockerBuildSpec]
    ) -> DockerBuildSpec:
        if isinstance(target, DockerBuildSpec):
            return target
        if isinstance(target, LanguageSpec):
            if target.docker_build:
                return target.docker_build
            target = target.safename
        target_str = str(target)
        if (
            target_str in self.spec.all_targets
            and self.spec.all_targets[target_str].docker_build
        ):
            return self.spec.all_targets[target_str].docker_build
        if target_str in self.spec.matrix_runtimes:
            return self.spec.matrix_runtimes[target_str]
        for lspec in self.spec.all_targets.values():
            if lspec.safename == target_str and lspec.docker_build:
                return lspec.docker_build
        raise ValueError(
            f"No DockerBuildSpec found for target/runtime '{target_str}'"
        )

    def generate_dockerfile(
        self,
        target: Union[str, LanguageSpec, DockerBuildSpec],
        release: Optional[str] = None,
        include_commit_log: bool = False,
        use_build_script: bool = False,
    ) -> str:
        build_spec = self.resolve_build_spec(target)
        base_image = self.resolve_base_image(build_spec)
        lines = [
            "# Auto-generated by tools.interop.harness.DockerImageBuilder",
            f"FROM {base_image}",
        ]
        if release and release != "master":
            lines.append(f"LABEL release={release}")
        for env_key, env_val in build_spec.env.items():
            lines.append(f"ENV {env_key}={env_val}")
        lines.append(f"COPY . {build_spec.workdir}")
        lines.append(f"WORKDIR {build_spec.workdir}")
        if use_build_script or not build_spec.build_commands:
            lines.append(
                f"RUN bash -l /var/local/jenkins/grpc/{build_spec.build_script}"
            )
        else:
            for cmd in build_spec.build_commands:
                lines.append(f"RUN {cmd}")
        if include_commit_log:
            build_info_dir = self.spec.matrix_config.get(
                "build_info_dir", "/var/local/build_info"
            )
            lines.append(f"COPY commit_log {build_info_dir}/")
        return "\n".join(lines) + "\n"

    def create_direct_build_jobspec(
        self,
        target: Union[str, LanguageSpec],
        tag: Optional[str] = None,
        release: Optional[str] = None,
        commit_log_path: Optional[str] = None,
        context_dir: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> jobset.JobSpec:
        build_spec = self.resolve_build_spec(target)
        target_name = str(target)
        safename = (
            target.safename
            if isinstance(target, LanguageSpec)
            else build_spec.base_name.replace("grpc_interop_", "")
        )
        if not tag:
            tag = f"grpc_interop_{safename}:{uuid.uuid4()}"

        include_commit_log = bool(
            commit_log_path and os.path.exists(commit_log_path)
        )
        dockerfile_content = self.generate_dockerfile(
            build_spec,
            release=release,
            include_commit_log=include_commit_log,
        )

        temp_dir = tempfile.mkdtemp(prefix=f"grpc_interop_build_{safename}_")
        atexit.register(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        dockerfile_path = os.path.join(temp_dir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile_content)

        if include_commit_log and commit_log_path:
            shutil.copy(commit_log_path, os.path.join(temp_dir, "commit_log"))

        build_context = context_dir or temp_dir
        cmdline = [
            "docker",
            "build",
            "--rm",
            "--tag",
            tag,
            "-f",
            dockerfile_path,
            build_context,
        ]
        timeout = timeout_seconds or int(
            self.spec.defaults.get("build_image_timeout_seconds", 45 * 60)
        )
        job = jobset.JobSpec(
            cmdline=cmdline,
            environ={"INTEROP_IMAGE": tag, "BASE_NAME": build_spec.base_name},
            shortname=f"build_docker_{target_name}",
            timeout_seconds=timeout,
        )
        job.tag = tag
        job.dockerfile_content = dockerfile_content
        return job

    def build_interop_image_jobspec(
        self,
        language: Union[str, LanguageSpec],
        tag: Optional[str] = None,
        direct_build: bool = False,
    ) -> jobset.JobSpec:
        lang_spec = (
            language
            if isinstance(language, LanguageSpec)
            else self.spec.all_targets[str(language)]
        )
        if direct_build:
            return self.create_direct_build_jobspec(lang_spec, tag=tag)

        if not tag:
            tag = "grpc_interop_%s:%s" % (lang_spec.safename, uuid.uuid4())
        env = {
            "INTEROP_IMAGE": tag,
            "BASE_NAME": "grpc_interop_%s" % lang_spec.safename,
        }
        builder_script = self.spec.defaults.get(
            "legacy_image_builder_script",
            "tools/run_tests/dockerize/build_interop_image.sh",
        )
        timeout = int(
            self.spec.defaults.get("build_image_timeout_seconds", 45 * 60)
        )
        build_job = jobset.JobSpec(
            cmdline=[builder_script],
            environ=env,
            shortname="build_docker_%s" % lang_spec,
            timeout_seconds=timeout,
        )
        build_job.tag = tag
        return build_job

    def plan_image_builds(
        self,
        languages: Iterable[Union[str, LanguageSpec]],
        servers: Iterable[str] = (),
        http2_clients: Iterable[Union[str, LanguageSpec]] = (),
        http2_interop: bool = False,
        http2_server_interop: bool = False,
        deduplicate: bool = True,
        direct_build: bool = False,
    ) -> Tuple[List[jobset.JobSpec], Dict[str, str]]:
        requested_targets: List[LanguageSpec] = []
        seen_names = set()

        for l in languages:
            lspec = (
                l
                if isinstance(l, LanguageSpec)
                else self.spec.all_targets[str(l)]
            )
            if lspec.name not in seen_names:
                requested_targets.append(lspec)
                seen_names.add(lspec.name)

        for s in servers:
            s_str = str(s)
            if s_str in self.spec.all_targets and s_str not in seen_names:
                requested_targets.append(self.spec.all_targets[s_str])
                seen_names.add(s_str)

        for hc in http2_clients:
            lspec = (
                hc
                if isinstance(hc, LanguageSpec)
                else self.spec.all_targets[str(hc)]
            )
            if lspec.name not in seen_names:
                requested_targets.append(lspec)
                seen_names.add(lspec.name)

        if http2_interop:
            requested_targets.append(self.spec.http2_client_target)
        if http2_server_interop:
            requested_targets.append(self.spec.http2_server_target)

        requested_targets.sort(
            key=lambda t: (t.safename, 0 if t.name == t.safename else 1, t.name)
        )

        build_jobs: List[jobset.JobSpec] = []
        docker_images: Dict[str, str] = {}
        safename_to_tag: Dict[str, str] = {}

        for target in requested_targets:
            if not target.docker_supported:
                continue
            if deduplicate and target.safename in safename_to_tag:
                docker_images[str(target)] = safename_to_tag[target.safename]
                continue

            job = self.build_interop_image_jobspec(
                target, direct_build=direct_build
            )
            build_jobs.append(job)
            safename_to_tag[target.safename] = job.tag
            docker_images[str(target)] = job.tag

        return build_jobs, docker_images


def build_interop_image_jobspec(
    language: Union[str, LanguageSpec],
    tag: Optional[str] = None,
    direct_build: bool = False,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    builder = DockerImageBuilder(spec or get_spec())
    return builder.build_interop_image_jobspec(
        language, tag=tag, direct_build=direct_build
    )


def add_files_to_image(
    image: str,
    files: Sequence[str],
    label: Optional[str] = None,
    build_info_dir: str = "/var/local/build_info",
) -> None:
    temp_dir = tempfile.mkdtemp()
    atexit.register(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
    for file_path in files:
        shutil.copy(file_path, temp_dir)
    dockerfile_lines = [
        f"FROM {image}",
        f"COPY . {build_info_dir}/",
    ]
    if label:
        dockerfile_lines.append(f"LABEL {label}")
    with open(os.path.join(temp_dir, "Dockerfile"), "w") as f:
        f.write("\n".join(dockerfile_lines) + "\n")
    repackage_cmd = ["docker", "build", "--rm", "--tag", image, temp_dir]
    subprocess.check_output(repackage_cmd, stderr=subprocess.STDOUT)


# ============================================================================
# JobSpec Builders & InteropRunner
# ============================================================================

_DEFAULT_ALLOW_FLAKES = False


def set_default_allow_flakes(allow_flakes: bool) -> None:
    global _DEFAULT_ALLOW_FLAKES
    _DEFAULT_ALLOW_FLAKES = bool(allow_flakes)


def docker_run_cmdline(
    cmdline: Sequence[str],
    image: str,
    docker_args: Sequence[str] = (),
    cwd: Optional[str] = None,
    environ: Optional[Dict[str, str]] = None,
    workdir_root: Optional[str] = None,
) -> List[str]:
    spec = get_spec()
    root_dir = workdir_root or spec.defaults["docker_workdir_root"]
    docker_cmdline = ["docker", "run", "-i", "--rm=true"]
    if environ:
        for k, v in list(environ.items()):
            docker_cmdline += ["-e", "%s=%s" % (k, v)]
    workdir = root_dir
    if cwd:
        workdir = os.path.join(workdir, cwd)
    docker_cmdline += ["-w", workdir]
    docker_cmdline += list(docker_args) + [image] + list(cmdline)
    return docker_cmdline


def manual_cmdline(docker_cmdline: Sequence[str], docker_image: str) -> str:
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
    return ["bash", "-c", " ".join(cmdline)]


def compute_engine_creds_required(
    language: Union[str, LanguageSpec],
    test_case: str,
    spec: Optional[InteropSpec] = None,
) -> bool:
    active_spec = spec or get_spec()
    auth_spec = active_spec.auth_test_case_map.get(test_case)
    if not auth_spec:
        return False
    lang_str = str(language)
    lang_spec = (
        language
        if isinstance(language, LanguageSpec)
        else active_spec.all_targets.get(lang_str)
    )
    return auth_spec.needs_compute_engine_creds(lang_str, lang_spec=lang_spec)


def auth_options(
    language: Union[str, LanguageSpec],
    test_case: str,
    google_default_creds_use_key_file: bool,
    service_account_key_file: Optional[str],
    default_service_account: Optional[str],
    spec: Optional[InteropSpec] = None,
) -> Tuple[List[str], Dict[str, str]]:
    active_spec = spec or get_spec()
    lang_str = str(language)
    lang_spec = (
        language
        if isinstance(language, LanguageSpec)
        else active_spec.all_targets.get(lang_str)
    )
    auth_spec = active_spec.auth_test_case_map.get(test_case)

    cmdargs: List[str] = []
    env: Dict[str, str] = {}
    if not auth_spec:
        return (cmdargs, env)

    oauth_scope_arg = (
        "--oauth_scope=%s" % active_spec.defaults["oauth_scope"]
    )
    key_file_arg = "--service_account_key_file=%s" % service_account_key_file
    default_account_arg = (
        "--default_service_account=%s" % default_service_account
    )

    if auth_spec.uses_service_account_key:
        cred_mode = (
            lang_spec.auth_credentials_mode if lang_spec else "flag"
        )
        if cred_mode == "env":
            env["GOOGLE_APPLICATION_CREDENTIALS"] = str(
                service_account_key_file
            )
        else:
            cmdargs += [key_file_arg]

    if auth_spec.uses_oauth_scope:
        cmdargs += [oauth_scope_arg]

    if (
        auth_spec.uses_service_account_key_when_flag
        == "google_default_creds_use_key_file"
        and google_default_creds_use_key_file
    ):
        env["GOOGLE_APPLICATION_CREDENTIALS"] = str(service_account_key_file)

    if auth_spec.needs_default_service_account(lang_str, lang_spec=lang_spec):
        cmdargs += [default_account_arg]

    return (cmdargs, env)


def _job_kill_handler(job) -> None:
    if getattr(job._spec, "container_name", None):
        dockerjob.docker_kill(job._spec.container_name)


def cloud_to_prod_jobspec(
    language: LanguageSpec,
    test_case: str,
    server_host_nickname: str,
    server_host: str,
    google_default_creds_use_key_file: bool,
    docker_image: Optional[str] = None,
    auth: bool = False,
    manual_cmd_log: Optional[List[str]] = None,
    service_account_key_file: Optional[str] = None,
    default_service_account: Optional[str] = None,
    transport_security: str = "tls",
    allow_flakes: Optional[bool] = None,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    active_spec = spec or get_spec()
    use_flakes = (
        _DEFAULT_ALLOW_FLAKES if allow_flakes is None else bool(allow_flakes)
    )
    container_name = None
    prod_port = active_spec.defaults.get("prod_server_port", 443)
    cmdargs = [
        "--server_host=%s" % server_host,
        "--server_port=%s" % prod_port,
        "--test_case=%s" % test_case,
    ]

    cred_spec = active_spec.custom_credentials_types.get(transport_security)
    if cred_spec and active_spec.is_client_supported_for_custom_credentials(
        str(language), transport_security
    ):
        transport_security_options = list(cred_spec["client_flags"])
    else:
        print(
            "Invalid transport security option %s in cloud_to_prod_jobspec."
            " Lang: %s" % (str(language), transport_security)
        )
        sys.exit(1)

    cmdargs = cmdargs + transport_security_options
    environ = dict(language.cloud_to_prod_env(), **language.global_env())
    if auth:
        auth_cmdargs, auth_env = auth_options(
            language,
            test_case,
            google_default_creds_use_key_file,
            service_account_key_file,
            default_service_account,
            spec=active_spec,
        )
        cmdargs += auth_cmdargs
        environ.update(auth_env)

    cmdline = bash_cmdline(language.client_cmd(cmdargs))
    cwd = language.client_cwd

    if docker_image:
        container_name = dockerjob.random_name(
            "interop_client_%s" % language.safename
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
    test_timeout = int(active_spec.defaults.get("test_timeout_seconds", 180))
    flake_retries = (
        int(active_spec.defaults.get("flake_retries", 4)) if use_flakes else 0
    )
    timeout_retries = (
        int(active_spec.defaults.get("timeout_retries", 2))
        if use_flakes
        else 0
    )
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="%s:%s:%s:%s:%s"
        % (
            suite_name,
            language,
            server_host_nickname,
            test_case,
            transport_security,
        ),
        timeout_seconds=test_timeout,
        flake_retries=flake_retries,
        timeout_retries=timeout_retries,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
        test_job.container_name = container_name
    return test_job


def cloud_to_cloud_jobspec(
    language: LanguageSpec,
    test_case: str,
    server_name: str,
    server_host: str,
    server_port: Union[int, str],
    docker_image: Optional[str] = None,
    transport_security: str = "tls",
    manual_cmd_log: Optional[List[str]] = None,
    allow_flakes: Optional[bool] = None,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    active_spec = spec or get_spec()
    use_flakes = (
        _DEFAULT_ALLOW_FLAKES if allow_flakes is None else bool(allow_flakes)
    )
    interop_only_options = list(
        active_spec.suites["cloud_to_cloud"].get(
            "client_extra_flags",
            [
                "--server_host_override=foo.test.google.fr",
                "--use_test_ca=true",
            ],
        )
    )
    sec_spec = active_spec.transport_security.get(transport_security)
    if sec_spec:
        interop_only_options += list(sec_spec["client_flags"])
    else:
        print(
            "Invalid transport security option %s in cloud_to_cloud_jobspec."
            % transport_security
        )
        sys.exit(1)

    client_test_case = test_case
    if (
        test_case
        in active_spec.http2_server_test_cases_that_use_grpc_clients
    ):
        client_test_case = (
            active_spec.grpc_client_test_cases_for_http2_server_test_cases[
                test_case
            ]
        )
    if client_test_case in language.unimplemented_test_cases():
        print(
            "asking client %s to run unimplemented test case %s"
            % (repr(language), client_test_case)
        )
        sys.exit(1)

    std_case = active_spec.standard_test_case_map.get(test_case)
    if std_case and std_case.extra_cloud_to_cloud_flags:
        interop_only_options += list(std_case.extra_cloud_to_cloud_flags)

    common_options = [
        "--test_case=%s" % client_test_case,
        "--server_host=%s" % server_host,
        "--server_port=%s" % server_port,
    ]

    if test_case in active_spec.http2_server_test_cases:
        if (
            test_case
            in active_spec.http2_server_test_cases_that_use_grpc_clients
        ):
            client_options = interop_only_options + common_options
            cmdline = bash_cmdline(language.client_cmd(client_options))
            cwd = language.client_cwd
        else:
            cmdline = bash_cmdline(
                language.client_cmd_http2interop(common_options)
            )
            cwd = language.http2_cwd
    else:
        cmdline = bash_cmdline(
            language.client_cmd(common_options + interop_only_options)
        )
        cwd = language.client_cwd

    environ = language.global_env()
    docker_supported = getattr(language, "docker_supported", True)
    container_name = None
    if docker_image and docker_supported:
        container_name = dockerjob.random_name(
            "interop_client_%s" % language.safename
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

    test_timeout = int(active_spec.defaults.get("test_timeout_seconds", 180))
    flake_retries = (
        int(active_spec.defaults.get("flake_retries", 4)) if use_flakes else 0
    )
    timeout_retries = (
        int(active_spec.defaults.get("timeout_retries", 2))
        if use_flakes
        else 0
    )
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="cloud_to_cloud:%s:%s_server:%s:%s"
        % (language, server_name, test_case, transport_security),
        timeout_seconds=test_timeout,
        flake_retries=flake_retries,
        timeout_retries=timeout_retries,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
        test_job.container_name = container_name
    return test_job


def server_jobspec(
    language: LanguageSpec,
    docker_image: str,
    transport_security: str = "tls",
    manual_cmd_log: Optional[List[str]] = None,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    active_spec = spec or get_spec()
    default_port = int(active_spec.defaults.get("default_server_port", 8080))
    container_name = dockerjob.random_name(
        "interop_server_%s" % language.safename
    )
    server_cmd = ["--port=%s" % default_port]
    sec_spec = active_spec.transport_security.get(transport_security)
    if sec_spec:
        server_cmd += list(sec_spec["server_flags"])
    else:
        print(
            "Invalid transport security option %s in server_jobspec."
            % transport_security
        )
        sys.exit(1)

    cmdline = bash_cmdline(language.server_cmd(server_cmd))
    environ = language.global_env()
    docker_args = ["--name=%s" % container_name]

    sdc = getattr(language, "server_docker_config", None)
    if sdc is None and getattr(language, "is_http2_target", False):
        sdc = active_spec.http2_server_target.server_docker_config

    if sdc and sdc.port_count > 1:
        docker_args += list(
            itertools.chain.from_iterable(
                ("-p", str(default_port + i)) for i in range(sdc.port_count)
            )
        )
        if sdc.healthcheck:
            hc = sdc.healthcheck
            docker_args += [
                "--health-cmd=%s"
                % hc.cmd_template.format(port=default_port),
                f"--health-interval={hc.interval}",
                f"--health-retries={hc.retries}",
                f"--health-timeout={hc.timeout}",
            ]
    else:
        docker_args += ["-p", str(default_port)]

    docker_cmdline = docker_run_cmdline(
        cmdline,
        image=docker_image,
        cwd=language.server_cwd,
        environ=environ,
        docker_args=docker_args,
    )
    if manual_cmd_log is not None:
        if manual_cmd_log == []:
            manual_cmd_log.append(
                'echo "Testing ${docker_image:=%s}"' % docker_image
            )
        manual_cmd_log.append(manual_cmdline(docker_cmdline, docker_image))

    server_timeout = int(
        active_spec.defaults.get("server_timeout_seconds", 30 * 60)
    )
    server_job = jobset.JobSpec(
        cmdline=docker_cmdline,
        environ=environ,
        shortname="interop_server_%s" % language,
        timeout_seconds=server_timeout,
    )
    server_job.container_name = container_name
    return server_job


def aggregate_http2_results(stdout: str) -> Optional[Dict[str, Any]]:
    match = re.search(r'\{"cases[^\]]*\]\}', stdout)
    if not match:
        return None

    results = json.loads(match.group(0))
    skipped = 0
    passed = 0
    failed = 0
    failed_cases = []
    for case in results["cases"]:
        if case.get("skipped", False):
            skipped += 1
        else:
            if case.get("passed", False):
                passed += 1
            else:
                failed += 1
                failed_cases.append(case.get("name", "NONAME"))
    return {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "failed_cases": ", ".join(failed_cases),
        "percent": 1.0 * passed / (passed + failed),
    }


@dataclass
class ExecutionPlan:
    """Complete plan of Docker builds, server jobs, client jobs, and skip explanations."""

    languages: List[LanguageSpec] = field(default_factory=list)
    servers: List[str] = field(default_factory=list)
    http2_clients: List[LanguageSpec] = field(default_factory=list)
    build_jobs: List[jobset.JobSpec] = field(default_factory=list)
    docker_images: Dict[str, str] = field(default_factory=dict)
    server_specs: Dict[str, jobset.JobSpec] = field(default_factory=dict)
    client_jobs: List[jobset.JobSpec] = field(default_factory=list)
    skipped_tests: List[Dict[str, str]] = field(default_factory=list)
    server_manual_cmd_log: Optional[List[str]] = None
    client_manual_cmd_log: Optional[List[str]] = None


class InteropRunner:
    """Executes or dry-runs interop test suites from InteropSpec."""

    def __init__(self, spec: Optional[InteropSpec] = None):
        self.spec = spec or get_spec()
        self.builder = DockerImageBuilder(self.spec)

    def cloud_to_prod_jobspec(self, *args, **kwargs) -> jobset.JobSpec:
        kwargs.setdefault("spec", self.spec)
        return cloud_to_prod_jobspec(*args, **kwargs)

    def cloud_to_cloud_jobspec(self, *args, **kwargs) -> jobset.JobSpec:
        kwargs.setdefault("spec", self.spec)
        return cloud_to_cloud_jobspec(*args, **kwargs)

    def server_jobspec(self, *args, **kwargs) -> jobset.JobSpec:
        kwargs.setdefault("spec", self.spec)
        return server_jobspec(*args, **kwargs)

    def auth_options(self, *args, **kwargs) -> Tuple[List[str], Dict[str, str]]:
        kwargs.setdefault("spec", self.spec)
        return auth_options(*args, **kwargs)

    def compute_engine_creds_required(self, *args, **kwargs) -> bool:
        kwargs.setdefault("spec", self.spec)
        return compute_engine_creds_required(*args, **kwargs)

    def resolve_languages_and_servers(
        self, args
    ) -> Tuple[List[LanguageSpec], List[str], List[LanguageSpec]]:
        raw_servers = set(
            s
            for s in itertools.chain.from_iterable(
                self.spec.servers if x == "all" else [x] for x in args.server
            )
        )
        # Filter out idle servers with 0 active server test cases (e.g. pythonasyncio)
        servers = set(
            s
            for s in raw_servers
            if s in self.spec.languages
            and self.spec.languages[s].has_active_server_cases()
        )
        if args.transport_security == "alts":
            servers = servers.intersection(
                self.spec.servers_for_alts_test_cases
            )

        all_included = [
            name
            for name, lspec in self.spec.languages.items()
            if lspec.included_in_all_clients
        ]
        languages = set(
            self.spec.languages[l]
            for l in itertools.chain.from_iterable(
                all_included if x == "all" else [x] for x in args.language
            )
        )
        if args.transport_security == "alts":
            alts_languages = set(
                self.spec.languages[l]
                for l in self.spec.languages_for_alts_test_cases
            )
            languages = languages.intersection(alts_languages)

        http2_clients = set()
        if args.http2_server_interop:
            http2_clients = set(
                self.spec.languages[l]
                for l in self.spec.languages_with_http2_clients_for_http2_server_test_cases
                if "all" in args.language or l in args.language
            )

        return (
            sorted(languages, key=str),
            sorted(servers),
            sorted(http2_clients, key=str),
        )

    def build_plan(
        self,
        args,
        server_addresses_override: Optional[
            Dict[str, Tuple[str, Union[int, str]]]
        ] = None,
        http2_port_mapper=None,
        direct_build: bool = False,
        docker_images_override: Optional[Dict[str, str]] = None,
    ) -> ExecutionPlan:
        set_default_allow_flakes(getattr(args, "allow_flakes", False))
        languages, servers, http2_clients = self.resolve_languages_and_servers(
            args
        )

        build_jobs: List[jobset.JobSpec] = []
        docker_images: Dict[str, str] = {}
        if docker_images_override is not None:
            docker_images = dict(docker_images_override)
        elif args.use_docker:
            build_jobs, docker_images = self.builder.plan_image_builds(
                languages=languages,
                servers=servers,
                http2_clients=http2_clients,
                http2_interop=args.http2_interop,
                http2_server_interop=args.http2_server_interop,
                deduplicate=True,
                direct_build=direct_build,
            )

        server_manual_cmd_log = [] if args.manual_run else None
        client_manual_cmd_log = [] if args.manual_run else None
        server_specs: Dict[str, jobset.JobSpec] = {}
        server_addresses: Dict[str, Tuple[str, Union[int, str]]] = {}

        default_port = int(
            self.spec.defaults.get("default_server_port", 8080)
        )
        for s in servers:
            lang = str(s)
            spec_job = server_jobspec(
                self.spec.languages[lang],
                docker_images.get(lang),
                args.transport_security,
                manual_cmd_log=server_manual_cmd_log,
                spec=self.spec,
            )
            server_specs[lang] = spec_job
            if server_addresses_override and lang in server_addresses_override:
                server_addresses[lang] = server_addresses_override[lang]
            elif args.manual_run:
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")
            else:
                server_addresses[lang] = ("localhost", default_port)

        if args.http2_server_interop:
            http2_srv = self.spec.http2_server_target
            lang = str(http2_srv)
            spec_job = server_jobspec(
                http2_srv,
                docker_images.get(lang),
                manual_cmd_log=server_manual_cmd_log,
                spec=self.spec,
            )
            server_specs[lang] = spec_job
            if args.manual_run:
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")

        jobs: List[jobset.JobSpec] = []
        skipped: List[Dict[str, str]] = []

        if args.cloud_to_prod:
            if args.transport_security not in ["tls"]:
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                for language in languages:
                    unimpl = language.unimplemented_test_cases()
                    for tc_spec in self.spec.standard_test_cases_spec:
                        test_case = tc_spec.name
                        if test_case in unimpl:
                            skipped.append(
                                {
                                    "suite": "cloud_to_prod",
                                    "client": str(language),
                                    "server": server_host_nickname,
                                    "test_case": test_case,
                                    "reason": f"unimplemented by client '{language}'",
                                }
                            )
                            continue
                        if not tc_spec.cloud_to_prod_eligible:
                            skipped.append(
                                {
                                    "suite": "cloud_to_prod",
                                    "client": str(language),
                                    "server": server_host_nickname,
                                    "test_case": test_case,
                                    "reason": "not cloud_to_prod_eligible",
                                }
                            )
                            continue
                        for (
                            transport_security
                        ) in args.custom_credentials_type:
                            if not self.spec.is_client_supported_for_custom_credentials(
                                str(language), transport_security
                            ):
                                skipped.append(
                                    {
                                        "suite": "cloud_to_prod",
                                        "client": str(language),
                                        "server": server_host_nickname,
                                        "test_case": f"{test_case}:{transport_security}",
                                        "reason": f"client '{language}' does not support custom_credentials_type '{transport_security}'",
                                    }
                                )
                                continue
                            test_job = cloud_to_prod_jobspec(
                                language,
                                test_case,
                                server_host_nickname,
                                self.spec.prod_servers[server_host_nickname],
                                google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                docker_image=docker_images.get(str(language)),
                                manual_cmd_log=client_manual_cmd_log,
                                service_account_key_file=args.service_account_key_file,
                                default_service_account=args.default_service_account,
                                transport_security=transport_security,
                                allow_flakes=args.allow_flakes,
                                spec=self.spec,
                            )
                            jobs.append(test_job)
                if args.http2_interop:
                    http2_client = self.spec.http2_client_target
                    for test_case in self.spec.http2_client_test_cases:
                        test_job = cloud_to_prod_jobspec(
                            http2_client,
                            test_case,
                            server_host_nickname,
                            self.spec.prod_servers[server_host_nickname],
                            google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                            docker_image=docker_images.get(str(http2_client)),
                            manual_cmd_log=client_manual_cmd_log,
                            service_account_key_file=args.service_account_key_file,
                            default_service_account=args.default_service_account,
                            transport_security=args.transport_security,
                            allow_flakes=args.allow_flakes,
                            spec=self.spec,
                        )
                        jobs.append(test_job)

        if args.cloud_to_prod_auth:
            if args.transport_security not in ["tls"]:
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                for language in languages:
                    unimpl = language.unimplemented_test_cases()
                    for auth_spec in self.spec.auth_test_cases_spec:
                        test_case = auth_spec.name
                        if (
                            args.skip_compute_engine_creds
                            and compute_engine_creds_required(
                                language, test_case, spec=self.spec
                            )
                        ):
                            skipped.append(
                                {
                                    "suite": "cloud_to_prod_auth",
                                    "client": str(language),
                                    "server": server_host_nickname,
                                    "test_case": test_case,
                                    "reason": "skipped due to --skip_compute_engine_creds",
                                }
                            )
                            continue
                        if test_case in unimpl:
                            skipped.append(
                                {
                                    "suite": "cloud_to_prod_auth",
                                    "client": str(language),
                                    "server": server_host_nickname,
                                    "test_case": test_case,
                                    "reason": f"unimplemented by client '{language}'",
                                }
                            )
                            continue
                        transport_security = auth_spec.transport_security
                        if (
                            transport_security
                            not in args.custom_credentials_type
                        ):
                            skipped.append(
                                {
                                    "suite": "cloud_to_prod_auth",
                                    "client": str(language),
                                    "server": server_host_nickname,
                                    "test_case": test_case,
                                    "reason": f"transport_security '{transport_security}' not in --custom_credentials_type",
                                }
                            )
                            continue
                        test_job = cloud_to_prod_jobspec(
                            language,
                            test_case,
                            server_host_nickname,
                            self.spec.prod_servers[server_host_nickname],
                            google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                            docker_image=docker_images.get(str(language)),
                            auth=True,
                            manual_cmd_log=client_manual_cmd_log,
                            service_account_key_file=args.service_account_key_file,
                            default_service_account=args.default_service_account,
                            transport_security=transport_security,
                            allow_flakes=args.allow_flakes,
                            spec=self.spec,
                        )
                        jobs.append(test_job)

        for server in args.override_server:
            server_name = server[0]
            (server_host, server_port) = server[1].split(":")
            server_addresses[server_name] = (server_host, server_port)

        skip_c2c_http2_servers = set(
            self.spec.suites["http2_client"].get(
                "skip_cloud_to_cloud_servers", []
            )
        )
        http2_srv_name = str(self.spec.http2_server_target)
        for server_name, server_address in list(server_addresses.items()):
            (server_host, server_port) = server_address
            server_language = self.spec.languages.get(server_name, None)
            if server_name == http2_srv_name and not server_language:
                server_language = self.spec.http2_server_target
            skip_server = (
                server_language.unimplemented_test_cases_server()
                if server_language
                else []
            )
            for language in languages:
                unimpl_client = language.unimplemented_test_cases()
                for test_case in self.spec.standard_test_cases:
                    if test_case in unimpl_client:
                        skipped.append(
                            {
                                "suite": "cloud_to_cloud",
                                "client": str(language),
                                "server": server_name,
                                "test_case": test_case,
                                "reason": f"unimplemented by client '{language}'",
                            }
                        )
                        continue
                    if test_case in skip_server:
                        skipped.append(
                            {
                                "suite": "cloud_to_cloud",
                                "client": str(language),
                                "server": server_name,
                                "test_case": test_case,
                                "reason": f"unimplemented by server '{server_name}'",
                            }
                        )
                        continue
                    test_job = cloud_to_cloud_jobspec(
                        language,
                        test_case,
                        server_name,
                        server_host,
                        server_port,
                        docker_image=docker_images.get(str(language)),
                        transport_security=args.transport_security,
                        manual_cmd_log=client_manual_cmd_log,
                        allow_flakes=args.allow_flakes,
                        spec=self.spec,
                    )
                    jobs.append(test_job)

            if args.http2_interop:
                http2_client = self.spec.http2_client_target
                skip_bad = (
                    (server_language and server_language.skip_http2_bad_client)
                    or server_name in skip_c2c_http2_servers
                )
                for test_case in self.spec.http2_client_test_cases:
                    if skip_bad:
                        skipped.append(
                            {
                                "suite": "http2_client",
                                "client": str(http2_client),
                                "server": server_name,
                                "test_case": test_case,
                                "reason": f"server '{server_name}' skipped for http2_client suite",
                            }
                        )
                        continue
                    test_job = cloud_to_cloud_jobspec(
                        http2_client,
                        test_case,
                        server_name,
                        server_host,
                        server_port,
                        docker_image=docker_images.get(str(http2_client)),
                        transport_security=args.transport_security,
                        manual_cmd_log=client_manual_cmd_log,
                        allow_flakes=args.allow_flakes,
                        spec=self.spec,
                    )
                    jobs.append(test_job)

        if args.http2_server_interop:
            http2_srv = self.spec.http2_server_target
            sorted_h2_cases = sorted(self.spec.http2_server_test_cases)
            negative_cases = [
                tc
                for tc in self.spec.http2_server_test_cases
                if tc
                not in self.spec.http2_server_test_cases_that_use_grpc_clients
            ]
            for language in http2_clients:
                for test_case in negative_cases:
                    offset = sorted_h2_cases.index(test_case)
                    server_port = default_port + offset
                    if http2_port_mapper is not None and not args.manual_run:
                        server_port = http2_port_mapper(server_port)
                    test_job = cloud_to_cloud_jobspec(
                        language,
                        test_case,
                        str(http2_srv),
                        "localhost",
                        server_port,
                        docker_image=docker_images.get(str(language)),
                        manual_cmd_log=client_manual_cmd_log,
                        allow_flakes=args.allow_flakes,
                        spec=self.spec,
                    )
                    jobs.append(test_job)

            for language in languages:
                unimpl_client = language.unimplemented_test_cases()
                for (
                    test_case
                ) in self.spec.http2_server_test_cases_that_use_grpc_clients:
                    if test_case in unimpl_client:
                        skipped.append(
                            {
                                "suite": "http2_server",
                                "client": str(language),
                                "server": str(http2_srv),
                                "test_case": test_case,
                                "reason": f"unimplemented by client '{language}'",
                            }
                        )
                        continue
                    offset = sorted_h2_cases.index(test_case)
                    server_port = default_port + offset
                    if http2_port_mapper is not None and not args.manual_run:
                        server_port = http2_port_mapper(server_port)
                    h2_spec = self.spec.http2_server_test_case_map[test_case]
                    sec_override = (
                        h2_spec.transport_security_override or "insecure"
                    )
                    if args.transport_security != sec_override:
                        print(
                            "Creating grpc client to http2 server test case "
                            "with insecure connection, even though "
                            "args.transport_security is not insecure. Http2 "
                            "test server only supports insecure connections."
                        )
                    test_job = cloud_to_cloud_jobspec(
                        language,
                        test_case,
                        str(http2_srv),
                        "localhost",
                        server_port,
                        docker_image=docker_images.get(str(language)),
                        transport_security=sec_override,
                        manual_cmd_log=client_manual_cmd_log,
                        allow_flakes=args.allow_flakes,
                        spec=self.spec,
                    )
                    jobs.append(test_job)

        return ExecutionPlan(
            languages=languages,
            servers=servers,
            http2_clients=http2_clients,
            build_jobs=build_jobs,
            docker_images=docker_images,
            server_specs=server_specs,
            client_jobs=jobs,
            skipped_tests=skipped,
            server_manual_cmd_log=server_manual_cmd_log,
            client_manual_cmd_log=client_manual_cmd_log,
        )

    def execute(self, args, direct_build: bool = False) -> int:
        set_default_allow_flakes(getattr(args, "allow_flakes", False))
        languages, servers, http2_clients = self.resolve_languages_and_servers(
            args
        )

        if args.use_docker:
            if not args.travis:
                print(
                    "Seen --use_docker flag, will run interop tests under docker."
                )
                print("")
                print(
                    "IMPORTANT: The changes you are testing need to be locally"
                    " committed"
                )
                print(
                    "because only the committed changes in the current branch will be"
                )
                print("copied to the docker environment.")
                time.sleep(5)

        if args.manual_run and not args.use_docker:
            print(
                "--manual_run is only supported with --use_docker option enabled."
            )
            return 1

        if not args.use_docker and servers:
            print(
                "Running interop servers is only supported with --use_docker option"
                " enabled."
            )
            return 1

        docker_images: Dict[str, str] = {}
        if args.use_docker:
            build_jobs, docker_images = self.builder.plan_image_builds(
                languages=languages,
                servers=servers,
                http2_clients=http2_clients,
                http2_interop=args.http2_interop,
                http2_server_interop=args.http2_server_interop,
                deduplicate=True,
                direct_build=direct_build,
            )
            if build_jobs:
                jobset.message(
                    "START",
                    "Building interop docker images.",
                    do_newline=True,
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
                    build_resultset,
                    self.spec.defaults["reports"]["docker_build_xml"],
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
                    for image in set(docker_images.values()):
                        dockerjob.remove_image(image, skip_nonexistent=True)
                    return 1

        server_manual_cmd_log = [] if args.manual_run else None
        client_manual_cmd_log = [] if args.manual_run else None
        server_jobs: Dict[str, Any] = {}
        server_addresses: Dict[str, Tuple[str, Union[int, str]]] = {}
        default_port = int(
            self.spec.defaults.get("default_server_port", 8080)
        )

        try:
            for s in servers:
                lang = str(s)
                spec_job = server_jobspec(
                    self.spec.languages[lang],
                    docker_images.get(lang),
                    args.transport_security,
                    manual_cmd_log=server_manual_cmd_log,
                    spec=self.spec,
                )
                if not args.manual_run:
                    job = dockerjob.DockerJob(spec_job)
                    server_jobs[lang] = job
                    server_addresses[lang] = (
                        "localhost",
                        job.mapped_port(default_port),
                    )
                else:
                    server_addresses[lang] = ("localhost", "${SERVER_PORT}")

            http2_server_job = None
            if args.http2_server_interop:
                http2_srv = self.spec.http2_server_target
                lang = str(http2_srv)
                spec_job = server_jobspec(
                    http2_srv,
                    docker_images.get(lang),
                    manual_cmd_log=server_manual_cmd_log,
                    spec=self.spec,
                )
                if not args.manual_run:
                    http2_server_job = dockerjob.DockerJob(spec_job)
                    server_jobs[lang] = http2_server_job
                else:
                    server_addresses[lang] = ("localhost", "${SERVER_PORT}")

            if args.http2_server_interop and not args.manual_run:
                health_timeout = int(
                    self.spec.defaults.get(
                        "http2_server_health_timeout_seconds", 600
                    )
                )
                http2_server_job.wait_for_healthy(
                    timeout_seconds=health_timeout
                )

            plan = self.build_plan(
                args,
                server_addresses_override=server_addresses,
                http2_port_mapper=(
                    http2_server_job.mapped_port
                    if http2_server_job is not None
                    else None
                ),
                direct_build=direct_build,
                docker_images_override=docker_images if args.use_docker else None,
            )
            client_manual_cmd_log = plan.client_manual_cmd_log
            jobs = plan.client_jobs

            if not jobs:
                print("No jobs to run.")
                for image in set(docker_images.values()):
                    dockerjob.remove_image(image, skip_nonexistent=True)
                return 1

            if args.manual_run:
                print("All tests will skipped --manual_run option is active.")

            if args.verbose:
                print("Jobs to run: \n%s\n" % "\n".join(str(j) for j in jobs))

            num_failures, resultset = jobset.run(
                jobs,
                newline_on_success=True,
                maxjobs=args.jobs,
                skip_jobs=args.manual_run,
            )
            if (
                args.bq_result_table
                and resultset
                and upload_interop_results_to_bq
            ):
                upload_interop_results_to_bq(resultset, args.bq_result_table)
            if num_failures:
                jobset.message("FAILED", "Some tests failed", do_newline=True)
            else:
                jobset.message("SUCCESS", "All tests passed", do_newline=True)

            write_cmdlog_maybe(
                server_manual_cmd_log,
                self.spec.defaults["reports"]["manual_server_cmds"],
            )
            write_cmdlog_maybe(
                client_manual_cmd_log,
                self.spec.defaults["reports"]["manual_client_cmds"],
            )

            report_utils.render_junit_xml_report(
                resultset, self.spec.defaults["reports"]["tests_xml"]
            )

            for name, job in list(resultset.items()):
                if str(self.spec.http2_client_target) in name:
                    job[0].http2results = aggregate_http2_results(
                        job[0].message
                    )

            return 1 if num_failures else 0
        finally:
            for server, job in list(server_jobs.items()):
                if not job.is_running():
                    print('Server "%s" has exited prematurely.' % server)

            dockerjob.finish_jobs(list(server_jobs.values()))

            for image in set(docker_images.values()):
                if not args.manual_run:
                    print("Removing docker image %s" % image)
                    dockerjob.remove_image(image)
                else:
                    print("Preserving docker image: %s" % image)


# ============================================================================
# Release Matrix API & MatrixRunner
# ============================================================================

_SPEC = get_spec()
LANG_RUNTIME_MATRIX: Dict[str, List[str]] = _SPEC.lang_runtime_matrix
LANG_RELEASE_MATRIX: Dict[str, "OrderedDict[str, ReleaseInfo]"] = (
    _SPEC.lang_release_matrix
)

# Dynamically bind legacy factory functions (CXXLanguage, GoLanguage, etc.) from config.json
for _lspec in _SPEC.all_targets.values():
    if _lspec.legacy_factory:
        globals()[_lspec.legacy_factory] = (lambda bound=_lspec: bound)
    for _alias in _lspec.legacy_aliases:
        globals()[_alias] = (lambda bound=_lspec: bound)
    if _lspec.server_legacy_factory:
        globals()[_lspec.server_legacy_factory] = (
            lambda bound=_lspec.as_http2_server_target(): bound
        )


def get_github_repo(lang: str, spec: Optional[InteropSpec] = None) -> str:
    active_spec = spec or get_spec()
    return active_spec.matrix_repos.get(lang, "https://github.com/grpc/grpc.git")


def get_release_tags(lang: str, spec: Optional[InteropSpec] = None) -> List[str]:
    active_spec = spec or get_spec()
    return list(active_spec.lang_release_matrix[lang].keys())


def get_runtimes_for_lang_release(
    lang: str, release: str, spec: Optional[InteropSpec] = None
) -> List[str]:
    active_spec = spec or get_spec()
    runtimes = list(active_spec.lang_runtime_matrix[lang])
    release_info = active_spec.lang_release_matrix[lang].get(release)
    if release_info and release_info.runtimes:
        runtimes = list(release_info.runtimes)
    return runtimes


def should_build_docker_interop_image_from_release_tag(
    lang: str, spec: Optional[InteropSpec] = None
) -> bool:
    active_spec = spec or get_spec()
    return active_spec.matrix_build_from_release_tag.get(lang, True)


def get_all_releases(spec: Optional[InteropSpec] = None) -> List[str]:
    active_spec = spec or get_spec()
    return sorted(
        list(
            set(
                release
                for release_dict in active_spec.lang_release_matrix.values()
                for release in release_dict.keys()
            )
        )
    )


def resolve_testcases_filename(
    lang: str,
    runtime: str,
    release: str,
    spec: Optional[InteropSpec] = None,
) -> str:
    active_spec = spec or get_spec()
    testcases_file = None
    release_info = active_spec.lang_release_matrix.get(lang, {}).get(release)
    if release_info:
        testcases_file = release_info.testcases_file
    if not testcases_file:
        testcases_file = "%s__master" % lang

    overrides = active_spec.matrix_config.get(
        "runtime_testcases_prefix_overrides", {}
    )
    if runtime in overrides:
        rule = overrides[runtime]
        if rule.get("lang") == lang:
            testcases_file = testcases_file.replace(
                rule["replace_from"], rule["replace_to"]
            )
    return testcases_file


def read_test_cases_file(
    lang: str,
    runtime: str,
    release: str,
    spec: Optional[InteropSpec] = None,
    testcases_dir: Optional[str] = None,
) -> List[str]:
    active_spec = spec or get_spec()
    testcases_file = resolve_testcases_filename(
        lang, runtime, release, spec=active_spec
    )
    if testcases_file in active_spec.matrix_testcase_profiles:
        profile = active_spec.matrix_testcase_profiles[testcases_file]
        servers = active_spec.matrix_config.get(
            "servers",
            [
                "grpc-test.sandbox.googleapis.com",
                "grpc-test4.sandbox.googleapis.com",
            ],
        )
        test_cases = active_spec.matrix_config.get(
            "test_cases",
            [
                tc.name
                for tc in active_spec.standard_test_cases_spec
                if tc.cloud_to_prod_eligible
            ],
        )
        return profile.render_commands(servers, test_cases)

    base_dir = testcases_dir or os.path.abspath(
        os.path.join(_THIS_DIR, "../interop_matrix/testcases")
    )
    testcases_filepath = os.path.join(base_dir, testcases_file)
    lines = []
    with open(testcases_filepath) as f:
        for line in f.readlines():
            line = re.sub(r"\#.*$", "", line)
            line = line.strip()
            if line and not line.startswith("echo"):
                lines.append(line)
    return lines


def get_test_images_for_lang(
    lang: str,
    release_arg: str,
    image_path_prefix: str,
    spec: Optional[InteropSpec] = None,
) -> Dict[str, List[Tuple[str, str]]]:
    active_spec = spec or get_spec()
    known_releases = get_release_tags(lang, spec=active_spec)
    if release_arg == "all":
        releases = known_releases
    else:
        if release_arg not in known_releases:
            jobset.message(
                "SKIPPED",
                "release %s for %s is not defined" % (release_arg, lang),
                do_newline=True,
            )
            return {}
        releases = [release_arg]

    images: Dict[str, List[Tuple[str, str]]] = {}
    for tag in releases:
        for runtime in get_runtimes_for_lang_release(
            lang, tag, spec=active_spec
        ):
            image_name = "%s/grpc_interop_%s:%s" % (
                image_path_prefix,
                runtime,
                tag,
            )
            image_tuple = (tag, image_name)
            if runtime not in images:
                images[runtime] = []
            images[runtime].append(image_tuple)
    return images


def generate_test_case_jobspecs(
    lang: str,
    runtime: str,
    release: str,
    suite_name: str,
    server_host: str = "74.125.206.210",
    allow_flakes: bool = False,
    spec: Optional[InteropSpec] = None,
) -> List[jobset.JobSpec]:
    active_spec = spec or get_spec()
    testcase_lines = read_test_cases_file(
        lang, runtime, release, spec=active_spec
    )
    timeout_sec = int(
        active_spec.matrix_config.get("test_timeout_seconds", 60)
    )
    flake_retries = (
        int(active_spec.defaults.get("matrix_flake_retries", 5))
        if allow_flakes
        else 0
    )

    job_spec_list = []
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
            r"--server_host=[^ ]*", r"--server_host=%s" % server_host, line
        )

        if server_host != server and "--server_host_override=" not in line:
            line = re.sub(
                r"(--server_host=[^ ]*)",
                r"\1 --server_host_override=%s" % server,
                line,
            )

        job_spec = jobset.JobSpec(
            cmdline=line,
            shortname="%s:%s:%s:%s"
            % (suite_name, lang, server_short, testcase_name),
            timeout_seconds=timeout_sec,
            shell=True,
            flake_retries=flake_retries,
        )
        job_spec_list.append(job_spec)
    return job_spec_list


def pull_image_for_lang(
    lang: str,
    image: str,
    release: str,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    active_spec = spec or get_spec()
    pull_timeout = int(
        active_spec.matrix_config.get("pull_image_timeout_seconds", 15 * 60)
    )
    cmdline = [
        "time gcloud docker -- pull %s && time docker run --rm=true %s"
        " /bin/true" % (image, image)
    ]
    return jobset.JobSpec(
        cmdline=cmdline,
        shortname="pull_image_{}".format(image),
        timeout_seconds=pull_timeout,
        shell=True,
        flake_retries=2,
    )


def cleanup_docker_image(image: str) -> None:
    jobset.message("START", "Cleanup docker image %s" % image, do_newline=True)
    dockerjob.remove_image(image, skip_nonexistent=True)


def build_matrix_image_jobspec(
    runtime: str,
    env: Dict[str, str],
    gcr_tag: str,
    stack_base: str,
    lang: str,
    docker_path: str = "us-docker.pkg.dev/grpc-testing/testing-images-public",
    direct_build: bool = False,
    commit_log_path: Optional[str] = None,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    active_spec = spec or get_spec()
    basename = "grpc_interop_%s" % runtime
    tag = "%s/%s:%s" % (docker_path, basename, gcr_tag)
    timeout_sec = int(
        active_spec.matrix_config.get("build_image_timeout_seconds", 30 * 60)
    )

    if direct_build:
        builder = DockerImageBuilder(active_spec)
        job = builder.create_direct_build_jobspec(
            runtime,
            tag=tag,
            release=gcr_tag,
            commit_log_path=commit_log_path,
            context_dir=stack_base if stack_base else None,
            timeout_seconds=timeout_sec,
        )
        job.environ.update(env)
        return job

    build_env = {"INTEROP_IMAGE": tag, "BASE_NAME": basename}
    build_env.update(env)
    image_builder_path = active_spec.defaults.get(
        "legacy_image_builder_script",
        "tools/run_tests/dockerize/build_interop_image.sh",
    )
    if should_build_docker_interop_image_from_release_tag(
        lang, spec=active_spec
    ):
        image_builder_path = os.path.join(stack_base, image_builder_path)
    build_job = jobset.JobSpec(
        cmdline=[image_builder_path],
        environ=build_env,
        shortname="build_docker_%s" % runtime,
        timeout_seconds=timeout_sec,
    )
    build_job.tag = tag
    return build_job


def maybe_apply_patches_on_git_tag(
    stack_base: str,
    lang: str,
    release: str,
    patches_base_dir: Optional[str] = None,
    spec: Optional[InteropSpec] = None,
) -> None:
    active_spec = spec or get_spec()
    files_to_patch = []
    release_info = active_spec.lang_release_matrix[lang].get(release)
    if release_info:
        files_to_patch = release_info.patch
    if not files_to_patch:
        return
    base_dir = patches_base_dir or os.path.abspath(
        os.path.join(_THIS_DIR, "../interop_matrix")
    )
    patch_file = os.path.abspath(
        os.path.join(base_dir, "patches/%s_%s/git_repo.patch" % (lang, release))
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


def checkout_grpc_stack(
    lang: str,
    release: str,
    git_checkout: bool,
    git_checkout_root: str,
    reuse_git_root: bool = False,
    patches_base_dir: Optional[str] = None,
    spec: Optional[InteropSpec] = None,
) -> str:
    active_spec = spec or get_spec()
    assert git_checkout and git_checkout_root

    if not os.path.exists(git_checkout_root):
        os.makedirs(git_checkout_root)

    repo = get_github_repo(lang, spec=active_spec)
    repo_dir = os.path.splitext(os.path.basename(repo))[0]
    stack_base = os.path.join(git_checkout_root, repo_dir)

    if not reuse_git_root and os.path.exists(stack_base):
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
    maybe_apply_patches_on_git_tag(
        stack_base,
        lang,
        release,
        patches_base_dir=patches_base_dir,
        spec=active_spec,
    )
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


class MatrixRunner:
    """Runs or dry-runs backward-compatibility matrix test suites."""

    def __init__(self, spec: Optional[InteropSpec] = None):
        self.spec = spec or get_spec()

    def test_release(
        self,
        lang: str,
        runtime: str,
        release: str,
        image: str,
        xml_report_tree,
        skip_tests: bool,
        server_host: str,
        allow_flakes: bool,
        jobs: int,
        bq_result_table: str = "",
    ) -> int:
        total_num_failures = 0
        suite_name = "%s__%s_%s" % (lang, runtime, release)
        job_spec_list = generate_test_case_jobspecs(
            lang,
            runtime,
            release,
            suite_name,
            server_host=server_host,
            allow_flakes=allow_flakes,
            spec=self.spec,
        )

        if not job_spec_list:
            jobset.message(
                "FAILED", "No test cases were found.", do_newline=True
            )
            total_num_failures += 1
        else:
            num_failures, resultset = jobset.run(
                job_spec_list,
                newline_on_success=True,
                add_env={"docker_image": image},
                maxjobs=jobs,
                skip_jobs=skip_tests,
            )
            if bq_result_table and resultset and upload_test_results:
                upload_test_results.upload_interop_results_to_bq(
                    resultset, bq_result_table
                )
            if skip_tests:
                jobset.message("FAILED", "Tests were skipped", do_newline=True)
                total_num_failures += 1
            if num_failures:
                total_num_failures += num_failures

            report_utils.append_junit_xml_results(
                xml_report_tree,
                resultset,
                self.spec.suites["matrix"].get(
                    "junit_package", "grpc_interop_matrix"
                ),
                suite_name,
                str(uuid.uuid4()),
            )
        return total_num_failures

    def run_tests_for_lang(
        self,
        lang: str,
        runtime: str,
        images: Sequence[Tuple[str, str]],
        xml_report_tree,
        args,
    ) -> int:
        skip_tests = False
        total_num_failures = 0

        max_downloads = int(
            self.spec.matrix_config.get("max_parallel_downloads", 6)
        )
        max_pull_jobs = min(args.jobs, max_downloads)
        max_chunk_size = max_pull_jobs
        chunk_count = (len(images) + max_chunk_size) // max_chunk_size

        for chunk_index in range(chunk_count):
            chunk_start = chunk_index * max_chunk_size
            chunk_size = min(max_chunk_size, len(images) - chunk_start)
            chunk_end = chunk_start + chunk_size
            pull_specs = []
            if not skip_tests:
                for release, image in images[chunk_start:chunk_end]:
                    pull_specs.append(
                        pull_image_for_lang(
                            lang, image, release, spec=self.spec
                        )
                    )

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
                total_num_failures += self.test_release(
                    lang,
                    runtime,
                    release,
                    image,
                    xml_report_tree,
                    skip_tests,
                    server_host=args.server_host,
                    allow_flakes=args.allow_flakes,
                    jobs=args.jobs,
                    bq_result_table=args.bq_result_table,
                )
            if not args.keep:
                for _, image in images[chunk_start:chunk_end]:
                    cleanup_docker_image(image)
        if not total_num_failures:
            jobset.message(
                "SUCCESS", "All {} tests passed".format(lang), do_newline=True
            )
        else:
            jobset.message(
                "FAILED", "Some {} tests failed".format(lang), do_newline=True
            )

        return total_num_failures

    def execute(self, args) -> int:
        all_langs = list(self.spec.lang_runtime_matrix.keys())
        languages = args.language if args.language != ["all"] else all_langs
        total_num_failures = 0
        xml_report_tree = report_utils.new_junit_xml_tree()
        for lang in languages:
            docker_images = get_test_images_for_lang(
                lang, args.release, args.docker_path, spec=self.spec
            )
            for runtime in sorted(docker_images.keys()):
                total_num_failures += self.run_tests_for_lang(
                    lang,
                    runtime,
                    docker_images[runtime],
                    xml_report_tree,
                    args,
                )

        report_utils.create_xml_report_file(xml_report_tree, args.report_file)
        return 1 if total_num_failures else 0


# ============================================================================
# CLI Parsers & Entrypoints (Delegating Introspection to interop_runner)
# ============================================================================


class _MultiSelectAction(argparse.Action):
    """Allows both `-l a b` and repeated `-l a -l b` while preserving default."""

    def __call__(self, parser, namespace, values, option_string=None):
        seen_attr = f"_seen_{self.dest}"
        if not getattr(namespace, seen_attr, False):
            setattr(namespace, seen_attr, True)
            current = []
        else:
            current = list(getattr(namespace, self.dest, []) or [])
        if isinstance(values, list):
            for v in values:
                if v not in current:
                    current.append(v)
        elif values not in current:
            current.append(values)
        setattr(namespace, self.dest, current)


def add_introspection_arguments(argp: argparse.ArgumentParser) -> None:
    argp.add_argument(
        "--list-languages",
        default=False,
        action="store_true",
        help="List all declaratively configured languages, capabilities, and auth modes.",
    )
    argp.add_argument(
        "--list-tests",
        default=False,
        action="store_true",
        help="List all declaratively configured test suites and test cases.",
    )
    argp.add_argument(
        "--explain-matrix",
        default=None,
        nargs="?",
        const="all",
        help="Explain which tests will run and why specific cases are included or skipped.",
    )
    argp.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="Print the planned Docker image builds, server containers, and test commands without executing them.",
    )
    argp.add_argument(
        "--show-dockerfile",
        default=None,
        type=str,
        metavar="TARGET",
        help="Print the generated single-pass Dockerfile for a language or runtime and exit.",
    )
    argp.add_argument(
        "--direct-docker-build",
        default=False,
        action="store_true",
        help="Build Docker images directly via generated Dockerfiles instead of build_interop_image.sh.",
    )


def build_interop_argparser(
    spec: Optional[InteropSpec] = None,
) -> argparse.ArgumentParser:
    active_spec = spec or get_spec()
    argp = argparse.ArgumentParser(description="Run interop tests.")
    argp.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(active_spec.languages.keys()),
        nargs="+",
        default=["all"],
        action=_MultiSelectAction,
        help="Clients to run. Objc client can be only run on OSX.",
    )
    argp.add_argument(
        "-j", "--jobs", default=multiprocessing.cpu_count(), type=int
    )
    argp.add_argument(
        "--cloud_to_prod",
        default=False,
        action="store_const",
        const=True,
        help="Run cloud_to_prod tests.",
    )
    argp.add_argument(
        "--cloud_to_prod_auth",
        default=False,
        action="store_const",
        const=True,
        help="Run cloud_to_prod_auth tests.",
    )
    argp.add_argument(
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
    argp.add_argument(
        "--prod_servers",
        choices=list(active_spec.prod_servers.keys()),
        default=["default"],
        nargs="+",
        help=(
            "The servers to run cloud_to_prod and cloud_to_prod_auth tests against."
        ),
    )
    argp.add_argument(
        "-s",
        "--server",
        choices=["all"] + sorted(active_spec.servers),
        nargs="+",
        action=_MultiSelectAction,
        help="Run cloud_to_cloud servers in a separate docker "
        + "image. Servers can only be started automatically if "
        + "--use_docker option is enabled.",
        default=[],
    )
    argp.add_argument(
        "--override_server",
        action="append",
        type=lambda kv: kv.split("="),
        help=(
            "Use servername=HOST:PORT to explicitly specify a server. E.g."
            " csharp=localhost:50000"
        ),
        default=[],
    )
    argp.add_argument(
        "--service_account_key_file",
        type=str,
        help="The service account key file to use for some auth interop tests.",
        default=active_spec.defaults["default_service_account_key_file"],
    )
    argp.add_argument(
        "--default_service_account",
        type=str,
        help=(
            "Default GCE service account email to use for some auth interop tests."
        ),
        default=active_spec.defaults["default_service_account"],
    )
    argp.add_argument(
        "-t",
        "--travis",
        default=False,
        action="store_const",
        const=True,
        help=(
            "When set, indicates that the script is running on CI (= not locally)."
        ),
    )
    argp.add_argument(
        "-v", "--verbose", default=False, action="store_const", const=True
    )
    argp.add_argument(
        "--use_docker",
        default=False,
        action="store_const",
        const=True,
        help="Run all the interop tests under docker. That provides "
        + "additional isolation and prevents the need to install "
        + "language specific prerequisites. Only available on Linux.",
    )
    argp.add_argument(
        "--allow_flakes",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Allow flaky tests to show as passing (re-runs failed tests up to five"
            " times)"
        ),
    )
    argp.add_argument(
        "--manual_run",
        default=False,
        action="store_const",
        const=True,
        help="Prepare things for running interop tests manually. "
        + "Preserve docker images after building them and skip "
        "actually running the tests. Only print commands to run by " + "hand.",
    )
    argp.add_argument(
        "--http2_interop",
        default=False,
        action="store_const",
        const=True,
        help="Enable HTTP/2 client edge case testing. (Bad client, good server)",
    )
    argp.add_argument(
        "--http2_server_interop",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Enable HTTP/2 server edge case testing. (Includes positive and"
            " negative tests"
        ),
    )
    argp.add_argument(
        "--transport_security",
        choices=active_spec.transport_security_options,
        default="tls",
        type=str,
        nargs="?",
        const=True,
        help="Which transport security mechanism to use.",
    )
    argp.add_argument(
        "--custom_credentials_type",
        choices=active_spec.custom_credentials_type_options,
        default=active_spec.custom_credentials_type_options,
        nargs="+",
        help=(
            "Credential types to test in the cloud_to_prod setup. Default is to"
            " test with all creds types possible."
        ),
    )
    argp.add_argument(
        "--skip_compute_engine_creds",
        default=False,
        action="store_const",
        const=True,
        help="Skip auth tests requiring access to compute engine credentials.",
    )
    argp.add_argument(
        "--bq_result_table",
        default="",
        type=str,
        nargs="?",
        help="Upload test results to a specified BQ table.",
    )
    add_introspection_arguments(argp)
    return argp


def build_matrix_test_argparser(
    spec: Optional[InteropSpec] = None,
) -> argparse.ArgumentParser:
    active_spec = spec or get_spec()
    languages = list(active_spec.lang_runtime_matrix.keys())
    releases = get_all_releases(active_spec)

    argp = argparse.ArgumentParser(description="Run interop tests.")
    argp.add_argument(
        "-j", "--jobs", default=multiprocessing.cpu_count(), type=int
    )
    argp.add_argument(
        "--docker_path",
        default=active_spec.matrix_config.get(
            "default_docker_path",
            "us-docker.pkg.dev/grpc-testing/testing-images-public",
        ),
        help="Path of docker images",
    )
    argp.add_argument(
        "--release",
        default="all",
        choices=["all"] + releases,
        help=(
            "Release tags to test.  When testing all "
            'releases defined in client_matrix.py, use "all".'
        ),
    )
    argp.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(languages),
        nargs="+",
        default=["all"],
        help="Languages to test",
    )
    argp.add_argument(
        "--keep",
        action="store_true",
        help="keep the created local images after finishing the tests.",
    )
    argp.add_argument(
        "--report_file",
        default=active_spec.defaults["reports"]["matrix_xml"],
        help="The result file to create.",
    )
    argp.add_argument(
        "--allow_flakes",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Allow flaky tests to show as passing (re-runs failed "
            "tests up to five times)"
        ),
    )
    argp.add_argument(
        "--bq_result_table",
        default="",
        type=str,
        nargs="?",
        help="Upload test results to a specified BQ table.",
    )
    argp.add_argument(
        "--server_host",
        default=active_spec.matrix_config.get(
            "default_server_host", "74.125.206.210"
        ),
        type=str,
        nargs="?",
        help="The gateway to backend services.",
    )
    add_introspection_arguments(argp)
    return argp


def build_matrix_build_argparser(
    spec: Optional[InteropSpec] = None,
) -> argparse.ArgumentParser:
    active_spec = spec or get_spec()
    languages = list(active_spec.lang_runtime_matrix.keys())
    releases = get_all_releases(active_spec)

    argp = argparse.ArgumentParser(description="Run interop tests.")
    argp.add_argument(
        "--docker_path",
        default=active_spec.matrix_config.get(
            "default_docker_path",
            "us-docker.pkg.dev/grpc-testing/testing-images-public",
        ),
        help="Path of docker images",
    )
    argp.add_argument(
        "--release",
        default="master",
        choices=["all", "master"] + releases,
        help=(
            "github commit tag to checkout.  When building all "
            'releases defined in client_matrix.py, use "all". Valid only '
            "with --git_checkout."
        ),
    )
    argp.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(languages),
        nargs="+",
        default=["all"],
        help="Test languages to build docker images for.",
    )
    argp.add_argument(
        "--git_checkout",
        action="store_true",
        help=(
            "Use a separate git clone tree for building grpc stack. "
            "Required when using --release flag.  By default, current"
            "tree and the sibling will be used for building grpc stack."
        ),
    )
    argp.add_argument(
        "--git_checkout_root",
        default=active_spec.matrix_config.get(
            "default_git_checkout_root", "/export/hda3/tmp/grpc_matrix"
        ),
        help=(
            "Directory under which grpc-go/java/main repo will be "
            "cloned.  Valid only with --git_checkout."
        ),
    )
    argp.add_argument(
        "--keep",
        action="store_true",
        help="keep the created local images after uploading to GCR",
    )
    argp.add_argument(
        "--reuse_git_root",
        default=False,
        action="store_const",
        const=True,
        help=(
            "reuse the repo dir. If False, the existing git root "
            "directory will removed before a clean checkout, because "
            "reusing the repo can cause git checkout error if you switch "
            "between releases."
        ),
    )
    argp.add_argument(
        "--upload_images",
        action="store_true",
        help=(
            "If set, images will be uploaded to container registry after building."
        ),
    )
    add_introspection_arguments(argp)
    return argp


def _handle_common_introspection(
    args, spec: InteropSpec, raw_argv: Sequence[str]
) -> Optional[int]:
    if getattr(args, "list_languages", False):
        res = run_rust_runner(["--list-languages"])
        sys.stdout.write(res.stdout)
        return 0
    if getattr(args, "list_tests", False):
        res = run_rust_runner(["--list-tests"])
        sys.stdout.write(res.stdout)
        return 0
    if getattr(args, "show_dockerfile", None):
        builder = DockerImageBuilder(spec)
        sys.stdout.write(builder.generate_dockerfile(args.show_dockerfile))
        return 0
    if getattr(args, "explain_matrix", None) is not None:
        res = run_rust_runner(
            [
                "--explain-matrix",
                str(args.explain_matrix),
                "--transport_security",
                getattr(args, "transport_security", "tls"),
            ]
        )
        sys.stdout.write(res.stdout)
        return 0
    return None


def run_interop_main(
    argv: Optional[Sequence[str]] = None, spec: Optional[InteropSpec] = None
) -> int:
    active_spec = spec or get_spec()
    argp = build_interop_argparser(active_spec)
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = argp.parse_args(raw_argv)

    intro_rc = _handle_common_introspection(args, active_spec, raw_argv)
    if intro_rc is not None:
        return intro_rc

    if getattr(args, "dry_run", False):
        res = run_rust_runner(raw_argv)
        sys.stdout.write(res.stdout)
        return 0

    runner = InteropRunner(active_spec)
    return runner.execute(
        args, direct_build=getattr(args, "direct_docker_build", False)
    )


def run_matrix_tests_main(
    argv: Optional[Sequence[str]] = None, spec: Optional[InteropSpec] = None
) -> int:
    active_spec = spec or get_spec()
    argp = build_matrix_test_argparser(active_spec)
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = argp.parse_args(raw_argv)

    intro_rc = _handle_common_introspection(args, active_spec, raw_argv)
    if intro_rc is not None:
        return intro_rc

    if getattr(args, "dry_run", False):
        all_langs = list(active_spec.lang_runtime_matrix.keys())
        languages = args.language if args.language != ["all"] else all_langs
        print("=== Dry-Run Matrix Test Plan ===")
        total_jobs = 0
        for lang in languages:
            docker_images = get_test_images_for_lang(
                lang, args.release, args.docker_path, spec=active_spec
            )
            for runtime in sorted(docker_images.keys()):
                for release, image in docker_images[runtime]:
                    suite_name = f"{lang}__{runtime}_{release}"
                    jobspecs = generate_test_case_jobspecs(
                        lang,
                        runtime,
                        release,
                        suite_name,
                        server_host=args.server_host,
                        allow_flakes=args.allow_flakes,
                        spec=active_spec,
                    )
                    total_jobs += len(jobspecs)
                    print(
                        f"  [{suite_name}] image={image} test_cases={len(jobspecs)}"
                    )
        print(f"Total matrix test jobs planned: {total_jobs}")
        return 0

    runner = MatrixRunner(active_spec)
    return runner.execute(args)


def run_create_matrix_images_main(
    argv: Optional[Sequence[str]] = None, spec: Optional[InteropSpec] = None
) -> int:
    active_spec = spec or get_spec()
    argp = build_matrix_build_argparser(active_spec)
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = argp.parse_args(raw_argv)

    intro_rc = _handle_common_introspection(args, active_spec, raw_argv)
    if intro_rc is not None:
        return intro_rc

    all_langs = list(active_spec.lang_runtime_matrix.keys())
    languages = args.language if args.language != ["all"] else all_langs

    if getattr(args, "dry_run", False):
        print("=== Dry-Run Matrix Image Build Plan ===")
        for lang in languages:
            if args.release == "all":
                releases = get_release_tags(lang, spec=active_spec)
            else:
                releases = [args.release]
            for release in releases:
                runtimes = get_runtimes_for_lang_release(
                    lang, release, spec=active_spec
                )
                for runtime in runtimes:
                    tag = f"{args.docker_path}/grpc_interop_{runtime}:{release}"
                    print(
                        f"  [build_docker_{runtime}] lang={lang} release={release} tag={tag}"
                    )
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(run_interop_main())


def verify_dockerfile_oci_labels(dockerfile_path: str) -> Dict[str, str]:
    """Extracts and verifies io.grpc.interop.* OCI metadata labels from a static Dockerfile."""
    with open(dockerfile_path, "r") as f:
        content = f.read()
    labels: Dict[str, str] = {}
    for match in re.finditer(r"^LABEL\s+(io\.grpc\.interop\.[a-z0-9_]+)=(.+)$", content, re.MULTILINE):
        key = match.group(1)
        val = match.group(2).strip()
        if val.startswith('"') and val.endswith('"'):
            val = val[1:-1].replace('\\"', '"')
        labels[key] = val
    required = [
        "io.grpc.interop.release",
        "io.grpc.interop.client_cmd",
        "io.grpc.interop.server_cmd",
        "io.grpc.interop.http2_client_cmd",
        "io.grpc.interop.workdir",
        "io.grpc.interop.env",
        "io.grpc.interop.capabilities",
    ]
    for req in required:
        if req not in labels:
            raise SpecValidationError(f"Dockerfile {dockerfile_path} missing required OCI label {req}")
    return labels


def resolve_release_metadata_from_rules(
    lang: str,
    runtime: str,
    release: str,
    spec: Optional[InteropSpec] = None,
) -> Dict[str, Any]:
    """Resolves release metadata from config.json matrix.release_metadata_rules without hardcoded languages."""
    active_spec = spec or get_spec()
    rules = active_spec.matrix_config.get("release_metadata_rules", [])
    ver_tuple = (999, 999, 999)
    m = re.match(r"^v?(\d+)\.(\d+)\.(\d+)", str(release))
    if m:
        ver_tuple = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    for rule in rules:
        langs = rule.get("langs", [])
        runtimes = rule.get("runtimes", ["*"])
        max_ver = tuple(rule.get("max_version", [999, 999, 999]))
        if lang in langs and ("*" in runtimes or runtime in runtimes) and ver_tuple <= max_ver:
            return dict(rule)
    raise SpecValidationError(f"No release metadata rule matched lang={lang}, runtime={runtime}, release={release}")
