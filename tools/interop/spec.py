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
"""Declarative specification loader and schema validator for gRPC interop tests."""

from collections import OrderedDict
import copy
from dataclasses import dataclass, field
import json
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple
import yaml

DEFAULT_SPEC_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "interop_spec.yaml"
)

VALID_ARGS_MODES = frozenset(
    {"append", "joined_flag", "ignore_args", "extract_port_template"}
)
VALID_AUTH_CRED_MODES = frozenset({"env", "flag"})


class SpecValidationError(ValueError):
    """Raised when interop_spec.yaml fails schema or cross-reference validation."""


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
    """Special server Docker container configuration (e.g., multi-port HTTP/2 server)."""

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
        docker_build: Optional[DockerBuildSpec] = None,
        server_docker_config: Optional[ServerDockerConfig] = None,
        is_http2_target: bool = False,
        server_unimplemented_client_cases: Optional[List[str]] = None,
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
        self._server_unimplemented_client_cases = (
            list(server_unimplemented_client_cases)
            if server_unimplemented_client_cases is not None
            else list(unimplemented_client_cases)
        )
        self.docker_build = docker_build
        self.server_docker_config = server_docker_config
        self.is_http2_target = is_http2_target

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

    def needs_compute_engine_creds(self, language_name: str) -> bool:
        if self.requires_compute_engine_creds:
            return True
        return str(language_name) in self.requires_compute_engine_creds_languages

    def needs_default_service_account(self, language_name: str) -> bool:
        if self.uses_default_service_account:
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
    """Validates schema structure and cross-references of the raw spec dictionary."""
    if not isinstance(raw, dict):
        raise SpecValidationError("Root of interop_spec must be a mapping")

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

    if str(raw["schema_version"]) not in ("1", "1.0"):
        raise SpecValidationError(
            f"Unsupported schema_version: {raw['schema_version']!r}"
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
    """Parsed, validated Single Source of Truth specification for gRPC interop."""

    def __init__(self, raw: Dict[str, Any], spec_path: str = DEFAULT_SPEC_PATH):
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

        # Parse test cases
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

        # Built-in group expansions
        self._builtin_groups: Dict[str, List[str]] = {
            "standard_test_cases": list(self.standard_test_cases),
            "standard_test_cases_except_first": list(
                self.standard_test_cases[1:]
            ),
            "auth_test_cases": list(self.auth_test_cases),
            "http2_client_test_cases": list(self.http2_client_test_cases),
            "http2_server_test_cases": list(self.http2_server_test_cases),
        }

        # Parse languages
        self.all_targets: Dict[str, LanguageSpec] = {}
        self.languages: Dict[str, LanguageSpec] = {}
        for lang_name, ldata in raw["languages"].items():
            lang_spec = self._parse_language(lang_name, ldata)
            self.all_targets[lang_name] = lang_spec
            if not lang_spec.is_http2_target:
                self.languages[lang_name] = lang_spec

        self.http2_client_target: LanguageSpec = self.all_targets["http2"]
        self.http2_server_target: LanguageSpec = self.all_targets[
            "http2"
        ].as_http2_server_target()

        # Servers list preserving canonical order
        self.servers: List[str] = [
            name
            for name, lspec in self.languages.items()
            if lspec.supports_server
        ]

        # Derived mappings matching run_interop_tests.py module attributes
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

        # Parse matrix section
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
            docker_build=docker_build,
            server_docker_config=server_docker_config,
            is_http2_target=bool(ldata.get("is_http2_target", False)),
            server_unimplemented_client_cases=server_unimpl_client,
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
    def load(cls, path: str = DEFAULT_SPEC_PATH) -> "InteropSpec":
        """Loads and validates InteropSpec from the given YAML/JSON path."""
        return load_spec(path)

    def get_language(self, name: str) -> LanguageSpec:
        """Returns LanguageSpec for a standard language or special target (e.g. 'http2')."""
        return self.all_targets[name]

    def get_matrix_testcase_commands(self, profile_name: str) -> List[str]:
        """Renders the 18 docker run test commands for a named matrix testcase profile."""
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
        """Returns structured explanation of which cloud_to_cloud tests run or are skipped."""
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


def load_spec(path: str = DEFAULT_SPEC_PATH) -> InteropSpec:
    """Loads and validates InteropSpec from a YAML or JSON file."""
    with open(path, "r") as f:
        if path.endswith(".json"):
            raw = json.load(f)
        else:
            raw = yaml.safe_load(f)
    return InteropSpec(raw, spec_path=path)


def get_spec() -> InteropSpec:
    """Returns the cached singleton InteropSpec."""
    global _CACHED_SPEC
    if _CACHED_SPEC is None:
        _CACHED_SPEC = load_spec(DEFAULT_SPEC_PATH)
    return _CACHED_SPEC
