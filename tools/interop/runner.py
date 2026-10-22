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
"""Capability-driven gRPC interop runner and matrix harness loaded from config.json."""

import argparse
import atexit
from enum import Enum
import itertools
import json
import multiprocessing
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
DEFAULT_CONFIG_PATH = os.path.join(_THIS_DIR, "config.json")
DEFAULT_SCHEMA_PATH = os.path.join(_THIS_DIR, "config.schema.json")

_PYTHON_UTILS_DIR = os.path.join(ROOT, "tools/run_tests/python_utils")
if _PYTHON_UTILS_DIR not in sys.path:
    sys.path.insert(0, _PYTHON_UTILS_DIR)

import dockerjob  # noqa: E402
import jobset  # noqa: E402
import report_utils  # noqa: E402

try:
    import upload_test_results  # noqa: E402
    from upload_test_results import upload_interop_results_to_bq  # noqa: E402
except ImportError:
    upload_test_results = None
    upload_interop_results_to_bq = None


class Capability(Enum):
    """Explicit positive feature and transport capabilities for gRPC interop."""

    CORE_INTEROP = "CORE_INTEROP"
    ADVANCED_INTEROP = "ADVANCED_INTEROP"
    TIMEOUT_ON_SLEEPING_SERVER = "TIMEOUT_ON_SLEEPING_SERVER"
    CLIENT_COMPRESSION_UNARY = "CLIENT_COMPRESSION_UNARY"
    CLIENT_COMPRESSION_STREAMING = "CLIENT_COMPRESSION_STREAMING"
    SERVER_COMPRESSION_UNARY = "SERVER_COMPRESSION_UNARY"
    SERVER_COMPRESSION_STREAMING = "SERVER_COMPRESSION_STREAMING"
    SPECIAL_STATUS_MESSAGE = "SPECIAL_STATUS_MESSAGE"
    DATA_FRAME_PADDING = "DATA_FRAME_PADDING"
    ORCA = "ORCA"
    SINGLE_INVOCATION_SUITE = "SINGLE_INVOCATION_SUITE"
    AUTH_CREDS = "AUTH_CREDS"
    AUTH_KEY_FILE_ENV = "AUTH_KEY_FILE_ENV"
    GOOGLE_DEFAULT_CREDS = "GOOGLE_DEFAULT_CREDS"
    GCE_CHANNEL_CREDS = "GCE_CHANNEL_CREDS"
    GCE_OAUTH2 = "GCE_OAUTH2"
    HTTP2_INTEROP = "HTTP2_INTEROP"
    HTTP2_SERVER_INTEROP = "HTTP2_SERVER_INTEROP"
    HTTP2_BADSERVER_CLIENT = "HTTP2_BADSERVER_CLIENT"
    TLS = "TLS"
    INSECURE = "INSECURE"
    ALTS = "ALTS"
    MAX_CONCURRENT_STREAMS_CONNECTION_SCALING = (
        "MAX_CONCURRENT_STREAMS_CONNECTION_SCALING"
    )


def _parse_capability(val):
    if isinstance(val, Capability):
        return val
    key = str(val).strip().upper()
    return Capability[key]


def _parse_capability_set(items):
    return frozenset(_parse_capability(x) for x in (items or []))


def load_schema(schema_path=None):
    """Loads tools/interop/config.schema.json."""
    path = schema_path or DEFAULT_SCHEMA_PATH
    with open(path, "r") as f:
        return json.load(f)


def _expand_release_selector(selector, releases):
    """Expands a release selector ('v1.0.1', 'v1.0.1..v1.19.0', or comma-separated) against releases."""
    matched = []
    for part in selector.split(","):
        part = part.strip()
        if not part:
            raise ValueError("Empty release selector segment in %r" % selector)
        if ".." in part:
            start_tag, end_tag = part.split("..", 1)
            if start_tag not in releases or end_tag not in releases:
                raise ValueError(
                    "Unknown release tag in range %r (known: %d releases)"
                    % (part, len(releases))
                )
            start_idx = releases.index(start_tag)
            end_idx = releases.index(end_tag)
            if start_idx > end_idx:
                raise ValueError(
                    "Reversed release range %r (%s comes after %s)"
                    % (part, start_tag, end_tag)
                )
            matched.extend(releases[start_idx : end_idx + 1])
        else:
            if part not in releases:
                raise ValueError("Unknown release tag %r in overrides" % part)
            matched.append(part)
    return matched


def _resolve_json_pointer(root_schema, ref):
    if not ref.startswith("#/"):
        raise ValueError("Unsupported external $ref: %s" % ref)
    node = root_schema
    for part in ref[2:].split("/"):
        node = node[part]
    return node


def _validate_schema_node(instance, node_schema, root_schema, path="$"):
    if "$ref" in node_schema:
        target = _resolve_json_pointer(root_schema, node_schema["$ref"])
        _validate_schema_node(instance, target, root_schema, path)

    expected_type = node_schema.get("type")
    if expected_type is not None:
        if expected_type == "object" and not isinstance(instance, dict):
            raise ValueError(
                "%s: expected object, got %s" % (path, type(instance).__name__)
            )
        if expected_type == "array" and not isinstance(instance, list):
            raise ValueError(
                "%s: expected array, got %s" % (path, type(instance).__name__)
            )
        if expected_type == "string" and not isinstance(instance, str):
            raise ValueError(
                "%s: expected string, got %s" % (path, type(instance).__name__)
            )
        if expected_type == "boolean" and not isinstance(instance, bool):
            raise ValueError(
                "%s: expected boolean, got %s" % (path, type(instance).__name__)
            )
        if expected_type == "integer" and (
            not isinstance(instance, int) or isinstance(instance, bool)
        ):
            raise ValueError(
                "%s: expected integer, got %s" % (path, type(instance).__name__)
            )

    if "const" in node_schema and instance != node_schema["const"]:
        raise ValueError(
            "%s: expected const %r, got %r"
            % (path, node_schema["const"], instance)
        )

    if "enum" in node_schema and instance not in node_schema["enum"]:
        raise ValueError(
            "%s: value %r not in enum %r"
            % (path, instance, node_schema["enum"])
        )

    if "oneOf" in node_schema:
        matches = 0
        last_err = None
        for sub in node_schema["oneOf"]:
            try:
                _validate_schema_node(instance, sub, root_schema, path)
                matches += 1
            except ValueError as err:
                last_err = err
        if matches != 1:
            raise ValueError(
                "%s: value matched %d oneOf branches (expected 1): %s"
                % (path, matches, last_err)
            )

    if isinstance(instance, str):
        if (
            "minLength" in node_schema
            and len(instance) < node_schema["minLength"]
        ):
            raise ValueError(
                "%s: string length %d < minLength %d"
                % (path, len(instance), node_schema["minLength"])
            )
        if "pattern" in node_schema and not re.search(
            node_schema["pattern"], instance
        ):
            raise ValueError(
                "%s: string %r does not match pattern %r"
                % (path, instance, node_schema["pattern"])
            )

    if isinstance(instance, list):
        if (
            "minItems" in node_schema
            and len(instance) < node_schema["minItems"]
        ):
            raise ValueError(
                "%s: array length %d < minItems %d"
                % (path, len(instance), node_schema["minItems"])
            )
        if node_schema.get("uniqueItems"):
            seen = set()
            for item in instance:
                key = json.dumps(item, sort_keys=True)
                if key in seen:
                    raise ValueError(
                        "%s: duplicate array item %r" % (path, item)
                    )
                seen.add(key)
        if "items" in node_schema:
            item_schema = node_schema["items"]
            for idx, item in enumerate(instance):
                _validate_schema_node(
                    item, item_schema, root_schema, "%s[%d]" % (path, idx)
                )

    if isinstance(instance, dict):
        if (
            "minProperties" in node_schema
            and len(instance) < node_schema["minProperties"]
        ):
            raise ValueError(
                "%s: object has %d properties, minProperties is %d"
                % (path, len(instance), node_schema["minProperties"])
            )
        for req_key in node_schema.get("required", ()):
            if req_key not in instance:
                raise ValueError(
                    "%s: missing required property %r" % (path, req_key)
                )
        props = node_schema.get("properties", {})
        pat_props = node_schema.get("patternProperties", {})
        add_props = node_schema.get("additionalProperties", True)
        for k, v in instance.items():
            sub_path = "%s.%s" % (path, k)
            matched = False
            if k in props:
                matched = True
                _validate_schema_node(v, props[k], root_schema, sub_path)
            for pat, pat_schema in pat_props.items():
                if re.search(pat, k):
                    matched = True
                    _validate_schema_node(v, pat_schema, root_schema, sub_path)
            if not matched:
                if add_props is False:
                    raise ValueError("%s: unknown property %r" % (path, k))
                if isinstance(add_props, dict):
                    _validate_schema_node(v, add_props, root_schema, sub_path)


def validate_config(data, schema=None):
    """Validates config data against config.schema.json and cross-field invariants."""
    active_schema = schema if schema is not None else load_schema()
    _validate_schema_node(data, active_schema, active_schema, "$")

    expected_caps = {c.value for c in Capability}
    if set(data["capabilities"]) != expected_caps:
        raise ValueError(
            "$.capabilities mismatch with Capability enum: %r"
            % (set(data["capabilities"]) ^ expected_caps)
        )

    std_cases = data["test_cases"]["standard"]
    for tc_name, tc_spec in data["test_cases"]["http2_server"].items():
        grpc_tc = tc_spec.get("grpc_client_test_case")
        if grpc_tc and grpc_tc not in std_cases:
            raise ValueError(
                "$.test_cases.http2_server.%s.grpc_client_test_case %r not in standard test_cases"
                % (tc_name, grpc_tc)
            )
    dedicated_cases = data["test_cases"].get("dedicated_server", {})
    for tc_name, tc_spec in dedicated_cases.items():
        srv = data["languages"].get(tc_spec["server"])
        if srv is None or "server_cmd" not in srv:
            raise ValueError(
                "$.test_cases.dedicated_server.%s.server %r is not a language with a server_cmd"
                % (tc_name, tc_spec["server"])
            )

    matrix = data["matrix"]
    for tc_name in matrix["test_cases"]:
        if tc_name not in std_cases:
            raise ValueError(
                "$.matrix.test_cases references unknown standard test case %r"
                % tc_name
            )

    profiles = matrix["custom_entrypoints"]
    for mlang, mdata in matrix["languages"].items():
        releases = mdata["releases"]
        seen_override_tags = set()
        tags_with_entrypoint = set()
        for selector, ov in mdata.get("release_overrides", {}).items():
            matched_tags = _expand_release_selector(selector, releases)
            for tag in matched_tags:
                if tag in seen_override_tags:
                    raise ValueError(
                        "$.matrix.languages.%s.release_overrides has overlapping selector for tag %r"
                        % (mlang, tag)
                    )
                seen_override_tags.add(tag)
            tc_file = ov.get("testcases_file")
            if tc_file and tc_file not in profiles:
                raise ValueError(
                    "$.matrix.languages.%s.release_overrides[%r] references unknown entrypoint %r"
                    % (mlang, selector, tc_file)
                )
            if tc_file:
                tags_with_entrypoint.update(matched_tags)
        # Releases without an overriding testcases_file use '<lang>__master'.
        default_profile = "%s__master" % mlang
        if default_profile not in profiles and (
            set(releases) - tags_with_entrypoint
        ):
            raise ValueError(
                "$.matrix.languages.%s missing default entrypoint %r in custom_entrypoints"
                % (mlang, default_profile)
            )


def load_config(config_path=None, schema_path=None):
    """Loads and validates tools/interop/config.json against tools/interop/config.schema.json."""
    path = config_path or DEFAULT_CONFIG_PATH
    with open(path, "r") as f:
        data = json.load(f)
    schema = load_schema(schema_path)
    validate_config(data, schema)
    return data


def _repo_to_build_context(repo_url):
    repo_name = os.path.splitext(os.path.basename(repo_url.rstrip("/")))[0]
    ctx_name = repo_name.replace("-", "_")
    env_var = (
        "GRPC_ROOT" if ctx_name == "grpc" else "%s_ROOT" % ctx_name.upper()
    )
    default_rel_path = "." if repo_name == "grpc" else "../%s" % repo_name
    return ctx_name, {
        "env_var": env_var,
        "default_rel_path": default_rel_path,
    }


def _derive_build_contexts(cfg):
    contexts = {}
    for lcfg in cfg.get("languages", {}).values():
        repo_url = lcfg.get("repo")
        if repo_url:
            ctx_name, ctx_spec = _repo_to_build_context(repo_url)
            contexts.setdefault(ctx_name, ctx_spec)
    contexts.update(cfg.get("settings", {}).get("build_contexts", {}))
    return contexts


_CONFIG = load_config()
_SETTINGS = _CONFIG["settings"]
_TIMEOUT_SETTINGS = _SETTINGS["timeouts"]
_RETRY_SETTINGS = _SETTINGS["retries"]

DOCKER_WORKDIR_ROOT = _SETTINGS["docker_workdir_root"]
_DEFAULT_SERVER_PORT = _SETTINGS["default_server_port"]
_TEST_TIMEOUT = _TIMEOUT_SETTINGS["test_seconds"]
_SERVER_TIMEOUT = _TIMEOUT_SETTINGS["server_seconds"]
_DOCKER_BUILD_TIMEOUT = _TIMEOUT_SETTINGS["docker_build_seconds"]
_HTTP2_SERVER_HEALTHCHECK_TIMEOUT = _TIMEOUT_SETTINGS[
    "http2_server_healthcheck_seconds"
]
_FLAKE_RETRIES = _RETRY_SETTINGS["flake_retries"]
_TIMEOUT_RETRIES = _RETRY_SETTINGS["timeout_retries"]
_BUILD_CONTEXTS = _derive_build_contexts(_CONFIG)
# interop-last's image builder. Every gRPC release cut before tools/interop/
# existed ships its own copy, and matrix-build runs that copy for those
# releases exactly as tools/interop_matrix/create_matrix_images.py did.
_LEGACY_IMAGE_BUILDER = "tools/run_tests/dockerize/build_interop_image.sh"
_INTEROP_STAGE_MARKER = "#=== INTEROP_BUILD_STAGE ==="
_DOCKER_BUILD_XML_REPORT = "interop_docker_build/sponge_log.xml"
_TESTS_XML_REPORT = "interop_test/sponge_log.xml"
_MATRIX_XML_REPORT = "report.xml"
_DEFAULT_SERVICE_ACCOUNT_KEY_FILE = _SETTINGS[
    "default_service_account_key_file"
]
_DEFAULT_SERVICE_ACCOUNT = _SETTINGS["default_service_account"]
_OAUTH_SCOPE = _SETTINGS["oauth_scope"]
_C2C_CLIENT_EXTRA_FLAGS = list(_SETTINGS["cloud_to_cloud_client_flags"])

_TRANSPORT_SECURITY_SPECS = {
    "tls": {
        "required_capability": Capability.TLS,
        "client_flags": ["--use_tls=true"],
        "server_flags": ["--use_tls=true"],
    },
    "alts": {
        "required_capability": Capability.ALTS,
        "client_flags": ["--use_tls=false", "--use_alts=true"],
        "server_flags": ["--use_tls=false", "--use_alts=true"],
    },
    "insecure": {
        "required_capability": Capability.INSECURE,
        "client_flags": ["--use_tls=false"],
        "server_flags": ["--use_tls=false"],
    },
}

_CUSTOM_CREDENTIALS_SPECS = {
    "tls": {
        "required_capability": Capability.TLS,
        "client_flags": ["--use_tls=true"],
    },
    "google_default_credentials": {
        "required_capability": Capability.GOOGLE_DEFAULT_CREDS,
        "client_flags": [
            "--custom_credentials_type=google_default_credentials",
        ],
    },
    "compute_engine_channel_creds": {
        "required_capability": Capability.GCE_CHANNEL_CREDS,
        "client_flags": [
            "--custom_credentials_type=compute_engine_channel_creds",
        ],
    },
}

_STANDARD_TEST_CASE_SPECS = dict(_CONFIG["test_cases"]["standard"])
_AUTH_TEST_CASE_SPECS = dict(_CONFIG["test_cases"]["auth"])
_HTTP2_CLIENT_TEST_CASE_SPECS = dict(_CONFIG["test_cases"]["http2_client"])
_HTTP2_SERVER_TEST_CASE_SPECS = dict(_CONFIG["test_cases"]["http2_server"])
_DEDICATED_SERVER_TEST_CASE_SPECS = dict(
    _CONFIG["test_cases"].get("dedicated_server", {})
)

_TEST_CASES = list(_STANDARD_TEST_CASE_SPECS.keys())
_AUTH_TEST_CASES = list(_AUTH_TEST_CASE_SPECS.keys())
_HTTP2_TEST_CASES = list(_HTTP2_CLIENT_TEST_CASE_SPECS.keys())
_HTTP2_SERVER_TEST_CASES = list(_HTTP2_SERVER_TEST_CASE_SPECS.keys())

TEST_CASE_REQUIRED_CAPABILITIES = {}
SERVER_TEST_CASE_REQUIRED_CAPABILITIES = {}
for _suite_name in ("standard", "auth", "http2_client"):
    for _tc_name, _tc_spec in _CONFIG["test_cases"][_suite_name].items():
        _caps = _parse_capability_set(_tc_spec["capabilities"])
        TEST_CASE_REQUIRED_CAPABILITIES[_tc_name] = _caps
        SERVER_TEST_CASE_REQUIRED_CAPABILITIES[_tc_name] = _caps
for _tc_name, _tc_spec in _HTTP2_SERVER_TEST_CASE_SPECS.items():
    TEST_CASE_REQUIRED_CAPABILITIES[_tc_name] = _parse_capability_set(
        _tc_spec["client_capabilities"]
    )
    SERVER_TEST_CASE_REQUIRED_CAPABILITIES[_tc_name] = _parse_capability_set(
        _tc_spec["server_capabilities"]
    )
for _tc_name, _tc_spec in _DEDICATED_SERVER_TEST_CASE_SPECS.items():
    TEST_CASE_REQUIRED_CAPABILITIES[_tc_name] = _parse_capability_set(
        _tc_spec["client_capabilities"]
    )

TRANSPORT_SECURITY_CAPABILITIES = {}
for _ts_name, _ts_cfg in _TRANSPORT_SECURITY_SPECS.items():
    TRANSPORT_SECURITY_CAPABILITIES[_ts_name] = _ts_cfg["required_capability"]
for _cc_name, _cc_cfg in _CUSTOM_CREDENTIALS_SPECS.items():
    TRANSPORT_SECURITY_CAPABILITIES[_cc_name] = _cc_cfg["required_capability"]


class CommandSpec:
    """Renders command tokens from declarative specification in config.json."""

    def __init__(
        self,
        command,
        args_mode="append",
        arg_template=None,
    ):
        self.command = list(command or [])
        self.args_mode = args_mode or "append"
        self.arg_template = arg_template

    @classmethod
    def from_dict(cls, raw):
        if not raw:
            return None
        if isinstance(raw, list):
            return cls(command=raw, args_mode="append")
        return cls(
            command=raw.get("command", []),
            args_mode=raw["args_mode"],
            arg_template=raw.get("arg_template"),
        )

    def render(self, args):
        args_list = list(args or [])
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
            for arg in args_list:
                match = re.search(r"--server_port=(\d+|\$\{SERVER_PORT\})", arg)
                if match:
                    portnum = match.group(1)
                    cmdline = (self.arg_template or "").format(port=portnum)
                    return [cmdline]
            return None
        raise ValueError("Unsupported args_mode: %s" % self.args_mode)


def check_client_supports(
    capabilities,
    test_case,
    transport_security="tls",
    skip_compute_engine_creds=False,
):
    """Returns True if a client with `capabilities` supports `(test_case, transport_security)`."""
    t_cap = TRANSPORT_SECURITY_CAPABILITIES.get(transport_security)
    if t_cap is None or t_cap not in capabilities:
        return False
    req = TEST_CASE_REQUIRED_CAPABILITIES.get(test_case)
    if req is None or not req.issubset(capabilities):
        return False
    if (
        Capability.SINGLE_INVOCATION_SUITE in capabilities
        and test_case in _TEST_CASES[1:]
    ):
        return False
    if skip_compute_engine_creds:
        auth_spec = _AUTH_TEST_CASE_SPECS.get(test_case)
        if auth_spec:
            if auth_spec.get("requires_compute_engine_creds"):
                return False
            req_cap_name = auth_spec.get(
                "requires_compute_engine_creds_capability"
            )
            if req_cap_name and _parse_capability(req_cap_name) in capabilities:
                return False
    return True


def check_server_supports(capabilities, test_case, transport_security="tls"):
    """Returns True if a server with `capabilities` supports `(test_case, transport_security)`."""
    t_cap = TRANSPORT_SECURITY_CAPABILITIES.get(transport_security)
    if t_cap is None or t_cap not in capabilities:
        return False
    req = SERVER_TEST_CASE_REQUIRED_CAPABILITIES.get(test_case)
    if req is None or not req.issubset(capabilities):
        return False
    return True


class ProdServerEndpoint:
    """Capability model for production interop target servers (default, gateway_v4)."""

    PROD_CAPABILITIES = _parse_capability_set(
        _CONFIG.get("prod_server_capabilities", [])
    )

    def __init__(
        self, nickname="default", host="grpc-test.sandbox.googleapis.com"
    ):
        self.nickname = nickname
        self.host = host
        self.server_capabilities = self.PROD_CAPABILITIES

    def supports(
        self,
        test_case,
        transport_security="tls",
        role="server",
        skip_compute_engine_creds=False,
    ):
        return check_server_supports(
            self.server_capabilities, test_case, transport_security
        )

    def supports_server(self, test_case, transport_security="tls"):
        return check_server_supports(
            self.server_capabilities, test_case, transport_security
        )

    def __str__(self):
        return self.nickname


class ExternalServerEndpoint:
    """Capability model for custom --override_server targets not in _LANGUAGES."""

    ALL_SERVER_CAPABILITIES = frozenset(Capability)

    def __init__(self, name):
        self.name = name
        self.safename = name
        self.server_capabilities = self.ALL_SERVER_CAPABILITIES

    def supports(
        self,
        test_case,
        transport_security="tls",
        role="server",
        skip_compute_engine_creds=False,
    ):
        return check_server_supports(
            self.server_capabilities, test_case, transport_security
        )

    def supports_server(self, test_case, transport_security="tls"):
        return check_server_supports(
            self.server_capabilities, test_case, transport_security
        )

    def __str__(self):
        return self.name


def _resolve_language_config(lang_name, raw_cfg, defaults):
    merged = dict(defaults)
    merged.update(raw_cfg)
    merged["safename"] = merged.get("safename", lang_name)
    if "included_in_all_clients" not in raw_cfg and merged.get(
        "is_http2_target", False
    ):
        merged["included_in_all_clients"] = False
    if "cwd" in raw_cfg:
        merged.setdefault("client_cwd", raw_cfg["cwd"])
        if raw_cfg.get("server_cmd") is not None:
            merged.setdefault("server_cwd", raw_cfg["cwd"])
        if raw_cfg.get("http2_client_cmd") is not None:
            merged.setdefault("http2_cwd", raw_cfg["cwd"])

    merged["client_capabilities"] = list(raw_cfg["client_capabilities"])
    merged["server_capabilities"] = list(raw_cfg["server_capabilities"])
    return merged


class ConfigurableLanguage:
    """Language/endpoint configuration loaded from tools/interop/config.json."""

    default_role = "client"

    def __init__(self, name, raw_cfg, role_override=None, use_contract=False):
        self.name = name
        self.repo = raw_cfg["repo"]
        self.safename = raw_cfg.get("safename", name)
        self.included_in_all_clients = bool(
            raw_cfg.get("included_in_all_clients", True)
        )
        self.docker_supported = bool(raw_cfg.get("docker_supported", True))
        self.is_http2_target = bool(raw_cfg.get("is_http2_target", False))
        self.is_server_supported = bool(
            raw_cfg.get("server_cmd") is not None and not self.is_http2_target
        )
        self.use_contract = bool(use_contract)
        if role_override:
            self.default_role = role_override

        self.client_cwd = raw_cfg.get("client_cwd")
        self.server_cwd = raw_cfg.get("server_cwd")
        self.http2_cwd = raw_cfg.get("http2_cwd")
        self._global_env = dict(raw_cfg.get("global_env") or {})

        self._client_cmd_spec = CommandSpec.from_dict(raw_cfg.get("client_cmd"))
        self._server_cmd_spec = CommandSpec.from_dict(raw_cfg.get("server_cmd"))
        self._http2_client_cmd_spec = CommandSpec.from_dict(
            raw_cfg.get("http2_client_cmd")
        )

        self._contract_client_spec = None
        self._contract_server_spec = None
        self._contract_http2_spec = None
        if self.docker_supported:
            if self._client_cmd_spec is not None:
                extra_flags = [
                    tok
                    for tok in self._client_cmd_spec.command[1:]
                    if tok.startswith("--use_")
                ]
                self._contract_client_spec = CommandSpec(
                    ["/interop/client"] + extra_flags
                )
            if self._server_cmd_spec is not None:
                srv_mode = (
                    "ignore_args"
                    if self._server_cmd_spec.args_mode == "ignore_args"
                    else "append"
                )
                self._contract_server_spec = CommandSpec(
                    ["/interop/server"], args_mode=srv_mode
                )
            if self._http2_client_cmd_spec is not None:
                self._contract_http2_spec = CommandSpec(
                    ["/interop/http2_client"]
                )

        self.client_capabilities = _parse_capability_set(
            raw_cfg.get("client_capabilities", [])
        )
        self.server_capabilities = _parse_capability_set(
            raw_cfg.get("server_capabilities", [])
        )

    def has_capability(self, capability, role=None):
        cap = _parse_capability(capability)
        effective_role = role or self.default_role
        caps = (
            self.server_capabilities
            if effective_role == "server"
            else self.client_capabilities
        )
        return cap in caps

    def supports_transport_security(self, transport_security, role=None):
        effective_role = role or self.default_role
        caps = (
            self.server_capabilities
            if effective_role == "server"
            else self.client_capabilities
        )
        t_cap = TRANSPORT_SECURITY_CAPABILITIES.get(transport_security)
        return t_cap is not None and t_cap in caps

    def supports(
        self,
        test_case,
        transport_security="tls",
        role=None,
        skip_compute_engine_creds=False,
    ):
        effective_role = role or self.default_role
        if effective_role == "server":
            return check_server_supports(
                self.server_capabilities, test_case, transport_security
            )
        return check_client_supports(
            self.client_capabilities,
            test_case,
            transport_security=transport_security,
            skip_compute_engine_creds=skip_compute_engine_creds,
        )

    def supports_client(
        self,
        test_case,
        transport_security="tls",
        skip_compute_engine_creds=False,
    ):
        return self.supports(
            test_case,
            transport_security=transport_security,
            role="client",
            skip_compute_engine_creds=skip_compute_engine_creds,
        )

    def supports_server(self, test_case, transport_security="tls"):
        return self.supports(
            test_case, transport_security=transport_security, role="server"
        )

    def client_cmd(self, args):
        if self.use_contract and self._contract_client_spec is not None:
            return self._contract_client_spec.render(args)
        if self._client_cmd_spec is None:
            raise ValueError(
                "Language %r does not define client_cmd" % self.name
            )
        return self._client_cmd_spec.render(args)

    def _render_server_cmd(self, args):
        if self.use_contract and self._contract_server_spec is not None:
            return self._contract_server_spec.render(args)
        return self._server_cmd_spec.render(args)

    @property
    def server_cmd(self):
        if self._server_cmd_spec is None:
            raise AttributeError(
                "Language %r does not define server_cmd" % self.name
            )
        return self._render_server_cmd

    def _render_http2_client_cmd(self, args):
        if self.use_contract and self._contract_http2_spec is not None:
            return self._contract_http2_spec.render(args)
        return self._http2_client_cmd_spec.render(args)

    @property
    def client_cmd_http2interop(self):
        if self._http2_client_cmd_spec is None:
            raise AttributeError(
                "Language %r does not define http2_client_cmd" % self.name
            )
        return self._render_http2_client_cmd

    def contract_client_cmd(self, args):
        if self._contract_client_spec is None:
            return self.client_cmd(args)
        return self._contract_client_spec.render(args)

    def contract_server_cmd(self, args):
        if self._contract_server_spec is None:
            return self.server_cmd(args)
        return self._contract_server_spec.render(args)

    def contract_http2_client_cmd(self, args):
        if self._contract_http2_spec is None:
            return self.client_cmd_http2interop(args)
        return self._contract_http2_spec.render(args)

    def global_env(self):
        return dict(self._global_env)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)


_LANGUAGES = {}
Http2Client = None
Http2Server = None
_LANG_DEFAULTS = _CONFIG["language_defaults"]

for _lname, _raw_lcfg in _CONFIG["languages"].items():
    _lcfg = _resolve_language_config(_lname, _raw_lcfg, _LANG_DEFAULTS)
    if _lcfg.get("is_http2_target"):

        def _make_http2_endpoint(role, name=_lname, cfg=dict(_lcfg)):
            def _factory(use_contract=False):
                return ConfigurableLanguage(
                    name, cfg, role_override=role, use_contract=use_contract
                )

            return _factory

        Http2Client = _make_http2_endpoint("client")
        Http2Server = _make_http2_endpoint("server")
    else:
        _LANGUAGES[_lname] = ConfigurableLanguage(_lname, _lcfg)

# Languages accepted by -s/--server and active cloud_to_cloud servers
_SERVER_CHOICE_NAMES = [
    name for name, lang in _LANGUAGES.items() if hasattr(lang, "server_cmd")
]
_SERVERS = [
    name
    for name in _SERVER_CHOICE_NAMES
    if _LANGUAGES[name].has_capability(Capability.CORE_INTEROP, role="server")
]

_GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES = {
    tc_name: tc_spec["grpc_client_test_case"]
    for tc_name, tc_spec in _HTTP2_SERVER_TEST_CASE_SPECS.items()
    if tc_spec.get("grpc_client_test_case")
}

_HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS = list(
    _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES.keys()
)

_TRANSPORT_SECURITY_OPTIONS = list(_TRANSPORT_SECURITY_SPECS.keys())
_CUSTOM_CREDENTIALS_TYPE_OPTIONS = list(_CUSTOM_CREDENTIALS_SPECS.keys())
prod_servers = dict(_CONFIG["prod_servers"])


def _resolve_language_obj(language):
    if isinstance(language, str):
        return _LANGUAGES.get(language)
    return language


def get_server_endpoint(server_name):
    """Returns a capability-aware server endpoint for `server_name`."""
    if server_name in _LANGUAGES:
        return _LANGUAGES[server_name]
    return ExternalServerEndpoint(server_name)


def docker_run_cmdline(
    cmdline, image, docker_args=None, cwd=None, environ=None
):
    """Wraps given cmdline array to create 'docker run' cmdline from it."""
    if docker_args is None:
        docker_args = []
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


def manual_cmdline(docker_cmdline, docker_image):
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


def write_cmdlog_maybe(cmdlog, filename):
    """Writes manual command log script if non-empty."""
    if cmdlog:
        with open(filename, "w") as logfile:
            logfile.write("#!/bin/bash\n")
            logfile.write("# DO NOT MODIFY\n")
            logfile.write(
                "# This file is generated by tools/interop/runner.py\n"
            )
            logfile.writelines("%s\n" % line for line in cmdlog)
        print("Command log written to file %s" % filename)


def _quote_shell_token(token):
    if (
        token.startswith('--args="')
        or token.startswith("--service_config_json='")
        or "${SERVER_PORT}" in token
        or " && " in token
        or "test/http2_test/" in token
    ):
        return token
    if token.startswith("--") and "=" in token:
        flag, val = token.split("=", 1)
        return "%s=%s" % (flag, shlex.quote(val))
    return shlex.quote(token)


def bash_cmdline(cmdline):
    """Creates bash -c cmdline from args list."""
    return ["bash", "-c", " ".join(_quote_shell_token(tok) for tok in cmdline)]


def auth_options(
    language,
    test_case,
    google_default_creds_use_key_file,
    service_account_key_file,
    default_service_account,
):
    """Returns (cmdline, env) tuple with cloud_to_prod_auth test options."""
    lang_obj = _resolve_language_obj(language)
    cmdargs = []
    env = {}

    oauth_scope_arg = "--oauth_scope=%s" % _OAUTH_SCOPE
    key_file_arg = "--service_account_key_file=%s" % service_account_key_file
    default_account_arg = (
        "--default_service_account=%s" % default_service_account
    )

    auth_spec = _AUTH_TEST_CASE_SPECS.get(test_case, {})
    if auth_spec.get("uses_service_account_key"):
        if lang_obj is not None and lang_obj.has_capability(
            Capability.AUTH_KEY_FILE_ENV, role="client"
        ):
            env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
        else:
            cmdargs += [key_file_arg]

    if auth_spec.get("uses_oauth_scope"):
        cmdargs += [oauth_scope_arg]

    def_acct_cap = auth_spec.get("uses_default_service_account_capability")
    if (
        def_acct_cap
        and lang_obj is not None
        and lang_obj.has_capability(
            _parse_capability(def_acct_cap), role="client"
        )
    ):
        cmdargs += [default_account_arg]

    if auth_spec.get("uses_default_service_account"):
        if (
            auth_spec.get("uses_service_account_key_when_flag")
            == "google_default_creds_use_key_file"
            and google_default_creds_use_key_file
        ):
            env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
        cmdargs += [default_account_arg]

    return (cmdargs, env)


def _job_kill_handler(job):
    if getattr(job._spec, "container_name", None):
        dockerjob.docker_kill(job._spec.container_name)


def cloud_to_prod_jobspec(
    language,
    test_case,
    server_host_nickname,
    server_host,
    google_default_creds_use_key_file,
    docker_image=None,
    auth=False,
    manual_cmd_log=None,
    service_account_key_file=None,
    default_service_account=None,
    transport_security="tls",
    allow_flakes=False,
    use_contract_entrypoints=False,
):
    """Creates jobspec for cloud-to-prod interop test."""
    container_name = None
    cmdargs = [
        "--server_host=%s" % server_host,
        "--server_port=443",
        "--test_case=%s" % test_case,
    ]
    cc_cfg = _CUSTOM_CREDENTIALS_SPECS.get(transport_security)
    if cc_cfg and language.supports_transport_security(
        transport_security, role="client"
    ):
        transport_security_options = list(cc_cfg["client_flags"])
    else:
        print(
            "Invalid transport security option %s in cloud_to_prod_jobspec."
            " Lang: %s" % (transport_security, str(language))
        )
        sys.exit(1)
    cmdargs = cmdargs + transport_security_options
    environ = language.global_env()
    if auth:
        auth_cmdargs, auth_env = auth_options(
            language,
            test_case,
            google_default_creds_use_key_file,
            service_account_key_file,
            default_service_account,
        )
        cmdargs += auth_cmdargs
        environ.update(auth_env)

    use_contract = bool(use_contract_entrypoints)
    if use_contract and getattr(language, "docker_supported", True):
        raw_cmd = language.contract_client_cmd(cmdargs)
        cwd = None
    else:
        raw_cmd = language.client_cmd(cmdargs)
        cwd = language.client_cwd
    cmdline = bash_cmdline(raw_cmd)

    if docker_image and getattr(language, "docker_supported", True):
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

    effective_allow_flakes = bool(allow_flakes)
    suite_name = "cloud_to_prod_auth" if auth else "cloud_to_prod"
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
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=_FLAKE_RETRIES if effective_allow_flakes else 0,
        timeout_retries=_TIMEOUT_RETRIES if effective_allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image and getattr(language, "docker_supported", True):
        test_job.container_name = container_name
    return test_job


def cloud_to_cloud_jobspec(
    language,
    test_case,
    server_name,
    server_host,
    server_port,
    docker_image=None,
    transport_security="tls",
    manual_cmd_log=None,
    allow_flakes=False,
    use_contract_entrypoints=False,
):
    """Creates jobspec for cloud-to-cloud interop test."""
    interop_only_options = list(_C2C_CLIENT_EXTRA_FLAGS)
    ts_cfg = _TRANSPORT_SECURITY_SPECS.get(transport_security)
    if ts_cfg:
        interop_only_options += list(ts_cfg["client_flags"])
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
    req_caps = TEST_CASE_REQUIRED_CAPABILITIES.get(client_test_case)
    if (
        req_caps is None
        or not req_caps.issubset(language.client_capabilities)
        or (
            Capability.SINGLE_INVOCATION_SUITE in language.client_capabilities
            and client_test_case in _TEST_CASES[1:]
        )
    ):
        print(
            "asking client %s to run unimplemented test case %s"
            % (repr(language), client_test_case)
        )
        sys.exit(1)

    tc_spec = _STANDARD_TEST_CASE_SPECS.get(
        test_case
    ) or _DEDICATED_SERVER_TEST_CASE_SPECS.get(test_case)
    if tc_spec and tc_spec.get("extra_cloud_to_cloud_flags"):
        interop_only_options += list(tc_spec["extra_cloud_to_cloud_flags"])

    common_options = [
        "--test_case=%s" % client_test_case,
        "--server_host=%s" % server_host,
        "--server_port=%s" % server_port,
    ]

    use_contract = bool(use_contract_entrypoints)
    use_docker_contract = use_contract and getattr(
        language, "docker_supported", True
    )
    if test_case in _HTTP2_SERVER_TEST_CASES:
        if test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
            client_options = interop_only_options + common_options
            if use_docker_contract:
                cmdline = bash_cmdline(
                    language.contract_client_cmd(client_options)
                )
                cwd = None
            else:
                cmdline = bash_cmdline(language.client_cmd(client_options))
                cwd = language.client_cwd
        else:
            if use_docker_contract:
                cmdline = bash_cmdline(
                    language.contract_http2_client_cmd(common_options)
                )
                cwd = None
            else:
                cmdline = bash_cmdline(
                    language.client_cmd_http2interop(common_options)
                )
                cwd = language.http2_cwd
    else:
        full_opts = common_options + interop_only_options
        if use_docker_contract:
            cmdline = bash_cmdline(language.contract_client_cmd(full_opts))
            cwd = None
        else:
            cmdline = bash_cmdline(language.client_cmd(full_opts))
            cwd = language.client_cwd

    environ = language.global_env()
    if docker_image and getattr(language, "docker_supported", True):
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

    effective_allow_flakes = bool(allow_flakes)
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="cloud_to_cloud:%s:%s_server:%s:%s"
        % (language, server_name, test_case, transport_security),
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=_FLAKE_RETRIES if effective_allow_flakes else 0,
        timeout_retries=_TIMEOUT_RETRIES if effective_allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image and getattr(language, "docker_supported", True):
        test_job.container_name = container_name
    return test_job


def server_jobspec(
    language,
    docker_image,
    transport_security="tls",
    manual_cmd_log=None,
    use_contract_entrypoints=False,
    extra_server_flags=None,
):
    """Creates jobspec for running a server container."""
    container_name = dockerjob.random_name(
        "interop_server_%s" % language.safename
    )
    server_cmd = ["--port=%s" % _DEFAULT_SERVER_PORT]
    ts_cfg = _TRANSPORT_SECURITY_SPECS.get(transport_security)
    if ts_cfg:
        server_cmd += list(ts_cfg["server_flags"])
    else:
        print(
            "Invalid transport security option %s in server_jobspec."
            % transport_security
        )
        sys.exit(1)
    server_cmd += list(extra_server_flags or [])

    use_contract = bool(use_contract_entrypoints)
    if use_contract and getattr(language, "docker_supported", True):
        cmdline = bash_cmdline(language.contract_server_cmd(server_cmd))
        server_cwd = None
    else:
        cmdline = bash_cmdline(language.server_cmd(server_cmd))
        server_cwd = language.server_cwd

    environ = language.global_env()
    docker_args = ["--name=%s" % container_name]
    if language.has_capability(Capability.HTTP2_SERVER_INTEROP, role="server"):
        docker_args += list(
            itertools.chain.from_iterable(
                ("-p", str(_DEFAULT_SERVER_PORT + i))
                for i in range(len(_HTTP2_SERVER_TEST_CASES))
            )
        )
        docker_args += [
            "--health-cmd=python test/http2_test/http2_server_health_check.py "
            "--server_host=%s --server_port=%d"
            % ("localhost", _DEFAULT_SERVER_PORT),
            "--health-interval=1s",
            "--health-retries=5",
            "--health-timeout=10s",
        ]
    else:
        docker_args += ["-p", str(_DEFAULT_SERVER_PORT)]

    docker_cmdline = docker_run_cmdline(
        cmdline,
        image=docker_image,
        cwd=server_cwd,
        environ=environ,
        docker_args=docker_args,
    )
    if manual_cmd_log is not None:
        if manual_cmd_log == []:
            manual_cmd_log.append(
                'echo "Testing ${docker_image:=%s}"' % docker_image
            )
        manual_cmd_log.append(manual_cmdline(docker_cmdline, docker_image))
    server_job = jobset.JobSpec(
        cmdline=docker_cmdline,
        environ=environ,
        shortname="interop_server_%s" % language,
        timeout_seconds=_SERVER_TIMEOUT,
    )
    server_job.container_name = container_name
    return server_job


def _read_propagate_env_args(repo_root, env):
    """Reads tools/run_tests/dockerize/docker_propagate_env.list into --build-arg flags."""
    args_out = []
    env_list_path = os.path.join(
        repo_root, "tools/run_tests/dockerize/docker_propagate_env.list"
    )
    if not os.path.isfile(env_list_path):
        return args_out
    with open(env_list_path, "r") as f:
        for raw_line in f:
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue
            if "=" in line:
                args_out.extend(["--build-arg", line])
            elif env.get(line):
                args_out.extend(["--build-arg", "%s=%s" % (line, env[line])])
    return args_out


def build_docker_image(image_tag, base_name, environ=None, dry_run=False):
    """Builds an interop Docker image from its two-stage Dockerfile via BuildKit.

    The `AS base` stage is the pinned toolchain image named by the
    `.current_version` file; the `AS interop` stage builds the interop binaries
    from the named build contexts. Dockerfiles without an interop stage (every
    gRPC release cut before tools/interop/ existed) are rejected: those
    releases are built by their own copy of interop-last's image builder,
    which matrix-build invokes directly.
    """
    env = dict(os.environ)
    if environ:
        env.update(environ)
    if not image_tag:
        image_tag = env.get("INTEROP_IMAGE", "")
    if not base_name:
        base_name = env.get("BASE_NAME", "")
    if not image_tag or not base_name:
        raise ValueError("image_tag and base_name are required")

    grpc_root = env.get("GRPC_ROOT") or ROOT
    dockerfile_root = env.get("INTEROP_DOCKERFILE_ROOT") or grpc_root
    base_image_dir = os.path.join(
        dockerfile_root, "tools/dockerfile/interoptest", base_name
    )
    if not os.path.isdir(base_image_dir):
        base_image_dir = os.path.join(
            ROOT, "tools/dockerfile/interoptest", base_name
        )
    df_path = os.path.join(base_image_dir, "Dockerfile")
    df_text = ""
    if os.path.isfile(df_path):
        with open(df_path, "r") as df:
            df_text = df.read()
    if _INTEROP_STAGE_MARKER not in df_text.splitlines():
        raise ValueError(
            "%s has no %r stage. Release checkouts without one are built by"
            " their own %s (runner.py matrix-build does this)."
            % (df_path, _INTEROP_STAGE_MARKER, _LEGACY_IMAGE_BUILDER)
        )

    ver_candidates = [
        base_image_dir + ".current_version",
        os.path.join(
            ROOT, "tools/dockerfile/interoptest", base_name + ".current_version"
        ),
    ]
    base_image = "base"
    for vpath in ver_candidates:
        if os.path.isfile(vpath):
            with open(vpath, "r") as vf:
                base_image = vf.read().strip()
            break

    release_label = env.get("RELEASE_LABEL", "")
    with tempfile.TemporaryDirectory() as temp_dir:
        empty_ctx = os.path.join(temp_dir, "empty")
        build_info_ctx = os.path.join(temp_dir, "build_info")
        os.makedirs(empty_ctx)
        os.makedirs(build_info_ctx)

        build_info_file = env.get("BUILD_INFO_FILE", "")
        if build_info_file and os.path.isfile(build_info_file):
            shutil.copy2(
                build_info_file,
                os.path.join(build_info_ctx, os.path.basename(build_info_file)),
            )

        home_sa = os.path.join(os.path.expanduser("~"), "service_account")
        service_account_ctx = home_sa if os.path.isdir(home_sa) else empty_ctx

        build_args = [
            "--target",
            "interop",
            "--tag",
            image_tag,
            "--build-arg",
            "BASE_IMAGE=%s" % base_image,
            "--build-arg",
            "RELEASE_LABEL=%s" % release_label,
        ]
        for ctx_name, ctx_spec in _BUILD_CONTEXTS.items():
            raw_dir = env.get(ctx_spec["env_var"])
            if not raw_dir:
                raw_dir = os.path.abspath(
                    os.path.join(grpc_root, ctx_spec["default_rel_path"])
                )
            ctx_dir = raw_dir if os.path.isdir(raw_dir) else empty_ctx
            build_args.extend(
                ["--build-context", "%s=%s" % (ctx_name, ctx_dir)]
            )
        build_args.extend(
            [
                "--build-context",
                "service_account=%s" % service_account_ctx,
                "--build-context",
                "build_info=%s" % build_info_ctx,
            ]
        )
        if release_label:
            build_args.extend(["--label", "release=%s" % release_label])

        build_args.extend(_read_propagate_env_args(ROOT, env))
        extra_args = env.get("BUILD_INTEROP_DOCKER_EXTRA_ARGS", "")
        if extra_args:
            build_args.extend(shlex.split(extra_args))

        if dry_run or env.get("INTEROP_BUILD_DRY_RUN"):
            print(
                "DRY_RUN: docker buildx build --load %s %s"
                % (" ".join(build_args), base_image_dir)
            )
            return (
                ["docker", "buildx", "build", "--load"]
                + build_args
                + [base_image_dir]
            )

        has_buildx = (
            subprocess.call(
                ["docker", "buildx", "version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            == 0
        )
        if has_buildx:
            cmd = ["docker", "buildx", "build", "--load"]
            if (
                subprocess.call(
                    ["docker", "buildx", "inspect", "default"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                == 0
            ):
                cmd.extend(["--builder", "default"])
            cmd.extend(build_args)
            cmd.append(base_image_dir)
            subprocess.check_call(cmd, env=env)
        else:
            env_bk = dict(env)
            env_bk["DOCKER_BUILDKIT"] = "1"
            cmd = ["docker", "build"] + build_args + [base_image_dir]
            subprocess.check_call(cmd, env=env_bk)
        print("Successfully built image %s" % image_tag)
        return 0


def build_interop_image_jobspec(language, tag=None):
    """Creates jobspec for building interop docker image for a language."""
    if not tag:
        tag = "grpc_interop_%s:%s" % (language.safename, uuid.uuid4())
    base_name = "grpc_interop_%s" % language.safename
    env = {
        "INTEROP_IMAGE": tag,
        "BASE_NAME": base_name,
    }
    build_job = jobset.JobSpec(
        cmdline=[
            sys.executable,
            os.path.abspath(__file__),
            "build-image",
            "--image",
            tag,
            "--base-name",
            base_name,
        ],
        environ=env,
        shortname="build_docker_%s" % (language),
        timeout_seconds=_DOCKER_BUILD_TIMEOUT,
    )
    build_job.tag = tag
    return build_job


def prepare_docker_build_environment(
    languages_to_build, grpc_root=None, environ=None
):
    """Ensures external language repositories and CI keystore credentials exist before Docker builds."""
    env = dict(os.environ)
    if environ:
        env.update(environ)
    root_dir = grpc_root or env.get("GRPC_ROOT") or ROOT

    keystore_dir = env.get("KOKORO_KEYSTORE_DIR")
    if keystore_dir:
        keystore_key = os.path.join(
            keystore_dir, "73836_interop_to_prod_tests_service_account_key"
        )
        if os.path.isfile(keystore_key):
            home_sa = os.path.join(os.path.expanduser("~"), "service_account")
            os.makedirs(home_sa, exist_ok=True)
            shutil.copy2(
                keystore_key,
                os.path.join(
                    home_sa,
                    os.path.basename(_DEFAULT_SERVICE_ACCOUNT_KEY_FILE),
                ),
            )

    seen_contexts = set()
    for lang in sorted(languages_to_build, key=str):
        if not getattr(lang, "docker_supported", True):
            continue
        repo_url = getattr(lang, "repo", None)
        if not repo_url:
            continue
        ctx_name, default_spec = _repo_to_build_context(repo_url)
        if ctx_name == "grpc" or ctx_name in seen_contexts:
            continue
        seen_contexts.add(ctx_name)
        ctx_spec = _BUILD_CONTEXTS.get(ctx_name, default_spec)
        target_dir = env.get(ctx_spec["env_var"])
        if not target_dir:
            target_dir = os.path.abspath(
                os.path.join(root_dir, ctx_spec["default_rel_path"])
            )
        if not os.path.isdir(target_dir):
            subprocess.check_call(
                ["git", "clone", "--recursive", repo_url, target_dir]
            )
            subprocess.check_call(["git", "rev-parse", "HEAD"], cwd=target_dir)


def create_docker_build_jobs(languages_to_build):
    """Deduplicates Docker image build jobs by `safename`."""
    docker_images = {}
    build_jobs = []
    by_safename = {}
    for l in languages_to_build:
        if not getattr(l, "docker_supported", True):
            continue
        by_safename.setdefault(l.safename, []).append(l)

    for safename in sorted(by_safename.keys()):
        group = by_safename[safename]
        canonical = next(
            (item for item in group if str(item) == safename), group[0]
        )
        job = build_interop_image_jobspec(canonical)
        build_jobs.append(job)
        for item in group:
            docker_images[str(item)] = job.tag

    return build_jobs, docker_images


def aggregate_http2_results(stdout):
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
    total = passed + failed
    return {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "failed_cases": ", ".join(failed_cases),
        "percent": (1.0 * passed / total) if total else 0.0,
    }


def _split_host_port(host_port):
    try:
        parsed = urllib.parse.urlsplit("//" + host_port)
        port_num = parsed.port
    except ValueError:
        return None, None
    if (
        not parsed.hostname
        or port_num is None
        or port_num < 1
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None, None
    host = (
        "[%s]" % parsed.hostname if ":" in parsed.hostname else parsed.hostname
    )
    return host, str(port_num)


def _parse_override_server(kv):
    if "=" not in kv:
        raise argparse.ArgumentTypeError(
            "Invalid --override_server value %r: expected SERVER=HOST:PORT" % kv
        )
    server_name, host_port = kv.split("=", 1)
    host, port = _split_host_port(host_port)
    if not server_name or host is None or port is None:
        raise argparse.ArgumentTypeError(
            "Invalid --override_server value %r: expected SERVER=HOST:PORT" % kv
        )
    return [server_name, host_port]


def build_interop_argparser():
    parser = argparse.ArgumentParser(description="Run interop tests.")
    parser.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(_LANGUAGES),
        nargs="+",
        default=["all"],
        help="Clients to run. Objc client can be only run on OSX.",
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
        choices=["all"] + sorted(set(_SERVER_CHOICE_NAMES) | set(_SERVERS)),
        nargs="+",
        help="Run cloud_to_cloud servers in a separate docker "
        + "image. Servers can only be started automatically if "
        + "--use_docker option is enabled.",
        default=[],
    )
    parser.add_argument(
        "--override_server",
        action="append",
        type=_parse_override_server,
        help=(
            "Use servername=HOST:PORT to explicitly specify a server. E.g."
            " csharp=localhost:50000"
        ),
        default=[],
    )
    parser.add_argument(
        "--service_account_key_file",
        type=str,
        help="The service account key file to use for some auth interop tests.",
        default=_DEFAULT_SERVICE_ACCOUNT_KEY_FILE,
    )
    parser.add_argument(
        "--default_service_account",
        type=str,
        help=(
            "Default GCE service account email to use for some auth interop"
            " tests."
        ),
        default=_DEFAULT_SERVICE_ACCOUNT,
    )
    parser.add_argument(
        "-t",
        "--travis",
        default=False,
        action="store_const",
        const=True,
        help=(
            "When set, indicates that the script is running on CI (= not"
            " locally)."
        ),
    )
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_const", const=True
    )
    parser.add_argument(
        "--use_docker",
        default=False,
        action="store_const",
        const=True,
        help="Run all the interop tests under docker. That provides "
        + "additional isolation and prevents the need to install "
        + "language specific prerequisites. Only available on Linux.",
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
        help="Prepare things for running interop tests manually. "
        + "Preserve docker images after building them and skip "
        "actually running the tests. Only print commands to run by " + "hand.",
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
    parser.add_argument(
        "--contract-entrypoints",
        dest="contract_entrypoints",
        default=False,
        action="store_true",
        help="Use standardized /interop/{client,server,http2_client} container entrypoints.",
    )
    return parser


argp = build_interop_argparser()


def run_interop_tests_main(cli_args=None):
    atexit.register(
        lambda: (
            subprocess.call(["stty", "echo"]) if sys.stdin.isatty() else None
        )
    )
    os.chdir(ROOT)
    args = argp.parse_args(cli_args)

    raw_servers = set(
        s
        for s in itertools.chain.from_iterable(
            _SERVERS if x == "all" else [x] for x in args.server
        )
    )
    servers = set(
        s
        for s in raw_servers
        if s in _LANGUAGES
        and _LANGUAGES[s].has_capability(Capability.CORE_INTEROP, role="server")
        and _LANGUAGES[s].supports_transport_security(
            args.transport_security, role="server"
        )
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

    all_default_clients = set(
        k
        for k, l in _LANGUAGES.items()
        if getattr(l, "included_in_all_clients", True)
        and not l.has_capability(
            Capability.SINGLE_INVOCATION_SUITE, role="client"
        )
    )
    languages = set(
        _LANGUAGES[l]
        for l in itertools.chain.from_iterable(
            all_default_clients if x == "all" else [x] for x in args.language
        )
        if _LANGUAGES[l].supports_transport_security(
            args.transport_security, role="client"
        )
    )

    languages_http2_clients_for_http2_server_interop = set()
    if args.http2_server_interop:
        languages_http2_clients_for_http2_server_interop = set(
            _LANGUAGES[l]
            for l in _LANGUAGES
            if _LANGUAGES[l].has_capability(
                Capability.HTTP2_BADSERVER_CLIENT, role="client"
            )
            and ("all" in args.language or l in args.language)
        )

    http2Interop = (
        Http2Client(use_contract=args.contract_entrypoints)
        if args.http2_interop
        else None
    )
    http2InteropServer = (
        Http2Server(use_contract=args.contract_entrypoints)
        if args.http2_server_interop
        else None
    )

    # Filter out any server that supports 0 test cases for the active configuration
    active_servers = set()
    for s in servers:
        srv_ep = get_server_endpoint(s)
        has_c2c = any(
            lang.supports(tc, args.transport_security)
            and srv_ep.supports_server(tc, args.transport_security)
            for lang in languages
            for tc in _TEST_CASES
        )
        has_h2 = bool(
            args.http2_interop
            and any(
                http2Interop.supports(tc, args.transport_security)
                and srv_ep.supports_server(tc, args.transport_security)
                for tc in _HTTP2_TEST_CASES
            )
        )
        if has_c2c or has_h2:
            active_servers.add(s)
    servers = active_servers

    docker_images = {}
    if args.use_docker:
        languages_to_build = set(
            _LANGUAGES[k]
            for k in set([str(l) for l in languages] + [s for s in servers])
        )
        languages_to_build = (
            languages_to_build
            | languages_http2_clients_for_http2_server_interop
        )

        if args.http2_interop:
            languages_to_build.add(http2Interop)

        if args.http2_server_interop:
            languages_to_build.add(http2InteropServer)

        prepare_docker_build_environment(languages_to_build)
        build_jobs, docker_images = create_docker_build_jobs(languages_to_build)

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

    server_jobs = {}
    server_addresses = {}
    try:
        for s in servers:
            lang = str(s)
            spec = server_jobspec(
                _LANGUAGES[lang],
                docker_images.get(lang),
                args.transport_security,
                manual_cmd_log=server_manual_cmd_log,
                use_contract_entrypoints=args.contract_entrypoints,
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
                use_contract_entrypoints=args.contract_entrypoints,
            )
            if not args.manual_run:
                http2_server_job = dockerjob.DockerJob(spec)
                server_jobs[lang] = http2_server_job
            else:
                server_addresses[lang] = ("localhost", "${SERVER_PORT}")

        jobs = []
        if args.cloud_to_prod:
            if args.transport_security not in ["tls"]:
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                prod_server = ProdServerEndpoint(
                    server_host_nickname, prod_servers[server_host_nickname]
                )
                for language in languages:
                    for test_case in _TEST_CASES:
                        for transport_security in args.custom_credentials_type:
                            if language.supports(
                                test_case, transport_security
                            ) and prod_server.supports(
                                test_case, transport_security
                            ):
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
                                    allow_flakes=args.allow_flakes,
                                    use_contract_entrypoints=args.contract_entrypoints,
                                )
                                jobs.append(test_job)
                if args.http2_interop:
                    for test_case in _HTTP2_TEST_CASES:
                        if http2Interop.supports(
                            test_case, args.transport_security
                        ) and prod_server.supports(test_case, "tls"):
                            test_job = cloud_to_prod_jobspec(
                                http2Interop,
                                test_case,
                                server_host_nickname,
                                prod_servers[server_host_nickname],
                                google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                docker_image=docker_images.get(
                                    str(http2Interop)
                                ),
                                manual_cmd_log=client_manual_cmd_log,
                                service_account_key_file=args.service_account_key_file,
                                default_service_account=args.default_service_account,
                                transport_security=args.transport_security,
                                allow_flakes=args.allow_flakes,
                                use_contract_entrypoints=args.contract_entrypoints,
                            )
                            jobs.append(test_job)

        if args.cloud_to_prod_auth:
            if args.transport_security not in ["tls"]:
                print("TLS is always enabled for cloud_to_prod scenarios.")
            for server_host_nickname in args.prod_servers:
                prod_server = ProdServerEndpoint(
                    server_host_nickname, prod_servers[server_host_nickname]
                )
                for language in languages:
                    for test_case in _AUTH_TEST_CASES:
                        auth_spec = _AUTH_TEST_CASE_SPECS[test_case]
                        transport_security = auth_spec.get(
                            "transport_security", "tls"
                        )
                        if (
                            transport_security
                            not in args.custom_credentials_type
                        ):
                            continue
                        if language.supports(
                            test_case,
                            transport_security,
                            skip_compute_engine_creds=args.skip_compute_engine_creds,
                        ) and prod_server.supports(
                            test_case, transport_security
                        ):
                            test_job = cloud_to_prod_jobspec(
                                language,
                                test_case,
                                server_host_nickname,
                                prod_servers[server_host_nickname],
                                google_default_creds_use_key_file=args.google_default_creds_use_key_file,
                                docker_image=docker_images.get(str(language)),
                                auth=True,
                                manual_cmd_log=client_manual_cmd_log,
                                service_account_key_file=args.service_account_key_file,
                                default_service_account=args.default_service_account,
                                transport_security=transport_security,
                                allow_flakes=args.allow_flakes,
                                use_contract_entrypoints=args.contract_entrypoints,
                            )
                            jobs.append(test_job)

        for server in args.override_server:
            if not isinstance(server, (list, tuple)) or len(server) != 2:
                print("Invalid --override_server entry: %r" % (server,))
                sys.exit(1)
            server_name = server[0]
            server_host, server_port = _split_host_port(str(server[1]))
            if not server_name or server_host is None or server_port is None:
                print("Invalid --override_server entry: %r" % (server,))
                sys.exit(1)
            server_addresses[server_name] = (server_host, server_port)

        for server_name, server_address in list(server_addresses.items()):
            (server_host, server_port) = server_address
            server_endpoint = get_server_endpoint(server_name)
            for language in languages:
                for test_case in _TEST_CASES:
                    if language.supports(
                        test_case, args.transport_security
                    ) and server_endpoint.supports_server(
                        test_case, args.transport_security
                    ):
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
                            use_contract_entrypoints=args.contract_entrypoints,
                        )
                        jobs.append(test_job)

            if args.http2_interop:
                for test_case in _HTTP2_TEST_CASES:
                    if http2Interop.supports(
                        test_case, args.transport_security
                    ) and server_endpoint.supports_server(
                        test_case, args.transport_security
                    ):
                        test_job = cloud_to_cloud_jobspec(
                            http2Interop,
                            test_case,
                            server_name,
                            server_host,
                            server_port,
                            docker_image=docker_images.get(str(http2Interop)),
                            transport_security=args.transport_security,
                            manual_cmd_log=client_manual_cmd_log,
                            allow_flakes=args.allow_flakes,
                            use_contract_entrypoints=args.contract_entrypoints,
                        )
                        jobs.append(test_job)

        if args.http2_server_interop:
            if not args.manual_run:
                http2_server_job.wait_for_healthy(
                    timeout_seconds=_HTTP2_SERVER_HEALTHCHECK_TIMEOUT
                )
            for language in languages_http2_clients_for_http2_server_interop:
                for test_case in set(_HTTP2_SERVER_TEST_CASES) - set(
                    _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS
                ):
                    if language.supports(
                        test_case, "tls"
                    ) and http2InteropServer.supports(test_case, "tls"):
                        offset = sorted(_HTTP2_SERVER_TEST_CASES).index(
                            test_case
                        )
                        server_port = _DEFAULT_SERVER_PORT + offset
                        if not args.manual_run:
                            server_port = http2_server_job.mapped_port(
                                server_port
                            )
                        test_job = cloud_to_cloud_jobspec(
                            language,
                            test_case,
                            str(http2InteropServer),
                            "localhost",
                            server_port,
                            docker_image=docker_images.get(str(language)),
                            manual_cmd_log=client_manual_cmd_log,
                            allow_flakes=args.allow_flakes,
                            use_contract_entrypoints=args.contract_entrypoints,
                        )
                        jobs.append(test_job)
            for language in languages:
                for test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
                    if language.supports(
                        test_case, "insecure"
                    ) and http2InteropServer.supports(test_case, "insecure"):
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
                                "Creating grpc client to http2 server test case "
                                "with insecure connection, even though "
                                "args.transport_security is not insecure. Http2 "
                                "test server only supports insecure connections."
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
                            allow_flakes=args.allow_flakes,
                            use_contract_entrypoints=args.contract_entrypoints,
                        )
                        jobs.append(test_job)

        # Test cases that need their own, specially configured server instance.
        for test_case, ds_spec in _DEDICATED_SERVER_TEST_CASE_SPECS.items():
            server_lang = ds_spec["server"]
            if server_lang not in servers:
                continue
            ds_clients = [
                language
                for language in sorted(languages, key=str)
                if language.supports(test_case, args.transport_security)
            ]
            if not ds_clients:
                print(
                    "%s tests will be skipped since none of the supported"
                    " client languages for that test case was specified"
                    % test_case
                )
                continue
            ds_name = ds_spec["server_name"]
            spec = server_jobspec(
                _LANGUAGES[server_lang],
                docker_images.get(server_lang),
                args.transport_security,
                manual_cmd_log=server_manual_cmd_log,
                use_contract_entrypoints=args.contract_entrypoints,
                extra_server_flags=ds_spec["server_flags"],
            )
            ds_port = "${SERVER_PORT}"
            if not args.manual_run:
                ds_job = dockerjob.DockerJob(spec)
                server_jobs[ds_name] = ds_job
                time.sleep(ds_spec.get("server_startup_seconds", 0))
                ds_port = ds_job.mapped_port(_DEFAULT_SERVER_PORT)
            for language in ds_clients:
                test_job = cloud_to_cloud_jobspec(
                    language,
                    test_case,
                    ds_name,
                    "localhost",
                    ds_port,
                    docker_image=docker_images.get(str(language)),
                    transport_security=args.transport_security,
                    manual_cmd_log=client_manual_cmd_log,
                    allow_flakes=args.allow_flakes,
                    use_contract_entrypoints=args.contract_entrypoints,
                )
                jobs.append(test_job)

        if not jobs:
            print("No jobs to run.")
            for image in sorted(set(docker_images.values())):
                dockerjob.remove_image(image, skip_nonexistent=True)
            docker_images.clear()
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
        if args.bq_result_table and resultset and upload_interop_results_to_bq:
            upload_interop_results_to_bq(resultset, args.bq_result_table)
        if num_failures:
            jobset.message("FAILED", "Some tests failed", do_newline=True)
        else:
            jobset.message("SUCCESS", "All tests passed", do_newline=True)

        write_cmdlog_maybe(server_manual_cmd_log, "interop_server_cmds.sh")
        write_cmdlog_maybe(client_manual_cmd_log, "interop_client_cmds.sh")

        report_utils.render_junit_xml_report(resultset, _TESTS_XML_REPORT)

        for name, job in list(resultset.items()):
            if "http2" in name:
                job[0].http2results = aggregate_http2_results(job[0].message)

        if num_failures:
            sys.exit(1)
        else:
            sys.exit(0)
    finally:
        for server, job in list(server_jobs.items()):
            if not job.is_running():
                print('Server "%s" has exited prematurely.' % server)

        dockerjob.finish_jobs([j for j in server_jobs.values()])

        for image in sorted(set(docker_images.values())):
            if not args.manual_run:
                print("Removing docker image %s" % image)
                dockerjob.remove_image(image)
            else:
                print("Preserving docker image: %s" % image)


# ============================================================================
# Backward-compatibility Release Matrix Support (matrix-test / matrix-build)
# ============================================================================

# Force Git to use the traditional "files" reference storage format to ensure
# compatibility with older Git versions inside docker images used for interop
# testing. See https://git-scm.com/docs/git-init for init.defaultRefFormat.
_GIT_INIT_EXTRA_ARGS = ["-c", "init.defaultRefFormat=files"]


class ReleaseInfo:
    """Info about a single release of a language."""

    def __init__(self, patch=None, runtimes=None, testcases_file=None):
        self.patch = list(patch) if patch else []
        self.runtimes = list(runtimes) if runtimes else []
        self.testcases_file = testcases_file

    def __eq__(self, other):
        if not (
            hasattr(other, "patch")
            and hasattr(other, "runtimes")
            and hasattr(other, "testcases_file")
        ):
            return False
        return (
            self.patch == other.patch
            and self.runtimes == other.runtimes
            and self.testcases_file == other.testcases_file
        )

    def __repr__(self):
        return "ReleaseInfo(patch=%r, runtimes=%r, testcases_file=%r)" % (
            self.patch,
            self.runtimes,
            self.testcases_file,
        )


class MatrixTestcaseProfile:
    """Renders historical matrix docker run commands from declarative profile."""

    def __init__(
        self,
        name,
        cmd,
        workdir=DOCKER_WORKDIR_ROOT,
        host_override=False,
        joined_flag=False,
        env=None,
        bash_login=False,
    ):
        self.name = name
        self.cmd = cmd
        self.workdir = workdir or DOCKER_WORKDIR_ROOT
        self.host_override = bool(host_override)
        self.joined_flag = bool(joined_flag)
        self.env = dict(env or {})
        self.bash_login = bool(bash_login)

    def render_commands(
        self, servers, test_cases, use_contract_entrypoints=False
    ):
        env_parts = ["-e %s=%s" % (k, v) for k, v in self.env.items()]
        env_str = (" " + " ".join(env_parts)) if env_parts else ""
        bash_cmd = "bash -l -c" if self.bash_login else "bash -c"
        lines = []
        for srv in servers:
            for tc in test_cases:
                if use_contract_entrypoints:
                    inner = (
                        "/interop/client --server_host=%s"
                        " --server_port=443 --use_tls=true --test_case=%s"
                        % (srv, tc)
                    )
                else:
                    if self.host_override:
                        flags = (
                            "--server_host=%s --server_host_override=%s"
                            " --server_port=443 --use_tls=true"
                            " --test_case=%s" % (srv, srv, tc)
                        )
                    else:
                        flags = (
                            "--server_host=%s --server_port=443"
                            " --test_case=%s --use_tls=true" % (srv, tc)
                        )
                    if self.joined_flag:
                        # The flags are nested inside `bash -c "..."`, so the
                        # inner quotes must be escaped to survive the outer
                        # shell (matches the legacy testcases/* files).
                        inner = '%s --args=\\"%s\\"' % (self.cmd, flags)
                    else:
                        inner = "%s %s" % (self.cmd, flags)
                line = (
                    "docker run -i --rm=true%s -w %s --net=host"
                    ' $docker_image %s "%s"'
                    % (env_str, self.workdir, bash_cmd, inner)
                )
                lines.append(line)
        return lines


_MATRIX_CFG = _CONFIG["matrix"]
_MATRIX_TIMEOUTS = _MATRIX_CFG["timeouts"]
_MATRIX_DEFAULT_DOCKER_PATH = _MATRIX_CFG["docker_path"]
_MATRIX_DEFAULT_GIT_CHECKOUT_ROOT = _MATRIX_CFG["git_checkout_root"]
_MATRIX_MAX_PARALLEL_DOWNLOADS = _MATRIX_CFG["max_parallel_downloads"]
_MATRIX_FLAKE_RETRIES = _MATRIX_CFG["flake_retries"]
_MATRIX_PULL_IMAGE_RETRIES = _MATRIX_CFG["pull_image_retries"]
_MATRIX_TEST_TIMEOUT_SECONDS = _MATRIX_TIMEOUTS["test_seconds"]
_MATRIX_PULL_IMAGE_TIMEOUT_SECONDS = _MATRIX_TIMEOUTS["pull_image_seconds"]
_MATRIX_BUILD_IMAGE_TIMEOUT_SECONDS = _MATRIX_TIMEOUTS["build_image_seconds"]
_MATRIX_BUILD_INFO = "/var/local/build_info"

LANG_RUNTIME_MATRIX = {}
LANG_RELEASE_MATRIX = {}
_MATRIX_REPOS = {}
_MATRIX_BUILD_FROM_RELEASE_TAG = {}
_MATRIX_ROOT_ENV_VAR = {}


def _resolve_language_repo(mlang, mdata):
    candidates = [mlang] + list(mdata.get("runtimes", []))
    for cand in candidates:
        if cand in _LANGUAGES:
            return _LANGUAGES[cand].repo
        for lang_obj in _LANGUAGES.values():
            if lang_obj.safename == cand:
                return lang_obj.repo
    return "https://github.com/grpc/grpc.git"


for _mlang, _mdata in _MATRIX_CFG["languages"].items():
    LANG_RUNTIME_MATRIX[_mlang] = list(_mdata.get("runtimes", [_mlang]))
    _repo_url = _resolve_language_repo(_mlang, _mdata)
    _MATRIX_REPOS[_mlang] = _repo_url
    _ctx_name, _ctx_spec = _repo_to_build_context(_repo_url)
    _MATRIX_BUILD_FROM_RELEASE_TAG[_mlang] = bool(
        _mdata.get("build_from_release_tag", _ctx_name == "grpc")
    )
    _MATRIX_ROOT_ENV_VAR[_mlang] = _mdata.get(
        "root_env_var", _ctx_spec["env_var"]
    )
    _releases_list = list(_mdata["releases"])
    _rel_od = {_rtag: ReleaseInfo() for _rtag in _releases_list}
    for _sel, _ov in _mdata.get("release_overrides", {}).items():
        for _rtag in _expand_release_selector(_sel, _releases_list):
            _rel_od[_rtag] = ReleaseInfo(
                patch=_ov.get("patch"),
                runtimes=_ov.get("runtimes"),
                testcases_file=_ov.get("testcases_file"),
            )
    LANG_RELEASE_MATRIX[_mlang] = _rel_od

_MATRIX_PROFILES = {}
for _pname, _pdata in _MATRIX_CFG.get("custom_entrypoints", {}).items():
    _MATRIX_PROFILES[_pname] = MatrixTestcaseProfile(
        name=_pname,
        cmd=_pdata["cmd"],
        workdir=_pdata.get("workdir", DOCKER_WORKDIR_ROOT),
        host_override=_pdata.get("host_override", False),
        joined_flag=_pdata.get("joined_flag", False),
        env=_pdata.get("env"),
        bash_login=_pdata.get("bash_login", False),
    )


def get_github_repo(lang):
    if lang in _LANGUAGES:
        return _LANGUAGES[lang].repo
    return _MATRIX_REPOS.get(lang, "https://github.com/grpc/grpc.git")


def get_release_tags(lang):
    """Returns list of known releases for given language."""
    return list(LANG_RELEASE_MATRIX[lang].keys())


def get_runtimes_for_lang_release(lang, release):
    """Get list of valid runtimes for given release of lang."""
    runtimes = list(LANG_RUNTIME_MATRIX[lang])
    release_info = LANG_RELEASE_MATRIX[lang].get(release)
    if release_info and release_info.runtimes:
        runtimes = list(release_info.runtimes)
    return runtimes


def should_build_docker_interop_image_from_release_tag(lang):
    return _MATRIX_BUILD_FROM_RELEASE_TAG.get(lang, True)


def get_all_releases():
    return sorted(
        list(
            set(
                release
                for release_dict in LANG_RELEASE_MATRIX.values()
                for release in release_dict.keys()
            )
        )
    )


def resolve_testcases_filename(lang, runtime, release):
    """Resolves the testcase profile/file name for a (lang, runtime, release) tuple."""
    testcases_file = None
    release_info = LANG_RELEASE_MATRIX.get(lang, {}).get(release)
    if release_info:
        testcases_file = release_info.testcases_file
    if not testcases_file:
        testcases_file = "%s__master" % lang

    if runtime != lang and testcases_file.startswith(lang + "__"):
        candidate = runtime + testcases_file[len(lang) :]
        if candidate in _MATRIX_PROFILES:
            testcases_file = candidate
    return testcases_file


def read_test_cases_file(
    lang,
    runtime,
    release,
    use_contract_entrypoints=False,
):
    """Returns the list of matrix test commands for (lang, runtime, release)."""
    testcases_file = resolve_testcases_filename(lang, runtime, release)
    if testcases_file in _MATRIX_PROFILES:
        profile = _MATRIX_PROFILES[testcases_file]
        servers = _MATRIX_CFG.get(
            "servers",
            [
                "grpc-test.sandbox.googleapis.com",
                "grpc-test4.sandbox.googleapis.com",
            ],
        )
        test_cases = _MATRIX_CFG.get("test_cases", _TEST_CASES[:9])
        return profile.render_commands(
            servers,
            test_cases,
            use_contract_entrypoints=use_contract_entrypoints,
        )
    raise ValueError("Unknown matrix testcase profile: %s" % testcases_file)


def get_test_images_for_lang(lang, release_arg, image_path_prefix):
    """Find docker images for a language across releases and runtimes."""
    known_releases = get_release_tags(lang)
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

    images = {}
    for tag in releases:
        for runtime in get_runtimes_for_lang_release(lang, tag):
            image_name = "%s/grpc_interop_%s:%s" % (
                image_path_prefix,
                runtime,
                tag,
            )
            images.setdefault(runtime, []).append((tag, image_name))
    return images


def generate_test_case_jobspecs(
    lang,
    runtime,
    release,
    suite_name,
    server_host=None,
    allow_flakes=False,
    use_contract_entrypoints=False,
):
    """Returns the list of test case JobSpecs for (lang, runtime, release)."""
    testcase_lines = read_test_cases_file(
        lang,
        runtime,
        release,
        use_contract_entrypoints=use_contract_entrypoints,
    )
    timeout_sec = _MATRIX_TEST_TIMEOUT_SECONDS
    flake_retries = _MATRIX_FLAKE_RETRIES if allow_flakes else 0

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
        if server_host:
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


def pull_image_for_lang(lang, image, release):
    """Pull an image for a given language from the image registry."""
    quoted_image = _quote_shell_token(image)
    cmdline = [
        "time docker pull %s && time docker run --rm=true %s /bin/true"
        % (quoted_image, quoted_image)
    ]
    return jobset.JobSpec(
        cmdline=cmdline,
        shortname="pull_image_{}".format(image),
        timeout_seconds=_MATRIX_PULL_IMAGE_TIMEOUT_SECONDS,
        shell=True,
        flake_retries=_MATRIX_PULL_IMAGE_RETRIES,
    )


def cleanup_docker_image(image):
    jobset.message("START", "Cleanup docker image %s" % image, do_newline=True)
    dockerjob.remove_image(image, skip_nonexistent=True)


def add_files_to_image(
    image, with_files, label=None, build_info_dir=_MATRIX_BUILD_INFO
):
    """Add files to a docker image via single-pass docker build."""
    if not with_files and not label:
        return
    temp_dir = tempfile.mkdtemp()
    try:
        ctx_dir = os.path.join(temp_dir, "ctx")
        os.makedirs(ctx_dir)
        copied_names = []
        for fpath in with_files:
            bname = os.path.basename(fpath)
            shutil.copy2(fpath, os.path.join(ctx_dir, bname))
            copied_names.append(bname)
        dockerfile_lines = ["FROM %s" % image]
        for bname in copied_names:
            dockerfile_lines.append(
                "COPY %s %s/%s" % (bname, build_info_dir, bname)
            )
        if label:
            dockerfile_lines.append("LABEL %s" % label)
        dockerfile_path = os.path.join(temp_dir, "Dockerfile")
        with open(dockerfile_path, "w") as df:
            df.write("\n".join(dockerfile_lines) + "\n")
        subprocess.check_call(
            ["docker", "build", "-t", image, "-f", dockerfile_path, ctx_dir]
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def build_matrix_image_jobspec(
    runtime,
    env,
    gcr_tag,
    stack_base,
    lang,
    docker_path=_MATRIX_DEFAULT_DOCKER_PATH,
):
    """Build interop docker image for a language with runtime.

    Release checkouts that ship interop-last's image builder are built with
    that builder, exactly as tools/interop_matrix/create_matrix_images.py did
    (it runs the release's own build_interop.sh in a container with the
    checkout bind-mounted). Other checkouts use `runner.py build-image`.
    """
    basename = "grpc_interop_%s" % runtime
    tag = "%s/%s:%s" % (docker_path, basename, gcr_tag)
    build_env = {"INTEROP_IMAGE": tag, "BASE_NAME": basename}
    build_env.update(env)
    cmdline = [sys.executable, os.path.abspath(__file__), "build-image"]
    if stack_base and should_build_docker_interop_image_from_release_tag(lang):
        release_builder = os.path.join(stack_base, _LEGACY_IMAGE_BUILDER)
        if os.path.isfile(release_builder):
            cmdline = [release_builder]
        else:
            build_env["INTEROP_DOCKERFILE_ROOT"] = stack_base
    build_job = jobset.JobSpec(
        cmdline=cmdline,
        environ=build_env,
        shortname="build_docker_%s" % runtime,
        timeout_seconds=_MATRIX_BUILD_IMAGE_TIMEOUT_SECONDS,
    )
    build_job.tag = tag
    return build_job


def maybe_apply_patches_on_git_tag(
    stack_base, lang, release, patches_base_dir=None
):
    files_to_patch = []
    release_info = LANG_RELEASE_MATRIX[lang].get(release)
    if release_info:
        files_to_patch = release_info.patch
    if not files_to_patch:
        return
    base_dir = patches_base_dir or os.path.join(ROOT, "tools/interop_matrix")
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
    lang,
    release,
    git_checkout,
    git_checkout_root,
    reuse_git_root=False,
    patches_base_dir=None,
):
    """Invokes 'git checkout' for the lang/release and returns directory created."""
    assert git_checkout and git_checkout_root

    if not os.path.exists(git_checkout_root):
        os.makedirs(git_checkout_root)

    repo = get_github_repo(lang)
    repo_dir = os.path.splitext(os.path.basename(repo))[0]
    stack_base = os.path.join(git_checkout_root, repo_dir)

    if not reuse_git_root and os.path.exists(stack_base):
        jobset.message("START", "Removing git checkout root.", do_newline=True)
        shutil.rmtree(stack_base)

    if not os.path.exists(stack_base):
        subprocess.check_call(
            ["git"] + _GIT_INIT_EXTRA_ARGS + ["clone", "--recursive", repo],
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
        ["git"] + _GIT_INIT_EXTRA_ARGS + ["submodule", "update", "--init"],
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


def build_matrix_build_argparser():
    matrix_langs = list(LANG_RUNTIME_MATRIX.keys())
    all_releases = get_all_releases()
    parser = argparse.ArgumentParser(
        description="Run interop docker image build."
    )
    parser.add_argument(
        "--gcr_path",
        "--docker_path",
        default=_MATRIX_DEFAULT_DOCKER_PATH,
        dest="docker_path",
        help="Path of registry where the images are stored",
    )
    parser.add_argument(
        "--release",
        default="master",
        choices=["all", "master"] + all_releases,
        help=(
            "github commit tag to checkout. When building all "
            "releases defined in config.json, specify "
            '"all". Valid only with --git_checkout.'
        ),
    )
    parser.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(matrix_langs),
        nargs="+",
        default=["all"],
        help="Languages to build",
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
        help="reuse the repo dir. If False, the existing git root "
        "directory will removed before a clean checkout, because "
        "reusing the repo can cause git checkout error if you switch "
        "between releases.",
    )
    parser.add_argument(
        "--git_checkout",
        action="store_true",
        help="Use a separate git clone tree for building grpc stack. "
        "Required when using --release flag. By default, current"
        "tree and the sibling language stack will be used for "
        "building.",
    )
    parser.add_argument(
        "--git_checkout_root",
        default=_MATRIX_DEFAULT_GIT_CHECKOUT_ROOT,
        help="Folder under which grpc-go/java/main repo will be cloned. "
        "Valid only with --git_checkout.",
    )
    parser.add_argument(
        "--upload_images",
        action="store_true",
        help="If set, images will be uploaded to container registry after building.",
    )
    return parser


def build_matrix_test_argparser():
    matrix_langs = list(LANG_RUNTIME_MATRIX.keys())
    all_releases = get_all_releases()
    parser = argparse.ArgumentParser(description="Run interop tests.")
    parser.add_argument(
        "-j", "--jobs", default=multiprocessing.cpu_count(), type=int
    )
    parser.add_argument(
        "--gcr_path",
        "--docker_path",
        default=_MATRIX_DEFAULT_DOCKER_PATH,
        dest="docker_path",
        help="Path of docker images in Google Container Registry",
    )
    parser.add_argument(
        "--release",
        default="all",
        choices=["all"] + all_releases,
        help="Release tags to test. When testing all "
        'releases defined in config.json, specify "all".',
    )
    parser.add_argument(
        "-l",
        "--language",
        choices=["all"] + sorted(matrix_langs),
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
        default=_MATRIX_XML_REPORT,
        help="The result file to create.",
    )
    parser.add_argument(
        "--allow_flakes",
        default=False,
        action="store_const",
        const=True,
        help=(
            "Allow flaky tests to show as passing (re-runs failed "
            "tests up to five times)"
        ),
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
        default=None,
        type=str,
        nargs="?",
        help="The gateway to backend services.",
    )
    parser.add_argument(
        "--contract-entrypoints",
        dest="contract_entrypoints",
        default=False,
        action="store_true",
        help="Use standardized /interop/client container entrypoint.",
    )
    return parser


def _run_matrix_release_tests(
    lang,
    runtime,
    release,
    image,
    xml_report_tree,
    skip_tests,
    jobs,
    server_host=None,
    allow_flakes=False,
    bq_result_table="",
    use_contract_entrypoints=False,
):
    total_num_failures = 0
    suite_name = "%s__%s_%s" % (lang, runtime, release)
    job_spec_list = generate_test_case_jobspecs(
        lang,
        runtime,
        release,
        suite_name,
        server_host=server_host,
        allow_flakes=allow_flakes,
        use_contract_entrypoints=use_contract_entrypoints,
    )

    if not job_spec_list:
        jobset.message("FAILED", "No test cases were found.", do_newline=True)
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
            "grpc_interop_matrix",
            suite_name,
            str(uuid.uuid4()),
        )
    return total_num_failures


def _run_matrix_tests_for_lang(
    lang,
    runtime,
    images,
    xml_report_tree,
    jobs,
    keep=False,
    server_host=None,
    allow_flakes=False,
    bq_result_table="",
    use_contract_entrypoints=False,
):
    skip_tests = False
    total_num_failures = 0

    max_pull_jobs = min(jobs, _MATRIX_MAX_PARALLEL_DOWNLOADS)
    max_chunk_size = max_pull_jobs
    chunk_count = (len(images) + max_chunk_size) // max_chunk_size

    for chunk_index in range(chunk_count):
        chunk_start = chunk_index * max_chunk_size
        chunk_size = min(max_chunk_size, len(images) - chunk_start)
        chunk_end = chunk_start + chunk_size
        pull_specs = []
        if not skip_tests:
            for release, image in images[chunk_start:chunk_end]:
                pull_specs.append(pull_image_for_lang(lang, image, release))

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
            total_num_failures += _run_matrix_release_tests(
                lang,
                runtime,
                release,
                image,
                xml_report_tree,
                skip_tests,
                jobs=jobs,
                server_host=server_host,
                allow_flakes=allow_flakes,
                bq_result_table=bq_result_table,
                use_contract_entrypoints=use_contract_entrypoints,
            )
        if not keep:
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


def run_matrix_tests_main(cli_args=None):
    parser = build_matrix_test_argparser()
    parsed_args = parser.parse_args(cli_args)
    matrix_langs = list(LANG_RUNTIME_MATRIX.keys())
    languages = (
        matrix_langs
        if "all" in parsed_args.language
        else list(dict.fromkeys(parsed_args.language))
    )
    total_num_failures = 0
    xml_report_tree = report_utils.new_junit_xml_tree()
    for lang in languages:
        docker_images = get_test_images_for_lang(
            lang, parsed_args.release, parsed_args.docker_path
        )
        for runtime in sorted(docker_images.keys()):
            total_num_failures += _run_matrix_tests_for_lang(
                lang,
                runtime,
                docker_images[runtime],
                xml_report_tree,
                jobs=parsed_args.jobs,
                keep=parsed_args.keep,
                server_host=parsed_args.server_host,
                allow_flakes=parsed_args.allow_flakes,
                bq_result_table=parsed_args.bq_result_table,
                use_contract_entrypoints=parsed_args.contract_entrypoints,
            )

    report_utils.create_xml_report_file(
        xml_report_tree, parsed_args.report_file
    )
    if total_num_failures:
        sys.exit(1)
    sys.exit(0)


def build_all_images_for_release(
    lang,
    release,
    parsed_args,
    docker_images_cleanup,
    patches_base_dir=None,
):
    """Build all docker images for a release across all runtimes."""
    docker_images = []
    build_jobs = []

    env = {}
    stack_base = ""
    if parsed_args.git_checkout:
        stack_base = checkout_grpc_stack(
            lang=lang,
            release=release,
            git_checkout=parsed_args.git_checkout,
            git_checkout_root=parsed_args.git_checkout_root,
            reuse_git_root=parsed_args.reuse_git_root,
            patches_base_dir=patches_base_dir,
        )
        var = _MATRIX_ROOT_ENV_VAR.get(lang, "GRPC_ROOT")
        env[var] = stack_base

    for runtime in get_runtimes_for_lang_release(lang, release):
        job = build_matrix_image_jobspec(
            runtime=runtime,
            env=env,
            gcr_tag=release,
            stack_base=stack_base,
            lang=lang,
            docker_path=parsed_args.docker_path,
        )
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

    if release != "master":
        commit_log = os.path.join(stack_base, "commit_log")
        if os.path.exists(commit_log):
            for image in docker_images:
                add_files_to_image(
                    image,
                    [commit_log],
                    "release=%s" % release,
                    build_info_dir=_MATRIX_BUILD_INFO,
                )
    return docker_images


def build_all_images_for_lang(
    lang,
    parsed_args,
    docker_images_cleanup,
    patches_base_dir=None,
):
    """Build all docker images for a language across releases and runtimes."""
    if not parsed_args.git_checkout:
        if parsed_args.release != "master":
            print(
                "Cannot use --release without also enabling --git_checkout.\n"
            )
            sys.exit(1)
        releases = [parsed_args.release]
    else:
        if parsed_args.release == "all":
            releases = get_release_tags(lang)
        else:
            if parsed_args.release not in ["master"] + get_release_tags(lang):
                jobset.message(
                    "SKIPPED",
                    "%s for %s is not defined" % (parsed_args.release, lang),
                    do_newline=True,
                )
                return []
            releases = [parsed_args.release]

    images = []
    for release in releases:
        images += build_all_images_for_release(
            lang,
            release,
            parsed_args,
            docker_images_cleanup,
            patches_base_dir=patches_base_dir,
        )
    jobset.message(
        "SUCCESS",
        "All docker images built for %s at %s." % (lang, releases),
        do_newline=True,
    )
    return images


def run_matrix_build_main(cli_args=None):
    parser = build_matrix_build_argparser()
    parsed_args = parser.parse_args(cli_args)
    docker_images_cleanup = []

    def _cleanup():
        if not parsed_args.keep:
            for image in docker_images_cleanup:
                dockerjob.remove_image(image, skip_nonexistent=True)

    atexit.register(_cleanup)
    matrix_langs = list(LANG_RUNTIME_MATRIX.keys())
    languages = (
        matrix_langs
        if "all" in parsed_args.language
        else list(dict.fromkeys(parsed_args.language))
    )
    for lang in languages:
        docker_images = build_all_images_for_lang(
            lang, parsed_args, docker_images_cleanup
        )
        for image in docker_images:
            if parsed_args.upload_images:
                jobset.message("START", "Uploading %s" % image, do_newline=True)
                assert (
                    image.startswith(parsed_args.docker_path)
                    and image.find(":") != -1
                )
                (image_name, tag_name) = image.rsplit(":", 1)
                alternate_image = "%s:infrastructure-public-image-%s" % (
                    image_name,
                    tag_name,
                )
                subprocess.call(
                    ["docker", "image", "tag", image, alternate_image]
                )
                subprocess.call(["docker", "push", image])
                subprocess.call(["docker", "push", alternate_image])
            else:
                print(
                    "Not uploading image %s, run with --upload_images to upload."
                    % image
                )
    return 0


def build_image_argparser():
    parser = argparse.ArgumentParser(
        description="Build an interop Docker image via BuildKit."
    )
    parser.add_argument(
        "--image_tag",
        "--image",
        dest="image_tag",
        default="",
        help="Target Docker image tag (defaults to $INTEROP_IMAGE).",
    )
    parser.add_argument(
        "--base_name",
        "--base-name",
        dest="base_name",
        default="",
        help="Interop Dockerfile directory name (defaults to $BASE_NAME).",
    )
    parser.add_argument(
        "--dry_run",
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Print the docker buildx build command without executing it.",
    )
    return parser


def run_build_image_main(cli_args=None):
    parser = build_image_argparser()
    parsed_args = parser.parse_args(cli_args)
    dry_run = parsed_args.dry_run or (
        os.environ.get("BUILD_INTEROP_DRY_RUN") == "1"
    )
    try:
        build_docker_image(
            image_tag=parsed_args.image_tag,
            base_name=parsed_args.base_name,
            dry_run=dry_run,
        )
    except ValueError as err:
        print("build-image: %s" % err, file=sys.stderr)
        return 1
    return 0


_SUBCOMMANDS = {
    "test": run_interop_tests_main,
    "matrix-test": run_matrix_tests_main,
    "matrix-build": run_matrix_build_main,
    "build-image": run_build_image_main,
}


def main(argv=None):
    cli_args = list(sys.argv[1:] if argv is None else argv)
    if cli_args and cli_args[0] in _SUBCOMMANDS:
        subcmd = cli_args[0]
        return _SUBCOMMANDS[subcmd](cli_args[1:])
    return run_interop_tests_main(cli_args)


if __name__ == "__main__":
    sys.exit(main())
