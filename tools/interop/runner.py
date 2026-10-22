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

from collections import OrderedDict
from enum import Enum
import argparse
import atexit
import copy
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
import uuid

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(_THIS_DIR, "../.."))
DEFAULT_CONFIG_PATH = os.path.join(_THIS_DIR, "config.json")

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
    AUTH_KEY_FILE_FLAG = "AUTH_KEY_FILE_FLAG"
    AUTH_KEY_FILE_ENV = "AUTH_KEY_FILE_ENV"
    GOOGLE_DEFAULT_CREDS = "GOOGLE_DEFAULT_CREDS"
    GCE_CHANNEL_CREDS = "GCE_CHANNEL_CREDS"
    GCE_OAUTH2 = "GCE_OAUTH2"
    OAUTH2_DEFAULT_ACCOUNT_ARG = "OAUTH2_DEFAULT_ACCOUNT_ARG"
    HTTP2_INTEROP = "HTTP2_INTEROP"
    HTTP2_SERVER_INTEROP = "HTTP2_SERVER_INTEROP"
    HTTP2_BADSERVER_CLIENT = "HTTP2_BADSERVER_CLIENT"
    HTTP2_BADPEER_CLIENT = "HTTP2_BADPEER_CLIENT"
    TLS = "TLS"
    INSECURE = "INSECURE"
    ALTS = "ALTS"


class TransportSecurity(Enum):
    """Transport security modes supported by gRPC interop suites."""

    TLS = "tls"
    ALTS = "alts"
    INSECURE = "insecure"
    GOOGLE_DEFAULT_CREDENTIALS = "google_default_credentials"
    COMPUTE_ENGINE_CHANNEL_CREDS = "compute_engine_channel_creds"


def _parse_capability(val):
    if isinstance(val, Capability):
        return val
    key = str(val).strip().upper()
    return Capability[key]


def _parse_capability_set(items):
    return frozenset(_parse_capability(x) for x in (items or []))


def load_config(config_path=None):
    """Loads and validates tools/interop/config.json."""
    path = config_path or DEFAULT_CONFIG_PATH
    with open(path, "r") as f:
        data = json.load(f)
    required_keys = (
        "schema_version",
        "capabilities",
        "defaults",
        "prod_servers",
        "transport_security",
        "custom_credentials_types",
        "suites",
        "test_cases",
        "test_case_capabilities",
        "languages",
        "matrix",
    )
    for k in required_keys:
        if k not in data:
            raise ValueError("Missing required key in config.json: %s" % k)
    return data


_CONFIG = load_config()

DOCKER_WORKDIR_ROOT = _CONFIG["defaults"].get(
    "docker_workdir_root", "/var/local/git/grpc"
)
_DEFAULT_SERVER_PORT = int(_CONFIG["defaults"].get("default_server_port", 8080))
_TEST_TIMEOUT = int(_CONFIG["defaults"].get("test_timeout_seconds", 3 * 60))
_DOCKER_BUILD_XML_REPORT = _CONFIG["defaults"].get(
    "docker_build_xml_report", "interop_docker_build/sponge_log.xml"
)
_TESTS_XML_REPORT = _CONFIG["defaults"].get(
    "tests_xml_report", "interop_test/sponge_log.xml"
)

_TEST_CASES = [tc["name"] for tc in _CONFIG["test_cases"]["standard"]]
_AUTH_TEST_CASES = [tc["name"] for tc in _CONFIG["test_cases"]["auth"]]
_HTTP2_TEST_CASES = [tc["name"] for tc in _CONFIG["test_cases"]["http2_client"]]
_HTTP2_SERVER_TEST_CASES = [
    tc["name"] for tc in _CONFIG["test_cases"]["http2_server"]
]

_AUTH_TEST_CASE_SPECS = {
    tc["name"]: tc for tc in _CONFIG["test_cases"]["auth"]
}
_STANDARD_TEST_CASE_SPECS = {
    tc["name"]: tc for tc in _CONFIG["test_cases"]["standard"]
}
_HTTP2_SERVER_TEST_CASE_SPECS = {
    tc["name"]: tc for tc in _CONFIG["test_cases"]["http2_server"]
}

TEST_CASE_REQUIRED_CAPABILITIES = {
    tc: _parse_capability_set(caps["client"])
    for tc, caps in _CONFIG["test_case_capabilities"].items()
}
SERVER_TEST_CASE_REQUIRED_CAPABILITIES = {
    tc: _parse_capability_set(caps["server"])
    for tc, caps in _CONFIG["test_case_capabilities"].items()
}

TRANSPORT_SECURITY_CAPABILITIES = {}
for ts_name, ts_cfg in _CONFIG["transport_security"].items():
    TRANSPORT_SECURITY_CAPABILITIES[ts_name] = _parse_capability(
        ts_cfg["required_capability"]
    )
for cc_name, cc_cfg in _CONFIG["custom_credentials_types"].items():
    TRANSPORT_SECURITY_CAPABILITIES[cc_name] = _parse_capability(
        cc_cfg["required_capability"]
    )

_SKIP_CLIENT_COMPRESSION = [
    "client_compressed_unary",
    "client_compressed_streaming",
]
_SKIP_SERVER_COMPRESSION = [
    "server_compressed_unary",
    "server_compressed_streaming",
]
_SKIP_COMPRESSION = _SKIP_CLIENT_COMPRESSION + _SKIP_SERVER_COMPRESSION
_SKIP_ADVANCED = [
    "status_code_and_message",
    "custom_metadata",
    "unimplemented_method",
    "unimplemented_service",
]
_SKIP_SPECIAL_STATUS_MESSAGE = ["special_status_message"]
_ORCA_TEST_CASES = ["orca_per_rpc", "orca_oob"]
_GOOGLE_DEFAULT_CREDS_TEST_CASE = "google_default_credentials"
_SKIP_GOOGLE_DEFAULT_CREDS = [_GOOGLE_DEFAULT_CREDS_TEST_CASE]
_COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE = "compute_engine_channel_credentials"
_SKIP_COMPUTE_ENGINE_CHANNEL_CREDS = [_COMPUTE_ENGINE_CHANNEL_CREDS_TEST_CASE]
_SKIP_DATA_FRAME_PADDING = ["data_frame_padding"]


class CommandSpec:
    """Renders command tokens from declarative specification in config.json."""

    def __init__(
        self,
        command,
        args_mode="append",
        arg_template=None,
        port_regex=None,
    ):
        self.command = list(command or [])
        self.args_mode = args_mode or "append"
        self.arg_template = arg_template
        self.port_regex = port_regex

    @classmethod
    def from_dict(cls, raw):
        if not raw:
            return None
        if isinstance(raw, list):
            return cls(command=raw, args_mode="append")
        return cls(
            command=raw.get("command", []),
            args_mode=raw.get("args_mode", "append"),
            arg_template=raw.get("arg_template"),
            port_regex=raw.get("port_regex"),
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
            pattern = self.port_regex or r"--server_port=(\d+)"
            for arg in args_list:
                match = re.search(pattern, arg)
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


def compute_unimplemented_client_cases(capabilities):
    """Computes backward-compatible unimplemented_test_cases() from positive client capabilities."""
    unimplemented = []
    seen = set()
    candidates = _TEST_CASES + ["data_frame_padding"] + _AUTH_TEST_CASES
    for tc in candidates:
        if tc in seen:
            continue
        req = TEST_CASE_REQUIRED_CAPABILITIES[tc]
        is_single_suite_skip = (
            Capability.SINGLE_INVOCATION_SUITE in capabilities
            and tc in _TEST_CASES[1:]
        )
        if is_single_suite_skip or not req.issubset(capabilities):
            unimplemented.append(tc)
            seen.add(tc)
    return unimplemented


def compute_unimplemented_server_cases(capabilities):
    """Computes backward-compatible unimplemented_test_cases_server() from positive server capabilities."""
    if not capabilities or Capability.CORE_INTEROP not in capabilities:
        if (
            Capability.HTTP2_SERVER_INTEROP in capabilities
            or Capability.HTTP2_INTEROP in capabilities
        ):
            return list(_TEST_CASES)
        return list(
            _TEST_CASES
            + _AUTH_TEST_CASES
            + _HTTP2_TEST_CASES
            + _HTTP2_SERVER_TEST_CASES
        )
    unimplemented = []
    for tc in _TEST_CASES:
        req = SERVER_TEST_CASE_REQUIRED_CAPABILITIES[tc]
        if not req.issubset(capabilities):
            unimplemented.append(tc)
    return unimplemented


class ServerEndpointView:
    """View of a language endpoint acting in the server role."""

    def __init__(self, language):
        self._language = language

    @property
    def capabilities(self):
        return self._language.server_capabilities

    @property
    def client_capabilities(self):
        return self._language.client_capabilities

    @property
    def server_capabilities(self):
        return self._language.server_capabilities

    def has_capability(self, cap, role="server"):
        return self._language.has_capability(cap, role=role)

    def supports_transport_security(self, transport_security, role="server"):
        return self._language.supports_transport_security(
            transport_security, role=role
        )

    def supports(
        self,
        test_case,
        transport_security="tls",
        role="server",
        skip_compute_engine_creds=False,
    ):
        return self._language.supports(
            test_case,
            transport_security=transport_security,
            role=role,
            skip_compute_engine_creds=skip_compute_engine_creds,
        )

    def unimplemented_test_cases(self):
        return self._language.unimplemented_test_cases()

    def unimplemented_test_cases_server(self):
        return self._language.unimplemented_test_cases_server()

    def __getattr__(self, name):
        return getattr(self._language, name)

    def __str__(self):
        return str(self._language)

    def __repr__(self):
        return "ServerEndpointView(%r)" % self._language


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
        self.client_capabilities = frozenset()

    def has_capability(self, cap, role="server"):
        return _parse_capability(cap) in self.server_capabilities

    def supports_transport_security(self, transport_security, role="server"):
        t_cap = TRANSPORT_SECURITY_CAPABILITIES.get(transport_security)
        return t_cap is not None and t_cap in self.server_capabilities

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

    def __str__(self):
        return self.nickname


class ExternalServerEndpoint:
    """Capability model for custom --override_server targets not in _LANGUAGES."""

    ALL_SERVER_CAPABILITIES = frozenset(Capability)

    def __init__(self, name):
        self.name = name
        self.safename = name
        self.server_capabilities = self.ALL_SERVER_CAPABILITIES
        self.client_capabilities = frozenset()

    def has_capability(self, cap, role="server"):
        return _parse_capability(cap) in self.server_capabilities

    def supports_transport_security(self, transport_security, role="server"):
        t_cap = TRANSPORT_SECURITY_CAPABILITIES.get(transport_security)
        return t_cap is not None and t_cap in self.server_capabilities

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

    def unimplemented_test_cases_server(self):
        return []

    def __str__(self):
        return self.name


class ConfigurableLanguage:
    """Language/endpoint configuration loaded from tools/interop/config.json."""

    default_role = "client"

    def __init__(self, name, raw_cfg, role_override=None, use_contract=False):
        self.name = name
        self._raw = dict(raw_cfg)
        self.safename = raw_cfg.get("safename", name)
        self.repo = raw_cfg.get("repo", "grpc")
        self.in_runner_languages = bool(
            raw_cfg.get("in_runner_languages", True)
        )
        self.included_in_all_clients = bool(
            raw_cfg.get("included_in_all_clients", True)
        )
        self.supports_server = bool(raw_cfg.get("supports_server", False))
        self.docker_supported = bool(raw_cfg.get("docker_supported", True))
        self.is_http2_target = bool(raw_cfg.get("is_http2_target", False))
        self.use_contract = bool(use_contract)
        if role_override:
            self.default_role = role_override

        self.client_cwd = raw_cfg.get("client_cwd")
        self.server_cwd = raw_cfg.get("server_cwd")
        self.http2_cwd = raw_cfg.get("http2_cwd")
        self._global_env = dict(raw_cfg.get("global_env") or {})
        self._cloud_to_prod_env = dict(raw_cfg.get("cloud_to_prod_env") or {})

        self._client_cmd_spec = CommandSpec.from_dict(raw_cfg.get("client_cmd"))
        self._server_cmd_spec = CommandSpec.from_dict(raw_cfg.get("server_cmd"))
        self._http2_client_cmd_spec = CommandSpec.from_dict(
            raw_cfg.get("http2_client_cmd")
        )

        contract_raw = raw_cfg.get("contract_commands") or {}
        self._contract_client_spec = CommandSpec.from_dict(
            contract_raw.get("client")
        )
        self._contract_server_spec = CommandSpec.from_dict(
            contract_raw.get("server")
        )
        self._contract_http2_spec = CommandSpec.from_dict(
            contract_raw.get("http2_client")
        )

        if (
            role_override == "server"
            and "http2_server_client_capabilities" in raw_cfg
        ):
            self.client_capabilities = _parse_capability_set(
                raw_cfg.get("http2_server_client_capabilities", [])
            )
            self.server_capabilities = _parse_capability_set(
                raw_cfg.get("server_capabilities", [])
            )
        elif (
            role_override == "client"
            and "http2_client_server_capabilities" in raw_cfg
        ):
            self.client_capabilities = _parse_capability_set(
                raw_cfg.get("client_capabilities", [])
            )
            self.server_capabilities = _parse_capability_set(
                raw_cfg.get("http2_client_server_capabilities", [])
            )
        else:
            self.client_capabilities = _parse_capability_set(
                raw_cfg.get("client_capabilities", [])
            )
            self.server_capabilities = _parse_capability_set(
                raw_cfg.get("server_capabilities", [])
            )

        self.server_docker_config = raw_cfg.get("server_docker_config")
        self.docker_build = raw_cfg.get("docker_build")

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

    def as_server(self):
        return ServerEndpointView(self)

    def client_cmd(self, args):
        if self.use_contract and self._contract_client_spec is not None:
            return self._contract_client_spec.render(args)
        if self._client_cmd_spec is None:
            raise ValueError(
                "Language %r does not define client_cmd" % self.name
            )
        return self._client_cmd_spec.render(args)

    def server_cmd(self, args):
        if self.use_contract and self._contract_server_spec is not None:
            return self._contract_server_spec.render(args)
        if self._server_cmd_spec is None:
            raise ValueError(
                "Language %r does not define server_cmd" % self.name
            )
        return self._server_cmd_spec.render(args)

    def client_cmd_http2interop(self, args):
        if self.use_contract and self._contract_http2_spec is not None:
            return self._contract_http2_spec.render(args)
        if self._http2_client_cmd_spec is None:
            raise ValueError(
                "Language %r does not define http2_client_cmd" % self.name
            )
        return self._http2_client_cmd_spec.render(args)

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

    def cloud_to_prod_env(self):
        return dict(self._cloud_to_prod_env)

    def unimplemented_test_cases(self):
        return compute_unimplemented_client_cases(self.client_capabilities)

    def unimplemented_test_cases_server(self):
        return compute_unimplemented_server_cases(self.server_capabilities)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)


def _make_language_subclass(lang_name, lang_cfg, role_override=None):
    class_name = lang_cfg.get("legacy_class_name", "Language_" + lang_name)

    def __init__(self, use_contract=False):
        ConfigurableLanguage.__init__(
            self,
            lang_name,
            lang_cfg,
            role_override=role_override,
            use_contract=use_contract,
        )

    cls_dict = {
        "__init__": __init__,
        "default_role": role_override or "client",
    }
    if lang_cfg.get("server_cmd") is None:
        # Hide server_cmd on client-only classes so hasattr(lang, "server_cmd") is False
        cls_dict["server_cmd"] = property(
            lambda self: (_ for _ in ()).throw(AttributeError("server_cmd"))
        )
    if lang_cfg.get("http2_client_cmd") is None:
        cls_dict["client_cmd_http2interop"] = property(
            lambda self: (_ for _ in ()).throw(
                AttributeError("client_cmd_http2interop")
            )
        )
    return type(class_name, (ConfigurableLanguage,), cls_dict)


_LANGUAGE_CLASSES = {}
_LANGUAGES = {}
Http2Client = None
Http2Server = None

for _lname, _lcfg in _CONFIG["languages"].items():
    if _lcfg.get("is_http2_target"):
        Http2Client = _make_language_subclass(
            _lname, _lcfg, role_override="client"
        )
        _srv_cfg = dict(_lcfg)
        _srv_cfg["legacy_class_name"] = "Http2Server"
        Http2Server = _make_language_subclass(
            _lname, _srv_cfg, role_override="server"
        )
        globals()["Http2Client"] = Http2Client
        globals()["Http2Server"] = Http2Server
    else:
        _cls = _make_language_subclass(_lname, _lcfg)
        _LANGUAGE_CLASSES[_lname] = _cls
        if _lcfg.get("legacy_class_name"):
            globals()[_lcfg["legacy_class_name"]] = _cls
        if _lcfg.get("in_runner_languages", True):
            _LANGUAGES[_lname] = _cls()

# Servers supported as cloud_to_cloud servers, derived from server_order and CORE_INTEROP capability
_SERVER_CHOICE_NAMES = list(_CONFIG.get("server_order", list(_LANGUAGES.keys())))
_SERVERS = [
    name
    for name in _SERVER_CHOICE_NAMES
    if name in _LANGUAGES
    and hasattr(_LANGUAGES[name], "server_cmd")
    and _LANGUAGES[name].has_capability(Capability.CORE_INTEROP, role="server")
]

_GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES = {
    tc["name"]: tc["grpc_client_test_case"]
    for tc in _CONFIG["test_cases"]["http2_server"]
    if tc.get("client_kind") == "grpc_client"
    and tc.get("grpc_client_test_case")
}

_HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS = list(
    _GRPC_CLIENT_TEST_CASES_FOR_HTTP2_SERVER_TEST_CASES.keys()
)

_LANGUAGES_WITH_HTTP2_CLIENTS_FOR_HTTP2_SERVER_TEST_CASES = [
    name
    for name in _CONFIG.get(
        "http2_badserver_client_order", list(_LANGUAGES.keys())
    )
    if name in _LANGUAGES
    and _LANGUAGES[name].has_capability(
        Capability.HTTP2_BADSERVER_CLIENT, role="client"
    )
]

_LANGUAGES_FOR_ALTS_TEST_CASES = [
    name
    for name in _CONFIG.get("alts_language_order", list(_LANGUAGES.keys()))
    if name in _LANGUAGES
    and _LANGUAGES[name].has_capability(Capability.ALTS, role="client")
]

_SERVERS_FOR_ALTS_TEST_CASES = [
    name
    for name in _CONFIG.get("alts_language_order", list(_LANGUAGES.keys()))
    if name in _SERVERS
    and _LANGUAGES[name].has_capability(Capability.ALTS, role="server")
]

_TRANSPORT_SECURITY_OPTIONS = list(_CONFIG["transport_security"].keys())
_CUSTOM_CREDENTIALS_TYPE_OPTIONS = list(
    _CONFIG["custom_credentials_types"].keys()
)
prod_servers = dict(_CONFIG["prod_servers"])


def _resolve_language_obj(language):
    if isinstance(language, str):
        return _LANGUAGES.get(language)
    return language


def get_server_endpoint(server_name):
    """Returns a capability-aware server endpoint for `server_name`."""
    if server_name in _LANGUAGES:
        return _LANGUAGES[server_name].as_server()
    return ExternalServerEndpoint(server_name)


def docker_run_cmdline(cmdline, image, docker_args=None, cwd=None, environ=None):
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
                "# This file is generated by"
                " run_interop_tests.py/create_testcases.sh\n"
            )
            logfile.writelines("%s\n" % line for line in cmdlog)
        print("Command log written to file %s" % filename)


def bash_cmdline(cmdline):
    """Creates bash -c cmdline from args list."""
    return ["bash", "-c", " ".join(cmdline)]


def compute_engine_creds_required(language, test_case):
    """Returns True if given test requires access to compute engine creds."""
    lang_obj = _resolve_language_obj(language)
    auth_spec = _AUTH_TEST_CASE_SPECS.get(test_case)
    if not auth_spec:
        return False
    if auth_spec.get("requires_compute_engine_creds"):
        return True
    req_cap_name = auth_spec.get("requires_compute_engine_creds_capability")
    if req_cap_name and lang_obj is not None:
        return lang_obj.has_capability(
            _parse_capability(req_cap_name), role="client"
        )
    return False


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

    oauth_scope_arg = "--oauth_scope=%s" % _CONFIG["defaults"].get(
        "oauth_scope", "https://www.googleapis.com/auth/xapi.zoo"
    )
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
        and (
            lang_obj.has_capability(
                _parse_capability(def_acct_cap), role="client"
            )
            or lang_obj.has_capability(Capability.GCE_OAUTH2, role="client")
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
        if test_case == "compute_engine_creds" and not auth_spec.get(
            "uses_oauth_scope"
        ):
            cmdargs += [oauth_scope_arg, default_account_arg]
        else:
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
    allow_flakes=None,
    use_contract_entrypoints=None,
):
    """Creates jobspec for cloud-to-prod interop test."""
    container_name = None
    cmdargs = [
        "--server_host=%s" % server_host,
        "--server_port=443",
        "--test_case=%s" % test_case,
    ]
    cc_cfg = _CONFIG["custom_credentials_types"].get(transport_security)
    if cc_cfg and language.supports_transport_security(
        transport_security, role="client"
    ):
        transport_security_options = list(cc_cfg["client_flags"])
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
        )
        cmdargs += auth_cmdargs
        environ.update(auth_env)

    use_contract = (
        use_contract_entrypoints
        if use_contract_entrypoints is not None
        else bool(getattr(args, "contract_entrypoints", False))
    )
    if use_contract and hasattr(language, "contract_client_cmd"):
        raw_cmd = language.contract_client_cmd(cmdargs)
        cwd = None
    else:
        raw_cmd = language.client_cmd(cmdargs)
        cwd = language.client_cwd
    cmdline = bash_cmdline(raw_cmd)

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

    effective_allow_flakes = (
        allow_flakes
        if allow_flakes is not None
        else bool(getattr(args, "allow_flakes", False))
    )
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
        flake_retries=4 if effective_allow_flakes else 0,
        timeout_retries=2 if effective_allow_flakes else 0,
        kill_handler=_job_kill_handler,
    )
    if docker_image:
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
    allow_flakes=None,
    use_contract_entrypoints=None,
):
    """Creates jobspec for cloud-to-cloud interop test."""
    interop_only_options = list(
        _CONFIG["suites"]["cloud_to_cloud"].get(
            "client_extra_flags",
            [
                "--server_host_override=foo.test.google.fr",
                "--use_test_ca=true",
            ],
        )
    )
    ts_cfg = _CONFIG["transport_security"].get(transport_security)
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
    if client_test_case in language.unimplemented_test_cases():
        print(
            "asking client %s to run unimplemented test case %s"
            % (repr(language), client_test_case)
        )
        sys.exit(1)

    std_spec = _STANDARD_TEST_CASE_SPECS.get(test_case)
    if std_spec and std_spec.get("extra_cloud_to_cloud_flags"):
        interop_only_options += list(std_spec["extra_cloud_to_cloud_flags"])

    common_options = [
        "--test_case=%s" % client_test_case,
        "--server_host=%s" % server_host,
        "--server_port=%s" % server_port,
    ]

    use_contract = (
        use_contract_entrypoints
        if use_contract_entrypoints is not None
        else bool(getattr(args, "contract_entrypoints", False))
    )
    if test_case in _HTTP2_SERVER_TEST_CASES:
        if test_case in _HTTP2_SERVER_TEST_CASES_THAT_USE_GRPC_CLIENTS:
            client_options = interop_only_options + common_options
            if use_contract and hasattr(language, "contract_client_cmd"):
                cmdline = bash_cmdline(
                    language.contract_client_cmd(client_options)
                )
                cwd = None
            else:
                cmdline = bash_cmdline(language.client_cmd(client_options))
                cwd = language.client_cwd
        else:
            if use_contract and hasattr(language, "contract_http2_client_cmd"):
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
        if use_contract and hasattr(language, "contract_client_cmd"):
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

    effective_allow_flakes = (
        allow_flakes
        if allow_flakes is not None
        else bool(getattr(args, "allow_flakes", False))
    )
    test_job = jobset.JobSpec(
        cmdline=cmdline,
        cwd=cwd,
        environ=environ,
        shortname="cloud_to_cloud:%s:%s_server:%s:%s"
        % (language, server_name, test_case, transport_security),
        timeout_seconds=_TEST_TIMEOUT,
        flake_retries=4 if effective_allow_flakes else 0,
        timeout_retries=2 if effective_allow_flakes else 0,
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
    use_contract_entrypoints=None,
):
    """Creates jobspec for running a server container."""
    container_name = dockerjob.random_name(
        "interop_server_%s" % language.safename
    )
    server_cmd = ["--port=%s" % _DEFAULT_SERVER_PORT]
    ts_cfg = _CONFIG["transport_security"].get(transport_security)
    if ts_cfg:
        server_cmd += list(ts_cfg["server_flags"])
    else:
        print(
            "Invalid transport security option %s in server_jobspec."
            % transport_security
        )
        sys.exit(1)

    use_contract = (
        use_contract_entrypoints
        if use_contract_entrypoints is not None
        else bool(getattr(args, "contract_entrypoints", False))
    )
    if use_contract and hasattr(language, "contract_server_cmd"):
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
        timeout_seconds=30 * 60,
    )
    server_job.container_name = container_name
    return server_job


def build_interop_image_jobspec(language, tag=None):
    """Creates jobspec for building interop docker image for a language."""
    if not tag:
        tag = "grpc_interop_%s:%s" % (language.safename, uuid.uuid4())
    env = {
        "INTEROP_IMAGE": tag,
        "BASE_NAME": "grpc_interop_%s" % language.safename,
    }
    build_job = jobset.JobSpec(
        cmdline=["tools/run_tests/dockerize/build_interop_image.sh"],
        environ=env,
        shortname="build_docker_%s" % (language),
        timeout_seconds=45 * 60,
    )
    build_job.tag = tag
    return build_job


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
    return {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "failed_cases": ", ".join(failed_cases),
        "percent": 1.0 * passed / (passed + failed),
    }


def _parse_override_server(kv):
    if "=" not in kv:
        raise argparse.ArgumentTypeError(
            "Invalid --override_server value %r: expected SERVER=HOST:PORT" % kv
        )
    server_name, host_port = kv.split("=", 1)
    if ":" not in host_port or not server_name:
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
        default=_CONFIG["defaults"].get(
            "service_account_key_file",
            "/root/service_account/grpc-testing-ebe7c1ac7381.json",
        ),
    )
    parser.add_argument(
        "--default_service_account",
        type=str,
        help=(
            "Default GCE service account email to use for some auth interop"
            " tests."
        ),
        default=_CONFIG["defaults"].get(
            "default_service_account",
            "830293263384-compute@developer.gserviceaccount.com",
        ),
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
args = argp.parse_args([])


def run_interop_tests_main(cli_args=None):
    global args
    atexit.register(lambda: subprocess.call(["stty", "echo"]))
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

    if not args.use_docker and servers:
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
            and srv_ep.supports(tc, args.transport_security)
            for lang in languages
            for tc in _TEST_CASES
        )
        has_h2 = bool(
            args.http2_interop
            and any(
                http2Interop.supports(tc, args.transport_security)
                and srv_ep.supports(tc, args.transport_security)
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
                            )
                            jobs.append(test_job)

        for server in args.override_server:
            if not isinstance(server, (list, tuple)) or len(server) != 2 or ":" not in server[1]:
                print("Invalid --override_server entry: %r" % (server,))
                sys.exit(1)
            server_name = server[0]
            (server_host, server_port) = server[1].split(":", 1)
            server_addresses[server_name] = (server_host, server_port)

        for server_name, server_address in list(server_addresses.items()):
            (server_host, server_port) = server_address
            server_endpoint = get_server_endpoint(server_name)
            for language in languages:
                for test_case in _TEST_CASES:
                    if language.supports(
                        test_case, args.transport_security
                    ) and server_endpoint.supports(
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
                        )
                        jobs.append(test_job)

            if args.http2_interop:
                for test_case in _HTTP2_TEST_CASES:
                    if http2Interop.supports(
                        test_case, args.transport_security
                    ) and server_endpoint.supports(
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
                        )
                        jobs.append(test_job)

        if args.http2_server_interop:
            if not args.manual_run:
                http2_server_job.wait_for_healthy(timeout_seconds=600)
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
# Backward-compatibility Release Matrix Support (client_matrix / create / run)
# ============================================================================


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
        return (
            "ReleaseInfo(patch=%r, runtimes=%r, testcases_file=%r)"
            % (self.patch, self.runtimes, self.testcases_file)
        )


class MatrixTestcaseProfile:
    """Renders historical matrix docker run commands from declarative profile."""

    def __init__(
        self,
        name,
        default_image,
        workdir,
        inner_command_template,
        env=None,
        bash_login=False,
        container_names=None,
    ):
        self.name = name
        self.default_image = default_image
        self.workdir = workdir
        self.inner_command_template = inner_command_template
        self.env = dict(env or {})
        self.bash_login = bool(bash_login)
        self.container_names = list(container_names or [])

    def render_commands(
        self, servers, test_cases, use_contract_entrypoints=False
    ):
        env_parts = ["-e %s=%s" % (k, v) for k, v in self.env.items()]
        env_str = (" " + " ".join(env_parts)) if env_parts else ""
        bash_cmd = "bash -l -c" if self.bash_login else "bash -c"
        lines = []
        idx = 0
        for srv in servers:
            for tc in test_cases:
                name_str = ""
                if self.container_names and idx < len(self.container_names):
                    name_str = " --name %s" % self.container_names[idx]
                if use_contract_entrypoints:
                    inner = (
                        "/interop/client --server_host=%s"
                        " --server_host_override=foo.test.google.fr"
                        " --server_port=443 --use_tls=true --test_case=%s"
                        % (srv, tc)
                    )
                else:
                    inner = self.inner_command_template.format(
                        server_host=srv, test_case=tc
                    )
                line = (
                    "docker run -i --rm=true%s -w %s --net=host%s"
                    ' $docker_image %s "%s"'
                    % (env_str, self.workdir, name_str, bash_cmd, inner)
                )
                lines.append(line)
                idx += 1
        return lines


_MATRIX_CFG = _CONFIG["matrix"]

LANG_RUNTIME_MATRIX = OrderedDict()
LANG_RELEASE_MATRIX = OrderedDict()
_MATRIX_REPOS = {}
_MATRIX_BUILD_FROM_RELEASE_TAG = {}
_MATRIX_ROOT_ENV_VAR = {}

for _mlang, _mdata in _MATRIX_CFG["languages"].items():
    LANG_RUNTIME_MATRIX[_mlang] = list(
        _mdata.get("default_runtimes", _mdata.get("runtimes", []))
    )
    _MATRIX_REPOS[_mlang] = _mdata["repo"]
    _MATRIX_BUILD_FROM_RELEASE_TAG[_mlang] = bool(
        _mdata.get(
            "build_from_release_tag",
            _mdata.get("build_docker_from_release_tag", True),
        )
    )
    _MATRIX_ROOT_ENV_VAR[_mlang] = _mdata.get("root_env_var", "GRPC_ROOT")
    _rel_od = OrderedDict()
    _releases_raw = _mdata["releases"]
    if isinstance(_releases_raw, dict):
        for _rtag, _rentry in _releases_raw.items():
            _rentry = _rentry or {}
            _rel_od[_rtag] = ReleaseInfo(
                patch=_rentry.get("patch"),
                runtimes=_rentry.get("runtimes"),
                testcases_file=_rentry.get("testcases_file"),
            )
    else:
        for _rentry in _releases_raw:
            _rtag = _rentry["tag"]
            _rel_od[_rtag] = ReleaseInfo(
                patch=_rentry.get("patch"),
                runtimes=_rentry.get("runtimes"),
                testcases_file=_rentry.get("testcases_file"),
            )
    LANG_RELEASE_MATRIX[_mlang] = _rel_od

_MATRIX_PROFILES = {}
for _pname, _pdata in _MATRIX_CFG.get("testcase_profiles", {}).items():
    _MATRIX_PROFILES[_pname] = MatrixTestcaseProfile(
        name=_pname,
        default_image=_pdata.get("default_image"),
        workdir=_pdata["workdir"],
        inner_command_template=_pdata["inner_command_template"],
        env=_pdata.get("env"),
        bash_login=_pdata.get("bash_login", False),
        container_names=_pdata.get("container_names"),
    )


def get_github_repo(lang):
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

    overrides = _MATRIX_CFG.get("runtime_testcases_prefix_overrides", {})
    if runtime in overrides:
        rule = overrides[runtime]
        if rule.get("lang") == lang:
            testcases_file = testcases_file.replace(
                rule["replace_from"], rule["replace_to"]
            )
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
    server_host="74.125.206.210",
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
    timeout_sec = int(_MATRIX_CFG.get("test_timeout_seconds", 60))
    flake_retries = (
        int(_CONFIG["defaults"].get("matrix_flake_retries", 5))
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


def pull_image_for_lang(lang, image, release):
    """Pull an image for a given language from the image registry."""
    pull_timeout = int(_MATRIX_CFG.get("pull_image_timeout_seconds", 15 * 60))
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


def cleanup_docker_image(image):
    jobset.message("START", "Cleanup docker image %s" % image, do_newline=True)
    dockerjob.remove_image(image, skip_nonexistent=True)


def add_files_to_image(image, with_files, label=None, build_info_dir="/var/local/build_info"):
    """Add files to a docker image via single-pass docker build."""
    temp_dir = tempfile.mkdtemp()
    try:
        copied_names = []
        for fpath in with_files:
            bname = os.path.basename(fpath)
            shutil.copy2(fpath, os.path.join(temp_dir, bname))
            copied_names.append(bname)
        dockerfile_lines = [
            "FROM %s" % image,
            "RUN mkdir -p %s" % build_info_dir,
        ]
        for bname in copied_names:
            dockerfile_lines.append("COPY %s %s/%s" % (bname, build_info_dir, bname))
        if label:
            dockerfile_lines.append("LABEL %s" % label)
        with open(os.path.join(temp_dir, "Dockerfile"), "w") as df:
            df.write("\n".join(dockerfile_lines) + "\n")
        subprocess.check_call(["docker", "build", "-t", image, temp_dir])
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def build_matrix_image_jobspec(
    runtime,
    env,
    gcr_tag,
    stack_base,
    lang,
    docker_path="us-docker.pkg.dev/grpc-testing/testing-images-public",
    image_builder_script="tools/run_tests/dockerize/build_interop_image.sh",
):
    """Build interop docker image for a language with runtime."""
    basename = "grpc_interop_%s" % runtime
    tag = "%s/%s:%s" % (docker_path, basename, gcr_tag)
    timeout_sec = int(_MATRIX_CFG.get("build_image_timeout_seconds", 30 * 60))
    build_env = {"INTEROP_IMAGE": tag, "BASE_NAME": basename}
    build_env.update(env)
    image_builder_path = image_builder_script
    if should_build_docker_interop_image_from_release_tag(lang):
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


def build_matrix_build_argparser():
    matrix_langs = list(LANG_RUNTIME_MATRIX.keys())
    all_releases = get_all_releases()
    parser = argparse.ArgumentParser(
        description="Run interop docker image build."
    )
    parser.add_argument(
        "--gcr_path",
        default=_MATRIX_CFG.get(
            "default_docker_path",
            "us-docker.pkg.dev/grpc-testing/testing-images-public",
        ),
        dest="docker_path",
        help="Path of registry where the images are stored",
    )
    parser.add_argument(
        "--release",
        default="master",
        choices=["all", "master"] + all_releases,
        help=(
            "github commit tag to checkout. When building all "
            "releases defined in client_matrix.py, specify "
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
        default=_MATRIX_CFG.get(
            "default_git_checkout_root", "/export/hda3/tmp/grpc_matrix"
        ),
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
        default=_MATRIX_CFG.get(
            "default_docker_path",
            "us-docker.pkg.dev/grpc-testing/testing-images-public",
        ),
        dest="docker_path",
        help="Path of docker images in Google Container Registry",
    )
    parser.add_argument(
        "--release",
        default="all",
        choices=["all", "master"] + all_releases,
        help="Release tags to test. When testing all "
        'releases defined in client_matrix.py, specify "all".',
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
        default="sponge_log.xml",
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
        default=_MATRIX_CFG.get("default_server_host", "74.125.206.210"),
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


if __name__ == "__main__":
    run_interop_tests_main()
