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
"""Capability-driven interop test model and self-describing OCI image metadata."""

import argparse
from enum import Enum
import json
import os
import subprocess
import sys

DOCKER_WORKDIR_ROOT = "/var/local/git/grpc"


class Capability(Enum):
    """Explicit feature capabilities for gRPC interop clients, servers, and tests."""

    CORE_INTEROP = "core_interop"
    TIMEOUT_ON_SLEEPING_SERVER = "timeout_on_sleeping_server"
    ADVANCED_INTEROP = "advanced_interop"
    CLIENT_COMPRESSION = "client_compression"
    SERVER_COMPRESSION_UNARY = "server_compression_unary"
    SERVER_COMPRESSION_STREAMING = "server_compression_streaming"
    SPECIAL_STATUS_MESSAGE = "special_status_message"
    ORCA = "orca"
    DATA_FRAME_PADDING = "data_frame_padding"
    HTTP2_INTEROP = "http2_interop"
    HTTP2_BADSERVER_CLIENT = "http2_badserver_client"
    HTTP2_SERVER_INTEROP = "http2_server_interop"
    TLS = "tls"
    INSECURE = "insecure"
    ALTS = "alts"
    AUTH_CREDS = "auth_creds"
    GOOGLE_DEFAULT_CREDS = "google_default_creds"
    GCE_CHANNEL_CREDS = "gce_channel_creds"
    AUTH_KEY_FILE_FLAG = "auth_key_file_flag"
    AUTH_KEY_FILE_ENV = "auth_key_file_env"
    GCE_OAUTH2 = "gce_oauth2"
    SINGLE_INVOCATION_SUITE = "single_invocation_suite"


CORE_INTEROP_TEST_CASES = [
    "large_unary",
    "empty_unary",
    "ping_pong",
    "empty_stream",
    "client_streaming",
    "server_streaming",
    "cancel_after_begin",
    "cancel_after_first_response",
    "timeout_on_sleeping_server",
]

ALL_INTEROP_TEST_CASES = [
    "large_unary",
    "empty_unary",
    "ping_pong",
    "empty_stream",
    "client_streaming",
    "server_streaming",
    "cancel_after_begin",
    "cancel_after_first_response",
    "timeout_on_sleeping_server",
    "custom_metadata",
    "status_code_and_message",
    "unimplemented_method",
    "client_compressed_unary",
    "server_compressed_unary",
    "client_compressed_streaming",
    "server_compressed_streaming",
    "unimplemented_service",
    "special_status_message",
    "orca_per_rpc",
    "orca_oob",
]

AUTH_INTEROP_TEST_CASES = [
    "compute_engine_creds",
    "jwt_token_creds",
    "oauth2_auth_token",
    "per_rpc_creds",
    "google_default_credentials",
    "compute_engine_channel_credentials",
]

HTTP2_CLIENT_TEST_CASES = ["tls", "framing"]

HTTP2_SERVER_TEST_CASES = [
    "rst_after_header",
    "rst_after_data",
    "rst_during_data",
    "goaway",
    "ping",
    "max_streams",
    "data_frame_padding",
    "no_df_padding_sanity_test",
]

# Required capabilities for a client to execute each test case.
TEST_CASE_REQUIRED_CAPABILITIES = {
    "large_unary": frozenset([Capability.CORE_INTEROP]),
    "empty_unary": frozenset([Capability.CORE_INTEROP]),
    "ping_pong": frozenset([Capability.CORE_INTEROP]),
    "empty_stream": frozenset([Capability.CORE_INTEROP]),
    "client_streaming": frozenset([Capability.CORE_INTEROP]),
    "server_streaming": frozenset([Capability.CORE_INTEROP]),
    "cancel_after_begin": frozenset([Capability.CORE_INTEROP]),
    "cancel_after_first_response": frozenset([Capability.CORE_INTEROP]),
    "timeout_on_sleeping_server": frozenset(
        [Capability.CORE_INTEROP, Capability.TIMEOUT_ON_SLEEPING_SERVER]
    ),
    "custom_metadata": frozenset(
        [Capability.CORE_INTEROP, Capability.ADVANCED_INTEROP]
    ),
    "status_code_and_message": frozenset(
        [Capability.CORE_INTEROP, Capability.ADVANCED_INTEROP]
    ),
    "unimplemented_method": frozenset(
        [Capability.CORE_INTEROP, Capability.ADVANCED_INTEROP]
    ),
    "unimplemented_service": frozenset(
        [Capability.CORE_INTEROP, Capability.ADVANCED_INTEROP]
    ),
    "client_compressed_unary": frozenset(
        [Capability.CORE_INTEROP, Capability.CLIENT_COMPRESSION]
    ),
    "client_compressed_streaming": frozenset(
        [Capability.CORE_INTEROP, Capability.CLIENT_COMPRESSION]
    ),
    "server_compressed_unary": frozenset(
        [Capability.CORE_INTEROP, Capability.SERVER_COMPRESSION_UNARY]
    ),
    "server_compressed_streaming": frozenset(
        [Capability.CORE_INTEROP, Capability.SERVER_COMPRESSION_STREAMING]
    ),
    "special_status_message": frozenset(
        [Capability.CORE_INTEROP, Capability.SPECIAL_STATUS_MESSAGE]
    ),
    "orca_per_rpc": frozenset([Capability.CORE_INTEROP, Capability.ORCA]),
    "orca_oob": frozenset([Capability.CORE_INTEROP, Capability.ORCA]),
    "compute_engine_creds": frozenset([Capability.AUTH_CREDS]),
    "jwt_token_creds": frozenset([Capability.AUTH_CREDS]),
    "oauth2_auth_token": frozenset([Capability.AUTH_CREDS]),
    "per_rpc_creds": frozenset([Capability.AUTH_CREDS]),
    "google_default_credentials": frozenset([Capability.GOOGLE_DEFAULT_CREDS]),
    "compute_engine_channel_credentials": frozenset(
        [Capability.GCE_CHANNEL_CREDS]
    ),
    "tls": frozenset([Capability.HTTP2_INTEROP]),
    "framing": frozenset([Capability.HTTP2_INTEROP]),
    "rst_after_header": frozenset([Capability.HTTP2_BADSERVER_CLIENT]),
    "rst_after_data": frozenset([Capability.HTTP2_BADSERVER_CLIENT]),
    "rst_during_data": frozenset([Capability.HTTP2_BADSERVER_CLIENT]),
    "goaway": frozenset([Capability.HTTP2_BADSERVER_CLIENT]),
    "ping": frozenset([Capability.HTTP2_BADSERVER_CLIENT]),
    "max_streams": frozenset([Capability.HTTP2_BADSERVER_CLIENT]),
    "data_frame_padding": frozenset([Capability.DATA_FRAME_PADDING]),
    "no_df_padding_sanity_test": frozenset([Capability.CORE_INTEROP]),
}

# Required capabilities for a server to serve each test case.
SERVER_TEST_CASE_REQUIRED_CAPABILITIES = {
    **{
        tc: TEST_CASE_REQUIRED_CAPABILITIES[tc]
        for tc in ALL_INTEROP_TEST_CASES
    },
    **{
        tc: TEST_CASE_REQUIRED_CAPABILITIES[tc]
        for tc in AUTH_INTEROP_TEST_CASES
    },
    "tls": frozenset([Capability.HTTP2_INTEROP]),
    "framing": frozenset([Capability.HTTP2_INTEROP]),
    "rst_after_header": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "rst_after_data": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "rst_during_data": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "goaway": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "ping": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "max_streams": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "data_frame_padding": frozenset([Capability.HTTP2_SERVER_INTEROP]),
    "no_df_padding_sanity_test": frozenset([Capability.HTTP2_SERVER_INTEROP]),
}

TRANSPORT_SECURITY_CAPABILITIES = {
    "tls": Capability.TLS,
    "insecure": Capability.INSECURE,
    "alts": Capability.ALTS,
    "google_default_credentials": Capability.GOOGLE_DEFAULT_CREDS,
    "compute_engine_channel_creds": Capability.GCE_CHANNEL_CREDS,
}

# Ordered list of test cases used to compute backward-compatible unimplemented_test_cases()
_LEGACY_CLIENT_UNIMPLEMENTED_ORDER = [
    *ALL_INTEROP_TEST_CASES[1:],
    "large_unary",
    "client_compressed_unary",
    "client_compressed_streaming",
    "server_compressed_unary",
    "server_compressed_streaming",
    "data_frame_padding",
    "special_status_message",
    "compute_engine_creds",
    "jwt_token_creds",
    "oauth2_auth_token",
    "per_rpc_creds",
    "google_default_credentials",
    "compute_engine_channel_credentials",
    "timeout_on_sleeping_server",
    "orca_per_rpc",
    "orca_oob",
]

# OCI Image Label keys
OCI_LABEL_CLIENT_CMD = "io.grpc.interop.client_cmd"
OCI_LABEL_SERVER_CMD = "io.grpc.interop.server_cmd"
OCI_LABEL_HTTP2_CLIENT_CMD = "io.grpc.interop.http2_client_cmd"
OCI_LABEL_WORKDIR = "io.grpc.interop.workdir"
OCI_LABEL_ENV = "io.grpc.interop.env"
OCI_LABEL_CAPABILITIES = "io.grpc.interop.capabilities"
OCI_LABEL_RELEASE = "io.grpc.interop.release"

OCI_LABEL_KEYS = (
    OCI_LABEL_CLIENT_CMD,
    OCI_LABEL_SERVER_CMD,
    OCI_LABEL_HTTP2_CLIENT_CMD,
    OCI_LABEL_WORKDIR,
    OCI_LABEL_ENV,
    OCI_LABEL_CAPABILITIES,
    OCI_LABEL_RELEASE,
)


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
        and test_case in ALL_INTEROP_TEST_CASES[1:]
    ):
        return False
    if skip_compute_engine_creds:
        if test_case == "compute_engine_creds":
            return False
        if (
            test_case == "oauth2_auth_token"
            and Capability.GCE_OAUTH2 in capabilities
        ):
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
    """Computes backward-compatible unimplemented_test_cases() from client capabilities."""
    unimplemented = []
    seen = set()
    # Preserve canonical ordering while avoiding duplicates unless expected by legacy callers
    candidates = (
        ALL_INTEROP_TEST_CASES
        + ["data_frame_padding"]
        + AUTH_INTEROP_TEST_CASES
    )
    for tc in candidates:
        if tc in seen:
            continue
        req = TEST_CASE_REQUIRED_CAPABILITIES[tc]
        is_single_suite_skip = (
            Capability.SINGLE_INVOCATION_SUITE in capabilities
            and tc in ALL_INTEROP_TEST_CASES[1:]
        )
        if is_single_suite_skip or not req.issubset(capabilities):
            unimplemented.append(tc)
            seen.add(tc)
    return unimplemented


def compute_unimplemented_server_cases(capabilities):
    """Computes backward-compatible unimplemented_test_cases_server() from server capabilities."""
    if not capabilities or Capability.CORE_INTEROP not in capabilities:
        if (
            Capability.HTTP2_SERVER_INTEROP in capabilities
            or Capability.HTTP2_INTEROP in capabilities
        ):
            return list(ALL_INTEROP_TEST_CASES)
        return list(
            ALL_INTEROP_TEST_CASES
            + AUTH_INTEROP_TEST_CASES
            + HTTP2_CLIENT_TEST_CASES
            + HTTP2_SERVER_TEST_CASES
        )
    unimplemented = []
    for tc in ALL_INTEROP_TEST_CASES:
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
        return f"ServerEndpointView({self._language!r})"


class ProdServerEndpoint:
    """Capability model for production interop target servers (default, gateway_v4)."""

    PROD_CAPABILITIES = frozenset(
        [
            Capability.CORE_INTEROP,
            Capability.TIMEOUT_ON_SLEEPING_SERVER,
            Capability.HTTP2_INTEROP,
            Capability.AUTH_CREDS,
            Capability.GOOGLE_DEFAULT_CREDS,
            Capability.GCE_CHANNEL_CREDS,
            Capability.TLS,
        ]
    )

    def __init__(self, nickname="default", host="grpc-test.sandbox.googleapis.com"):
        self.nickname = nickname
        self.host = host
        self.server_capabilities = self.PROD_CAPABILITIES
        self.client_capabilities = frozenset()

    def has_capability(self, cap, role="server"):
        return cap in self.server_capabilities

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
        return cap in self.server_capabilities

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


class CapabilityLanguageMixin:
    """Mixin providing capability queries, backward-compatible skip lists, and OCI metadata."""

    client_capabilities = frozenset()
    server_capabilities = frozenset()
    default_role = "client"

    def has_capability(self, capability, role=None):
        effective_role = role or self.default_role
        caps = (
            self.server_capabilities
            if effective_role == "server"
            else self.client_capabilities
        )
        return capability in caps

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

    def unimplemented_test_cases(self):
        return compute_unimplemented_client_cases(self.client_capabilities)

    def unimplemented_test_cases_server(self):
        return compute_unimplemented_server_cases(self.server_capabilities)

    def to_oci_metadata(self, release="master"):
        """Builds self-describing OciImageMetadata for this language definition."""
        client_cmd = []
        if hasattr(self, "client_cmd") and getattr(self, "safename", "") != "objc":
            raw_client = self.client_cmd(["{args}"])
            if raw_client:
                if raw_client[-1] == "{args}":
                    client_cmd = list(raw_client[:-1])
                else:
                    client_cmd = list(raw_client)

        server_cmd = []
        if hasattr(self, "server_cmd"):
            raw_server = self.server_cmd(["{args}"])
            if raw_server:
                if raw_server[-1] == "{args}":
                    server_cmd = list(raw_server[:-1])
                else:
                    server_cmd = list(raw_server)

        http2_cmd = []
        if hasattr(self, "client_cmd_http2interop"):
            raw_h2 = self.client_cmd_http2interop(["{args}"])
            if raw_h2:
                if raw_h2[-1] == "{args}":
                    http2_cmd = list(raw_h2[:-1])
                else:
                    http2_cmd = list(raw_h2)

        def _abs_workdir(rel_cwd):
            if not rel_cwd:
                return DOCKER_WORKDIR_ROOT
            return os.path.join(DOCKER_WORKDIR_ROOT, rel_cwd)

        c_wd = _abs_workdir(getattr(self, "client_cwd", None))
        s_wd = _abs_workdir(getattr(self, "server_cwd", None))
        h_wd = _abs_workdir(getattr(self, "http2_cwd", None))
        if c_wd == s_wd == h_wd:
            workdir = c_wd
        else:
            workdir = {"client": c_wd, "server": s_wd, "http2": h_wd}

        env = dict(self.global_env()) if hasattr(self, "global_env") else {}
        return OciImageMetadata(
            client_cmd=client_cmd,
            server_cmd=server_cmd,
            http2_client_cmd=http2_cmd,
            workdir=workdir,
            env=env,
            capabilities=self.client_capabilities,
            server_capabilities=self.server_capabilities,
            release=release,
        )


def _format_cmd_template(cmd_template, args):
    """Formats a command template (list of tokens or shell string) with `args`."""
    if not cmd_template:
        return []
    args = list(args)
    if isinstance(cmd_template, str):
        if "{args}" in cmd_template:
            return [cmd_template.replace("{args}", " ".join(args))]
        return [cmd_template] + args
    tokens = list(cmd_template)
    replaced = False
    out = []
    for token in tokens:
        if "{args}" in token:
            out.append(token.replace("{args}", " ".join(args)))
            replaced = True
        else:
            out.append(token)
    if not replaced:
        out.extend(args)
    return out


class OciImageMetadata:
    """Structured OCI image metadata for self-describing gRPC interop images."""

    def __init__(
        self,
        client_cmd,
        server_cmd=None,
        http2_client_cmd=None,
        workdir=DOCKER_WORKDIR_ROOT,
        env=None,
        capabilities=None,
        server_capabilities=None,
        release="master",
        login_shell=False,
    ):
        if isinstance(client_cmd, str):
            self.client_cmd = [client_cmd] if client_cmd else []
        else:
            self.client_cmd = list(client_cmd or [])
        if isinstance(server_cmd, str):
            self.server_cmd = [server_cmd] if server_cmd else []
        else:
            self.server_cmd = list(server_cmd or [])
        if isinstance(http2_client_cmd, str):
            self.http2_client_cmd = (
                [http2_client_cmd] if http2_client_cmd else []
            )
        else:
            self.http2_client_cmd = list(http2_client_cmd or [])
        self.workdir = workdir or DOCKER_WORKDIR_ROOT
        self.env = dict(env or {})
        self.capabilities = frozenset(capabilities or [])
        self.server_capabilities = frozenset(server_capabilities or [])
        self.release = release or "master"
        self.login_shell = bool(login_shell)

    @property
    def client_workdir(self):
        if isinstance(self.workdir, dict):
            return self.workdir.get("client", DOCKER_WORKDIR_ROOT)
        return self.workdir

    @property
    def server_workdir(self):
        if isinstance(self.workdir, dict):
            return self.workdir.get("server", DOCKER_WORKDIR_ROOT)
        return self.workdir

    @property
    def http2_workdir(self):
        if isinstance(self.workdir, dict):
            return self.workdir.get("http2", DOCKER_WORKDIR_ROOT)
        return self.workdir

    def format_client_cmd(self, args):
        return _format_cmd_template(self.client_cmd, args)

    def format_server_cmd(self, args):
        return _format_cmd_template(self.server_cmd, args)

    def format_http2_client_cmd(self, args):
        return _format_cmd_template(self.http2_client_cmd, args)

    def supports(
        self,
        test_case,
        transport_security="tls",
        role="client",
        skip_compute_engine_creds=False,
    ):
        if role == "server":
            return check_server_supports(
                self.server_capabilities, test_case, transport_security
            )
        return check_client_supports(
            self.capabilities,
            test_case,
            transport_security=transport_security,
            skip_compute_engine_creds=skip_compute_engine_creds,
        )

    def to_oci_labels(self):
        """Serializes metadata into the 7 structured OCI image labels."""
        workdir_val = (
            self.workdir
            if isinstance(self.workdir, str)
            else json.dumps(self.workdir, sort_keys=True)
        )
        if self.server_capabilities:
            cap_val = json.dumps(
                {
                    "client": sorted(c.value for c in self.capabilities),
                    "server": sorted(c.value for c in self.server_capabilities),
                },
                sort_keys=True,
            )
        else:
            cap_val = ",".join(sorted(c.value for c in self.capabilities))
        return {
            OCI_LABEL_CLIENT_CMD: json.dumps(self.client_cmd),
            OCI_LABEL_SERVER_CMD: json.dumps(self.server_cmd),
            OCI_LABEL_HTTP2_CLIENT_CMD: json.dumps(self.http2_client_cmd),
            OCI_LABEL_WORKDIR: workdir_val,
            OCI_LABEL_ENV: json.dumps(self.env),
            OCI_LABEL_CAPABILITIES: cap_val,
            OCI_LABEL_RELEASE: self.release,
        }

    @classmethod
    def from_oci_labels(cls, labels):
        """Parses structured OCI labels from an inspected Docker image."""
        if not labels or not isinstance(labels, dict):
            return None
        if OCI_LABEL_CLIENT_CMD not in labels:
            return None

        def _parse_cmd(raw):
            if not raw:
                return []
            raw = raw.strip()
            if raw.startswith("["):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list):
                        return [str(x) for x in parsed]
                except ValueError:
                    pass
            return [raw]

        def _parse_workdir(raw):
            if not raw:
                return DOCKER_WORKDIR_ROOT
            raw = raw.strip()
            if raw.startswith("{"):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        return parsed
                except ValueError:
                    pass
            return raw

        def _parse_env(raw):
            if not raw:
                return {}
            raw = raw.strip()
            if raw.startswith("{"):
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        return {str(k): str(v) for k, v in parsed.items()}
                except ValueError:
                    pass
            env = {}
            for item in raw.split(","):
                if "=" in item:
                    k, v = item.split("=", 1)
                    env[k.strip()] = v.strip()
            return env

        def _parse_cap_list(items):
            out = set()
            val_map = {c.value: c for c in Capability}
            name_map = {c.name.lower(): c for c in Capability}
            for item in items:
                key = str(item).strip().lower()
                if not key:
                    continue
                if key in val_map:
                    out.add(val_map[key])
                elif key in name_map:
                    out.add(name_map[key])
            return frozenset(out)

        raw_caps = labels.get(OCI_LABEL_CAPABILITIES, "").strip()
        client_caps = frozenset()
        server_caps = frozenset()
        if raw_caps.startswith("{"):
            try:
                parsed_caps = json.loads(raw_caps)
                if isinstance(parsed_caps, dict):
                    client_caps = _parse_cap_list(
                        parsed_caps.get("client", [])
                    )
                    server_caps = _parse_cap_list(
                        parsed_caps.get("server", [])
                    )
            except ValueError:
                pass
        elif raw_caps.startswith("["):
            try:
                parsed_caps = json.loads(raw_caps)
                if isinstance(parsed_caps, list):
                    client_caps = _parse_cap_list(parsed_caps)
            except ValueError:
                pass
        elif raw_caps:
            client_caps = _parse_cap_list(raw_caps.split(","))

        release = (
            labels.get(OCI_LABEL_RELEASE)
            or labels.get("release")
            or "master"
        )
        return cls(
            client_cmd=_parse_cmd(labels.get(OCI_LABEL_CLIENT_CMD, "")),
            server_cmd=_parse_cmd(labels.get(OCI_LABEL_SERVER_CMD, "")),
            http2_client_cmd=_parse_cmd(
                labels.get(OCI_LABEL_HTTP2_CLIENT_CMD, "")
            ),
            workdir=_parse_workdir(labels.get(OCI_LABEL_WORKDIR, "")),
            env=_parse_env(labels.get(OCI_LABEL_ENV, "")),
            capabilities=client_caps,
            server_capabilities=server_caps,
            release=release,
        )

    def to_dockerfile_labels(self):
        """Returns Dockerfile LABEL directives for this metadata."""
        labels = self.to_oci_labels()
        lines = []
        for k, v in labels.items():
            lines.append(f"LABEL {k}={json.dumps(v)}")
        if self.release and self.release != "master":
            lines.append(f"LABEL release={json.dumps(self.release)}")
        return lines

    def generate_dockerfile(
        self,
        base_image,
        base_name,
        propagate_env=None,
        include_commit_log=False,
    ):
        """Generates a single-pass Dockerfile baking the build step and OCI labels."""
        lines = [f"FROM {base_image}"]
        lines.extend(self.to_dockerfile_labels())
        for env_key in propagate_env or []:
            lines.append(f"ARG {env_key}")
        if include_commit_log:
            lines.append("COPY commit_log /var/local/build_info/commit_log")
        lines.append("COPY jenkins /var/local/jenkins")
        lines.append(
            f'RUN bash -l "/var/local/jenkins/grpc/tools/dockerfile/interoptest/{base_name}/build_interop.sh" '
            "&& rm -rf /var/local/jenkins"
        )
        return "\n".join(lines) + "\n"

    def build_matrix_client_cmdline(
        self,
        server_host,
        server_dns,
        test_case,
        docker_image_var="$docker_image",
    ):
        """Builds the docker run shell command for a matrix cloud-to-prod client test."""
        client_args = [f"--server_host={server_host}"]
        if server_host != server_dns:
            client_args.append(f"--server_host_override={server_dns}")
        client_args.extend(
            [
                "--server_port=443",
                f"--test_case={test_case}",
                "--use_tls=true",
            ]
        )
        inner_tokens = self.format_client_cmd(client_args)
        inner_cmd = " ".join(inner_tokens).replace('"', '\\"')

        docker_parts = ["docker", "run", "-i", "--rm=true"]
        for k, v in self.env.items():
            docker_parts.extend(["-e", f"{k}={v}"])
        docker_parts.extend(
            ["-w", self.client_workdir, "--net=host", docker_image_var]
        )
        if self.login_shell:
            docker_parts.extend(["bash", "-l", "-c", f'"{inner_cmd}"'])
        else:
            docker_parts.extend(["bash", "-c", f'"{inner_cmd}"'])
        return " ".join(docker_parts)


# Default capability sets for languages
BASE_RPC_CAPABILITIES = frozenset(
    [
        Capability.CORE_INTEROP,
        Capability.TIMEOUT_ON_SLEEPING_SERVER,
        Capability.ADVANCED_INTEROP,
        Capability.TLS,
        Capability.INSECURE,
    ]
)

ALL_COMPRESSION_CAPABILITIES = frozenset(
    [
        Capability.CLIENT_COMPRESSION,
        Capability.SERVER_COMPRESSION_UNARY,
        Capability.SERVER_COMPRESSION_STREAMING,
    ]
)

DEFAULT_MATRIX_CLIENT_CAPABILITIES = frozenset(
    [
        Capability.CORE_INTEROP,
        Capability.TIMEOUT_ON_SLEEPING_SERVER,
        Capability.TLS,
    ]
)


def _parse_release_version(release):
    if not release or release == "master":
        return (999, 999, 999)
    v = release.lstrip("v")
    parts = []
    for p in v.split("."):
        if p == "x" or not p.isdigit():
            parts.append(0)
        else:
            parts.append(int(p))
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def get_matrix_image_metadata(lang, runtime, release="master"):
    """In-memory fallback catalog for historical and current matrix images."""
    v = _parse_release_version(release)
    env = {}
    server_cmd = []
    http2_cmd = []
    login_shell = False
    caps = DEFAULT_MATRIX_CLIENT_CAPABILITIES

    if lang in ("cxx", "c++"):
        if v <= (1, 31, 1):
            client_cmd = ["bins/opt/interop_client"]
            server_cmd = ["bins/opt/interop_server"]
            http2_cmd = ["bins/opt/http2_client"]
        else:
            client_cmd = ["cmake/build/interop_client"]
            server_cmd = ["cmake/build/interop_server"]
            http2_cmd = ["cmake/build/http2_client"]
        workdir = "/var/local/git/grpc"
    elif lang == "go":
        client_cmd = ["go", "run", "client.go"]
        server_cmd = ["go", "run", "server.go"]
        http2_cmd = ["go", "run", "negative_http2_client.go"]
        workdir = "/go/src/google.golang.org/grpc/interop/client"
        if v >= (1, 34, 0):
            env = {"GO111MODULE": "on"}
    elif lang in ("java", "javaokhttp"):
        if lang == "javaokhttp":
            client_cmd = ["./run-test-client.sh", "--use_okhttp=true"]
        else:
            client_cmd = ["./run-test-client.sh"]
        server_cmd = ["./run-test-server.sh"]
        http2_cmd = [
            "./interop-testing/build/install/grpc-interop-testing/bin/http2-client"
        ]
        workdir = "/var/local/git/grpc/../grpc-java"
    elif lang in ("python", "pythonasyncio"):
        workdir = "/var/local/git/grpc"
        env = {
            "PYTHONPATH": "/var/local/git/grpc/src/python/gens",
            "LD_LIBRARY_PATH": "/var/local/git/grpc/libs/opt",
        }
        if v <= (1, 10, 1):
            py_bin = "py27/bin/python"
        elif v <= (1, 17, 1):
            py_bin = "py27_native/bin/python"
        elif v <= (1, 39, 0):
            py_bin = "py37_native/bin/python"
        elif v <= (1, 43, 2):
            py_bin = "py39_native/bin/python"
        else:
            py_bin = "py39/bin/python"
        if runtime == "pythonasyncio" or lang == "pythonasyncio":
            client_cmd = [
                py_bin,
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--use-asyncio",
                "--client",
                '--args="{args}"',
            ]
            server_cmd = [
                py_bin,
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--use-asyncio",
                "--server",
                '--args="{args}"',
            ]
        else:
            client_cmd = [
                py_bin,
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--client",
                '--args="{args}"',
            ]
            server_cmd = [
                py_bin,
                "src/python/grpcio_tests/setup.py",
                "run_interop",
                "--server",
                '--args="{args}"',
            ]
        http2_cmd = [
            py_bin,
            "src/python/grpcio_tests/tests/http2/negative_http2_client.py",
        ]
    elif lang in ("node", "nodepurejs"):
        if v <= (1, 0, 1):
            workdir = "/var/local/git/grpc"
            client_cmd = ["node", "src/node/interop/interop_client.js"]
            server_cmd = ["node", "src/node/interop/interop_server.js"]
            login_shell = True
        elif v <= (1, 6, 6):
            workdir = "/var/local/git/grpc"
            client_cmd = [
                "tools/run_tests/interop/with_nvm.sh",
                "node",
                "src/node/interop/interop_client.js",
            ]
            server_cmd = [
                "tools/run_tests/interop/with_nvm.sh",
                "node",
                "src/node/interop/interop_server.js",
            ]
        else:
            workdir = "/var/local/git/grpc-node"
            fixture = (
                "./test/fixtures/js_js"
                if (runtime == "nodepurejs" or lang == "nodepurejs")
                else "./test/fixtures/native_native"
            )
            client_cmd = [
                "packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh",
                "node",
                "--require",
                fixture,
                "test/interop/interop_client.js",
            ]
            server_cmd = [
                "packages/grpc-native-core/deps/grpc/tools/run_tests/interop/with_nvm.sh",
                "node",
                "--require",
                fixture,
                "test/interop/interop_server.js",
            ]
    elif lang == "ruby":
        workdir = "/var/local/git/grpc"
        if v <= (1, 0, 1):
            client_cmd = [
                "source",
                "/usr/local/rvm/scripts/rvm",
                "&&",
                "ruby",
                "src/ruby/pb/test/client.rb",
            ]
            server_cmd = [
                "source",
                "/usr/local/rvm/scripts/rvm",
                "&&",
                "ruby",
                "src/ruby/pb/test/server.rb",
            ]
        else:
            client_cmd = [
                "tools/run_tests/interop/with_rvm.sh",
                "ruby",
                "src/ruby/pb/test/client.rb",
            ]
            server_cmd = [
                "tools/run_tests/interop/with_rvm.sh",
                "ruby",
                "src/ruby/pb/test/server.rb",
            ]
    elif lang in ("php", "php7"):
        workdir = "/var/local/git/grpc"
        client_cmd = ["src/php/bin/interop_client.sh"]
        server_cmd = ["src/php/bin/interop_server.sh"]
    elif lang == "csharp":
        if runtime == "csharpcoreclr":
            client_cmd = ["dotnet", "exec", "Grpc.IntegrationTesting.Client.dll"]
            server_cmd = ["dotnet", "exec", "Grpc.IntegrationTesting.Server.dll"]
            if v <= (1, 17, 1):
                fw = "netcoreapp1.0"
            elif v <= (1, 19, 0):
                fw = "netcoreapp1.1"
            elif v <= (1, 42, 0):
                fw = "netcoreapp2.1"
            else:
                fw = "netcoreapp3.1"
            workdir = f"/var/local/git/grpc/src/csharp/Grpc.IntegrationTesting.Client/bin/Debug/{fw}"
        else:
            client_cmd = ["mono", "Grpc.IntegrationTesting.Client.exe"]
            server_cmd = ["mono", "Grpc.IntegrationTesting.Server.exe"]
            if v <= (1, 2, 5):
                workdir = "/var/local/git/grpc/src/csharp/Grpc.IntegrationTesting.Client/bin/Debug"
            else:
                workdir = "/var/local/git/grpc/src/csharp/Grpc.IntegrationTesting.Client/bin/Debug/net45"
    elif lang == "aspnetcore":
        client_cmd = ["dotnet", "exec", "InteropTestsClient.dll"]
        server_cmd = ["dotnet", "exec", "InteropTestsWebsite.dll"]
        workdir = {
            "client": "/var/local/git/grpc/../grpc-dotnet/output/InteropTestsClient",
            "server": "/var/local/git/grpc/../grpc-dotnet/output/InteropTestsWebsite",
            "http2": "/var/local/git/grpc",
        }
    elif lang == "dart":
        client_cmd = ["dart", "bin/client.dart"]
        server_cmd = ["dart", "bin/server.dart"]
        workdir = "/var/local/git/grpc/../grpc-dart/interop"
    elif lang == "http2":
        client_cmd = ["tools/http2_interop/http2_interop.test", "-test.v"]
        server_cmd = ["python test/http2_test/http2_test_server.py"]
        workdir = "/var/local/git/grpc"
        caps = frozenset(
            [
                Capability.HTTP2_INTEROP,
                Capability.HTTP2_SERVER_INTEROP,
                Capability.TLS,
                Capability.INSECURE,
            ]
        )
    else:
        raise ValueError(f"Unsupported matrix language/runtime: {lang}/{runtime}")

    return OciImageMetadata(
        client_cmd=client_cmd,
        server_cmd=server_cmd,
        http2_client_cmd=http2_cmd,
        workdir=workdir,
        env=env,
        capabilities=caps,
        release=release,
        login_shell=login_shell,
    )


def get_base_name_image_metadata(base_name, release="master"):
    """Resolves OciImageMetadata from a Docker base image name (e.g. grpc_interop_cxx)."""
    runtime = base_name
    if runtime.startswith("grpc_interop_"):
        runtime = runtime[len("grpc_interop_") :]
    if runtime.startswith("go"):
        lang = "go"
    elif runtime.startswith("java"):
        lang = "java"
    elif runtime in ("csharp", "csharpcoreclr"):
        lang = "csharp"
    elif runtime == "php7":
        lang = "php"
    elif runtime in ("python", "pythonasyncio"):
        lang = "python"
    else:
        lang = runtime
    return get_matrix_image_metadata(lang, runtime, release=release)


def inspect_image_oci_labels(image):
    """Inspects OCI labels on a local Docker image, returning None if unavailable."""
    if not image:
        return None
    try:
        raw = subprocess.check_output(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{json .Config.Labels}}",
                image,
            ],
            stderr=subprocess.DEVNULL,
        )
        labels = json.loads(raw.decode("utf-8").strip())
        if isinstance(labels, dict):
            return labels
    except Exception:
        pass
    return None


def resolve_image_metadata(
    lang, runtime, release="master", image=None, oci_labels=None
):
    """Resolves OciImageMetadata from explicit OCI labels, image inspect, or fallback catalog."""
    labels = oci_labels
    if labels is None and image:
        labels = inspect_image_oci_labels(image)
    if labels:
        parsed = OciImageMetadata.from_oci_labels(labels)
        if parsed is not None:
            return parsed
    return get_matrix_image_metadata(lang, runtime, release=release)


def _cli_main():
    parser = argparse.ArgumentParser(
        description="Emit OCI labels or Dockerfile for gRPC interop images."
    )
    parser.add_argument("--base-name", required=True)
    parser.add_argument("--base-image", default="")
    parser.add_argument("--release", default="master")
    parser.add_argument(
        "--emit-dockerfile",
        action="store_true",
        help="Emit complete single-pass Dockerfile.",
    )
    parser.add_argument(
        "--emit-labels-only",
        action="store_true",
        help="Emit Dockerfile LABEL lines only.",
    )
    parser.add_argument(
        "--include-commit-log",
        action="store_true",
        help="Include COPY commit_log directive.",
    )
    parser.add_argument(
        "--propagate-env-file",
        default="",
        help="Optional env file of build-arg names to propagate.",
    )
    cli_args = parser.parse_args()

    # Allow environment variable overrides for any of the 7 OCI labels
    env_labels = {}
    env_map = {
        "INTEROP_LABEL_CLIENT_CMD": OCI_LABEL_CLIENT_CMD,
        "INTEROP_LABEL_SERVER_CMD": OCI_LABEL_SERVER_CMD,
        "INTEROP_LABEL_HTTP2_CLIENT_CMD": OCI_LABEL_HTTP2_CLIENT_CMD,
        "INTEROP_LABEL_WORKDIR": OCI_LABEL_WORKDIR,
        "INTEROP_LABEL_ENV": OCI_LABEL_ENV,
        "INTEROP_LABEL_CAPABILITIES": OCI_LABEL_CAPABILITIES,
        "INTEROP_LABEL_RELEASE": OCI_LABEL_RELEASE,
    }
    for env_k, oci_k in env_map.items():
        if os.environ.get(env_k):
            env_labels[oci_k] = os.environ[env_k]

    metadata = get_base_name_image_metadata(
        cli_args.base_name, release=cli_args.release
    )
    if env_labels:
        merged_labels = metadata.to_oci_labels()
        merged_labels.update(env_labels)
        metadata = OciImageMetadata.from_oci_labels(merged_labels)

    if cli_args.emit_labels_only:
        print("\n".join(metadata.to_dockerfile_labels()))
        return

    if cli_args.emit_dockerfile:
        prop_env = []
        if cli_args.propagate_env_file and os.path.exists(
            cli_args.propagate_env_file
        ):
            with open(cli_args.propagate_env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and os.environ.get(line):
                        prop_env.append(line)
        sys.stdout.write(
            metadata.generate_dockerfile(
                base_image=cli_args.base_image,
                base_name=cli_args.base_name,
                propagate_env=prop_env,
                include_commit_log=cli_args.include_commit_log,
            )
        )
        return


if __name__ == "__main__":
    _cli_main()
