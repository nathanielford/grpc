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
"""Container-Network & Docker Compose v2 orchestrator for gRPC interop tests."""

from collections import OrderedDict
import json

from .bake_manifest import safename_for_language

DEFAULT_COMPOSE_NETWORK = "interop-net"
DEFAULT_SERVER_PORT = 8080


def server_service_name(server_lang_or_name):
    """Returns deterministic container network hostname for an interop server.

    Examples:
      'c++' / CXXLanguage() -> 'interop-server-cxx'
      'go' / GoLanguage() -> 'interop-server-go'
      'http2' / Http2Server() -> 'interop-server-http2'
    """
    safename = safename_for_language(server_lang_or_name)
    normalized = safename.replace(".", "-").replace("_", "-").replace("+", "x")
    return "interop-server-%s" % normalized


def server_healthcheck_cmd(server_lang_or_name, port=DEFAULT_SERVER_PORT):
    """Returns the native healthcheck command for a server container."""
    safename = safename_for_language(server_lang_or_name)
    if safename == "http2":
        return (
            "python test/http2_test/http2_server_health_check.py "
            "--server_host=localhost --server_port=%d" % int(port)
        )
    return "bash -c ': < /dev/tcp/127.0.0.1/%d'" % int(port)


def server_docker_healthcheck_args(
    server_lang_or_name, port=DEFAULT_SERVER_PORT
):
    """Returns `docker run` healthcheck CLI flags for a server container."""
    safename = safename_for_language(server_lang_or_name)
    retries = 5 if safename == "http2" else 30
    return [
        "--health-cmd=%s" % server_healthcheck_cmd(server_lang_or_name, port),
        "--health-interval=1s",
        "--health-retries=%d" % retries,
        "--health-timeout=10s",
    ]


def server_has_runnable_tests(
    server_name,
    server_language,
    client_languages,
    test_cases,
    http2_interop=False,
    http2_test_cases=None,
):
    """Returns True if at least one test case will actually execute against `server_name`.

    Prevents building or starting idle server containers (such as `pythonasyncio`,
    whose `unimplemented_test_cases_server()` skips all test cases).
    """
    skip_server = (
        set(server_language.unimplemented_test_cases_server())
        if server_language is not None
        else set()
    )
    for client_lang in client_languages:
        skip_client = set(client_lang.unimplemented_test_cases())
        for test_case in test_cases:
            if test_case not in skip_client and test_case not in skip_server:
                return True

    if http2_interop and server_name != "go":
        for test_case in http2_test_cases or []:
            if test_case not in skip_server:
                return True

    return False


def _extract_container_details_from_jobspec(
    spec, default_image=None, default_workdir="/var/local/git/grpc"
):
    """Extracts image, working_dir, environment, and command from a JobSpec."""
    cmdline = list(spec.cmdline) if isinstance(spec.cmdline, (list, tuple)) else [str(spec.cmdline)]
    env = OrderedDict(sorted((spec.environ or {}).items()))
    workdir = default_workdir
    image = default_image
    inner_cmd = cmdline

    if len(cmdline) >= 2 and cmdline[0] == "docker" and cmdline[1] == "run":
        idx = 2
        while idx < len(cmdline):
            token = cmdline[idx]
            if token in ("-i", "--rm=true", "--rm", "-d"):
                idx += 1
            elif token == "-e" and idx + 1 < len(cmdline):
                kv = cmdline[idx + 1]
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    env[k] = v
                idx += 2
            elif token == "-w" and idx + 1 < len(cmdline):
                workdir = cmdline[idx + 1]
                idx += 2
            elif token in ("-p", "--network", "--network-alias") and idx + 1 < len(cmdline):
                idx += 2
            elif (
                token.startswith("--name=")
                or token.startswith("--net=")
                or token.startswith("--network=")
                or token.startswith("--network-alias=")
                or token.startswith("--health-")
            ):
                idx += 1
            else:
                image = token
                inner_cmd = cmdline[idx + 1 :]
                break

    return image, workdir, env, inner_cmd


def generate_compose_dict(
    server_specs_by_lang,
    client_jobspecs,
    docker_images=None,
    network_name=DEFAULT_COMPOSE_NETWORK,
    http2_server_ports_count=8,
):
    """Builds a Docker Compose v2 specification dictionary for the interop topology."""
    docker_images = docker_images or {}
    services = OrderedDict()
    server_service_names = OrderedDict()

    for lang_name, spec in server_specs_by_lang.items():
        svc_name = server_service_name(lang_name)
        server_service_names[lang_name] = svc_name
        image, workdir, env, inner_cmd = _extract_container_details_from_jobspec(
            spec,
            default_image=docker_images.get(
                lang_name,
                "grpc_interop_%s:latest" % safename_for_language(lang_name),
            ),
        )
        safename = safename_for_language(lang_name)
        if safename == "http2":
            ports = [
                str(DEFAULT_SERVER_PORT + i)
                for i in range(http2_server_ports_count)
            ]
        else:
            ports = [str(DEFAULT_SERVER_PORT)]

        svc = OrderedDict()
        svc["image"] = image
        svc["working_dir"] = workdir
        if env:
            svc["environment"] = env
        svc["command"] = inner_cmd
        svc["expose"] = list(ports)
        svc["ports"] = list(ports)
        svc["networks"] = OrderedDict(
            [
                (
                    network_name,
                    OrderedDict([("aliases", [svc_name])]),
                )
            ]
        )
        svc["healthcheck"] = OrderedDict(
            [
                (
                    "test",
                    [
                        "CMD-SHELL",
                        server_healthcheck_cmd(lang_name, DEFAULT_SERVER_PORT),
                    ],
                ),
                ("interval", "1s"),
                ("timeout", "10s"),
                ("retries", 5 if safename == "http2" else 30),
            ]
        )
        services[svc_name] = svc

    for idx, job in enumerate(client_jobspecs):
        shortname = getattr(job, "shortname", "client_%d" % idx)
        parts = shortname.split(":")
        client_lang = parts[1] if len(parts) > 1 else "client"
        if client_lang == "objc":
            continue
        sanitized_name = (
            shortname.replace(":", "-")
            .replace("_", "-")
            .replace("+", "x")
            .replace(".", "-")
            .lower()
        )
        image, workdir, env, inner_cmd = _extract_container_details_from_jobspec(
            job,
            default_image=docker_images.get(
                client_lang,
                "grpc_interop_%s:latest" % safename_for_language(client_lang),
            ),
        )
        svc = OrderedDict()
        svc["image"] = image
        svc["working_dir"] = workdir
        if env:
            svc["environment"] = env
        svc["command"] = inner_cmd
        svc["networks"] = [network_name]

        if len(parts) >= 3 and parts[0] == "cloud_to_cloud":
            raw_server = parts[2]
            if raw_server.endswith("_server"):
                raw_server = raw_server[: -len("_server")]
            target_svc = server_service_names.get(raw_server)
            if target_svc:
                svc["depends_on"] = OrderedDict(
                    [
                        (
                            target_svc,
                            OrderedDict([("condition", "service_healthy")]),
                        )
                    ]
                )
        services[sanitized_name] = svc

    return OrderedDict(
        [
            ("name", "grpc-interop"),
            (
                "networks",
                OrderedDict(
                    [
                        (
                            network_name,
                            OrderedDict([("driver", "bridge")]),
                        )
                    ]
                ),
            ),
            ("services", services),
        ]
    )


def _yaml_scalar(val):
    """Formats a scalar value safely for YAML."""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val)
    if (
        not s
        or s.startswith("-")
        or any(
            c in s
            for c in (
                ":",
                "#",
                "{",
                "}",
                "[",
                "]",
                ",",
                "&",
                "*",
                "?",
                "|",
                "<",
                ">",
                "=",
                "!",
                "%",
                "@",
                "`",
                '"',
                "'",
                " ",
                "\t",
                "\n",
            )
        )
        or s.lower() in ("true", "false", "null", "yes", "no", "on", "off")
        or s.isdigit()
    ):
        return json.dumps(s)
    return s


def _render_yaml_lines(obj, indent=0):
    """Renders nested dicts/lists into clean YAML without external dependencies."""
    prefix = " " * indent
    lines = []
    if isinstance(obj, dict):
        if not obj:
            lines.append("%s{}" % prefix)
            return lines
        for k, v in obj.items():
            key_str = _yaml_scalar(k) if not isinstance(k, str) or ":" in k else k
            if isinstance(v, dict):
                if not v:
                    lines.append("%s%s: {}" % (prefix, key_str))
                else:
                    lines.append("%s%s:" % (prefix, key_str))
                    lines.extend(_render_yaml_lines(v, indent + 2))
            elif isinstance(v, (list, tuple)):
                if not v:
                    lines.append("%s%s: []" % (prefix, key_str))
                else:
                    lines.append("%s%s:" % (prefix, key_str))
                    lines.extend(_render_yaml_lines(v, indent + 2))
            else:
                lines.append("%s%s: %s" % (prefix, key_str, _yaml_scalar(v)))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            if isinstance(item, (dict, list, tuple)):
                lines.append("%s-" % prefix)
                lines.extend(_render_yaml_lines(item, indent + 2))
            else:
                lines.append("%s- %s" % (prefix, _yaml_scalar(item)))
    else:
        lines.append("%s%s" % (prefix, _yaml_scalar(obj)))
    return lines


def generate_compose_yaml(
    server_specs_by_lang,
    client_jobspecs,
    docker_images=None,
    network_name=DEFAULT_COMPOSE_NETWORK,
    http2_server_ports_count=8,
):
    """Generates a Docker Compose v2 YAML string for the interop test topology."""
    compose_dict = generate_compose_dict(
        server_specs_by_lang=server_specs_by_lang,
        client_jobspecs=client_jobspecs,
        docker_images=docker_images,
        network_name=network_name,
        http2_server_ports_count=http2_server_ports_count,
    )
    header = [
        "# Generated by tools/interop_bake/compose_manifest.py",
        "# Run servers with: docker compose -f <file> up -d",
    ]
    return "\n".join(header + _render_yaml_lines(compose_dict)) + "\n"
