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
"""Unified CLI for gRPC interop testing, matrix image builds, and introspection."""

import argparse
import multiprocessing
import sys
from typing import List, Optional, Sequence

from .builder import DockerImageBuilder
from .matrix import (
    MatrixRunner,
    get_all_releases,
    get_release_tags,
    get_runtimes_for_lang_release,
    get_test_images_for_lang,
    generate_test_case_jobspecs,
)
from .runner import InteropRunner
from .spec import InteropSpec, get_spec


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
    """Adds developer-friendly introspection and dry-run flags to an ArgumentParser."""
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
    """Builds the CLI ArgumentParser 100% compatible with run_interop_tests.py."""
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
    """Builds the CLI ArgumentParser 100% compatible with run_interop_matrix_tests.py."""
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
    """Builds the CLI ArgumentParser 100% compatible with create_matrix_images.py."""
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


def print_languages(spec: InteropSpec) -> None:
    """Prints a formatted table of all declaratively defined languages and targets."""
    header = (
        f"{'LANGUAGE':<15} {'SAFENAME':<14} {'REPO':<18} "
        f"{'SERVER':<7} {'HTTP2_CLI':<10} {'DOCKER':<7} {'AUTH_MODE':<10} {'UNIMPL(C/S)'}"
    )
    print(header)
    print("-" * len(header))
    for name, lspec in spec.all_targets.items():
        has_h2 = lspec.http2_client_cmd_spec is not None
        unimpl_c = len(lspec.unimplemented_test_cases())
        unimpl_s = len(lspec.unimplemented_test_cases_server())
        print(
            f"{name:<15} {lspec.safename:<14} {lspec.repo:<18} "
            f"{str(lspec.supports_server):<7} {str(has_h2):<10} "
            f"{str(lspec.docker_supported):<7} {lspec.auth_credentials_mode:<10} "
            f"{unimpl_c}/{unimpl_s}"
        )


def print_tests(spec: InteropSpec) -> None:
    """Prints all test suites and test cases from the declarative specification."""
    print("=== Test Suites ===")
    for sname, sdata in spec.suites.items():
        print(f"  {sname:<20}: {sdata.get('description', '')}")

    print("\n=== Standard Test Cases (%d) ===" % len(spec.standard_test_cases_spec))
    for tc in spec.standard_test_cases_spec:
        c2p = "cloud_to_prod+c2c" if tc.cloud_to_prod_eligible else "cloud_to_cloud_only"
        extra = (
            f" flags={tc.extra_cloud_to_cloud_flags}"
            if tc.extra_cloud_to_cloud_flags
            else ""
        )
        print(f"  {tc.name:<32} [{c2p}]{extra}")

    print("\n=== Auth Test Cases (%d) ===" % len(spec.auth_test_cases_spec))
    for ac in spec.auth_test_cases_spec:
        gce = (
            "all"
            if ac.requires_compute_engine_creds
            else (
                ",".join(ac.requires_compute_engine_creds_languages) or "none"
            )
        )
        print(
            f"  {ac.name:<36} [security={ac.transport_security}, "
            f"key_file={ac.uses_service_account_key}, oauth_scope={ac.uses_oauth_scope}, "
            f"gce_creds={gce}]"
        )

    print(
        "\n=== HTTP/2 Client Test Cases (%d) ==="
        % len(spec.http2_client_test_cases)
    )
    for hc in spec.http2_client_test_cases:
        print(f"  {hc}")

    print(
        "\n=== HTTP/2 Server Test Cases (%d) ==="
        % len(spec.http2_server_test_cases_spec)
    )
    sorted_h2 = sorted(spec.http2_server_test_cases)
    for sc in spec.http2_server_test_cases_spec:
        offset = sorted_h2.index(sc.name)
        port = int(spec.defaults["default_server_port"]) + offset
        override = (
            f" -> grpc_case={sc.grpc_client_test_case} ({sc.transport_security_override})"
            if sc.grpc_client_test_case
            else ""
        )
        print(f"  {sc.name:<28} [port={port}, kind={sc.client_kind}]{override}")


def print_explain_interop(args, spec: InteropSpec) -> None:
    """Prints an explanation of included and skipped tests for the given CLI flags."""
    runner = InteropRunner(spec)
    explain_target = getattr(args, "explain_matrix", None)
    if (
        isinstance(explain_target, str)
        and explain_target != "all"
        and explain_target in spec.languages
        and args.language == ["all"]
    ):
        args.language = [explain_target]
    # If no suite was explicitly enabled, explain cloud_to_cloud across selected languages/servers
    has_suite = bool(
        args.cloud_to_prod
        or args.cloud_to_prod_auth
        or args.server
        or args.override_server
        or args.http2_interop
        or args.http2_server_interop
    )
    if not has_suite:
        langs, servers, _ = runner.resolve_languages_and_servers(args)
        srv_list = servers if servers else list(spec.servers)
        records = spec.explain_cloud_to_cloud_matrix(
            [str(l) for l in langs],
            srv_list,
            transport_security=args.transport_security,
        )
        included = [r for r in records if r["status"] == "INCLUDED"]
        skipped = [r for r in records if r["status"] == "SKIPPED"]
        print(
            f"=== Cloud-to-Cloud Matrix Explanation (transport_security={args.transport_security}) ==="
        )
        print(f"Included: {len(included)} | Skipped: {len(skipped)}")
        for r in records:
            print(
                f"  [{r['status']:<8}] {r['client']} -> {r['server']} : {r['test_case']} ({r['reason']})"
            )
        return

    plan = runner.build_plan(
        args, direct_build=getattr(args, "direct_docker_build", False)
    )
    print(
        f"=== Execution Matrix Explanation (Included={len(plan.client_jobs)}, Skipped={len(plan.skipped_tests)}) ==="
    )
    if plan.build_jobs:
        print(f"\nDocker Images to Build ({len(plan.build_jobs)} deduplicated):")
        for bjob in plan.build_jobs:
            print(f"  - {bjob.shortname}: {bjob.tag}")
    print(f"\nIncluded Test Jobs ({len(plan.client_jobs)}):")
    for job in plan.client_jobs:
        print(f"  [INCLUDED] {job.shortname}")
    if plan.skipped_tests:
        print(f"\nSkipped Test Cases ({len(plan.skipped_tests)}):")
        for s in plan.skipped_tests:
            print(
                f"  [SKIPPED ] {s['suite']}:{s['client']}:{s['server']}:{s['test_case']} — {s['reason']}"
            )


def print_dry_run_interop(args, spec: InteropSpec) -> None:
    """Prints the dry-run execution plan for run_interop_tests.py."""
    runner = InteropRunner(spec)
    plan = runner.build_plan(
        args, direct_build=getattr(args, "direct_docker_build", False)
    )
    print("=== Dry-Run Execution Plan ===")
    print(f"Selected Client Languages : {[str(l) for l in plan.languages]}")
    print(f"Selected Server Languages : {plan.servers}")
    print(f"Docker Build Jobs         : {len(plan.build_jobs)}")
    for bjob in plan.build_jobs:
        print(f"  [BUILD] {bjob.shortname} -> {bjob.tag} : {bjob.cmdline}")
    print(f"Server Containers         : {len(plan.server_specs)}")
    for sname, sjob in plan.server_specs.items():
        print(f"  [SERVER] {sname} ({sjob.shortname}) : {sjob.cmdline}")
    print(f"Client Test Jobs          : {len(plan.client_jobs)}")
    for cjob in plan.client_jobs:
        print(f"  [TEST] {cjob.shortname} : {cjob.cmdline}")


def handle_common_introspection(args, spec: InteropSpec) -> bool:
    """Handles --list-languages, --list-tests, and --show-dockerfile if set. Returns True if handled."""
    if getattr(args, "show_dockerfile", None):
        builder = DockerImageBuilder(spec)
        print(builder.generate_dockerfile(args.show_dockerfile), end="")
        return True
    if getattr(args, "list_languages", False):
        print_languages(spec)
        return True
    if getattr(args, "list_tests", False):
        print_tests(spec)
        return True
    return False


def run_interop_cli(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint for run_interop_tests.py and `tools/interop/cli.py`."""
    spec = get_spec()
    argp = build_interop_argparser(spec)
    args = argp.parse_args(argv)

    if handle_common_introspection(args, spec):
        return 0
    if args.explain_matrix:
        print_explain_interop(args, spec)
        return 0
    if args.dry_run:
        print_dry_run_interop(args, spec)
        return 0

    runner = InteropRunner(spec)
    return runner.execute(args, direct_build=args.direct_docker_build)


def run_matrix_test_cli(argv: Optional[Sequence[str]] = None) -> int:
    """Entrypoint for run_interop_matrix_tests.py."""
    spec = get_spec()
    argp = build_matrix_test_argparser(spec)
    args = argp.parse_args(argv)

    if handle_common_introspection(args, spec):
        return 0

    all_langs = list(spec.lang_runtime_matrix.keys())
    languages = args.language if args.language != ["all"] else all_langs

    if args.explain_matrix or args.dry_run:
        total_suites = 0
        total_jobs = 0
        for lang in languages:
            images = get_test_images_for_lang(
                lang, args.release, args.docker_path, spec=spec
            )
            for runtime, tag_tuples in sorted(images.items()):
                for release, image in tag_tuples:
                    suite_name = f"{lang}__{runtime}_{release}"
                    specs = generate_test_case_jobspecs(
                        lang,
                        runtime,
                        release,
                        suite_name,
                        server_host=args.server_host,
                        allow_flakes=args.allow_flakes,
                        spec=spec,
                    )
                    total_suites += 1
                    total_jobs += len(specs)
                    print(
                        f"Suite {suite_name} (image={image}, cases={len(specs)})"
                    )
                    if args.dry_run:
                        for s in specs:
                            print(f"  [MATRIX_TEST] {s.shortname} : {s.cmdline}")
        print(f"Total matrix suites: {total_suites}, total test jobs: {total_jobs}")
        return 0

    runner = MatrixRunner(spec)
    return runner.execute(args)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Top-level unified CLI dispatcher supporting subcommands and direct flags."""
    args_list = list(sys.argv[1:] if argv is None else argv)
    if args_list and args_list[0] == "matrix-test":
        return run_matrix_test_cli(args_list[1:])
    if args_list and args_list[0] == "run":
        return run_interop_cli(args_list[1:])
    return run_interop_cli(args_list)


if __name__ == "__main__":
    sys.exit(main())
