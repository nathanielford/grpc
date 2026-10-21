#!/usr/bin/env python3
# Copyright 2017 gRPC authors.
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
"""Run tests using docker images in Google Container Registry per matrix."""

from __future__ import print_function

import argparse
import atexit
import json
import multiprocessing
import os
import re
import subprocess
import sys
import uuid

# Language Runtime Matrix
import client_matrix

python_util_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../run_tests/python_utils")
)
sys.path.append(python_util_dir)
import dockerjob
import jobset
import report_utils

try:
    import upload_test_results
except ImportError as e:
    upload_test_results = None

_TEST_TIMEOUT_SECONDS = 60
_PULL_IMAGE_TIMEOUT_SECONDS = 15 * 60
_MAX_PARALLEL_DOWNLOADS = 6
_LANGUAGES = list(client_matrix.LANG_RUNTIME_MATRIX.keys())
# All gRPC release tags, flattened, deduped and sorted.
_RELEASES = sorted(
    list(
        set(
            release
            for release_dict in list(client_matrix.LANG_RELEASE_MATRIX.values())
            for release in list(release_dict.keys())
        )
    )
)

argp = argparse.ArgumentParser(description="Run interop tests.")
argp.add_argument("-j", "--jobs", default=multiprocessing.cpu_count(), type=int)
argp.add_argument(
    "--docker_path",
    default="us-docker.pkg.dev/grpc-testing/testing-images-public",
    help="Path of docker images",
)
argp.add_argument(
    "--release",
    default="all",
    choices=["all"] + _RELEASES,
    help=(
        "Release tags to test.  When testing all "
        'releases defined in client_matrix.py, use "all".'
    ),
)
argp.add_argument(
    "-l",
    "--language",
    choices=["all"] + sorted(_LANGUAGES),
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
    "--report_file", default="report.xml", help="The result file to create."
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
# Requests will be routed through specified VIP by default.
# See go/grpc-interop-tests (internal-only) for details.
argp.add_argument(
    "--server_host",
    default="74.125.206.210",
    type=str,
    nargs="?",
    help="The gateway to backend services.",
)
argp.add_argument(
    "--contract_entrypoints",
    default=False,
    action="store_const",
    const=True,
    help=(
        "Invoke the standardized /interop/client container entrypoint "
        "instead of legacy release-specific commands."
    ),
)


def _get_test_images_for_lang(lang, release_arg, image_path_prefix):
    """Find docker images for a language across releases and runtimes.

    Returns dictionary of list of (<tag>, <image-full-path>) keyed by runtime.
    """
    if release_arg == "all":
        # Use all defined releases for given language
        releases = client_matrix.get_release_tags(lang)
    else:
        # Look for a particular release.
        if release_arg not in client_matrix.get_release_tags(lang):
            jobset.message(
                "SKIPPED",
                "release %s for %s is not defined" % (release_arg, lang),
                do_newline=True,
            )
            return {}
        releases = [release_arg]

    # Image tuples keyed by runtime.
    images = {}
    for tag in releases:
        for runtime in client_matrix.get_runtimes_for_lang_release(lang, tag):
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


def _cleanup_docker_image(image):
    jobset.message("START", "Cleanup docker image %s" % image, do_newline=True)
    dockerjob.remove_image(image, skip_nonexistent=True)


args = argp.parse_args() if __name__ == "__main__" else argp.parse_args([])


def _generate_test_case_jobspecs(
    lang,
    runtime,
    release,
    suite_name,
    server_host=None,
    allow_flakes=None,
    contract_entrypoints=None,
):
    """Programmatically generate JobSpec list for (lang, runtime, release)."""
    resolved_server_host = (
        server_host if server_host is not None else args.server_host
    )
    resolved_allow_flakes = (
        allow_flakes if allow_flakes is not None else args.allow_flakes
    )
    resolved_contract = (
        contract_entrypoints
        if contract_entrypoints is not None
        else args.contract_entrypoints
    )

    job_spec_list = []
    for server_short, server_fqdn in client_matrix.INTEROP_MATRIX_SERVERS:
        for testcase_name in client_matrix.INTEROP_MATRIX_TEST_CASES:
            line = client_matrix.build_matrix_docker_cmdline(
                lang=lang,
                runtime=runtime,
                release=release,
                server_host=resolved_server_host,
                server_host_override=server_fqdn,
                test_case=testcase_name,
                docker_image="$docker_image",
                contract_entrypoints=resolved_contract,
            )
            print("Creating jobspec with cmdline '{}'".format(line))
            spec = jobset.JobSpec(
                cmdline=line,
                shortname="%s:%s:%s:%s"
                % (suite_name, lang, server_short, testcase_name),
                timeout_seconds=_TEST_TIMEOUT_SECONDS,
                shell=True,
                flake_retries=5 if resolved_allow_flakes else 0,
            )
            job_spec_list.append(spec)
    return job_spec_list


def _pull_image_for_lang(lang, image, release):
    """Pull an image for a given language form the image registry."""
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


def _test_release(lang, runtime, release, image, xml_report_tree, skip_tests):
    total_num_failures = 0
    suite_name = "%s__%s_%s" % (lang, runtime, release)
    job_spec_list = _generate_test_case_jobspecs(
        lang, runtime, release, suite_name
    )

    if not job_spec_list:
        jobset.message("FAILED", "No test cases were found.", do_newline=True)
        total_num_failures += 1
    else:
        num_failures, resultset = jobset.run(
            job_spec_list,
            newline_on_success=True,
            add_env={"docker_image": image},
            maxjobs=args.jobs,
            skip_jobs=skip_tests,
        )
        if args.bq_result_table and resultset and upload_test_results:
            upload_test_results.upload_interop_results_to_bq(
                resultset, args.bq_result_table
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


def _run_tests_for_lang(lang, runtime, images, xml_report_tree):
    """Find and run all test cases for a language.

    images is a list of (<release-tag>, <image-full-path>) tuple.
    """
    skip_tests = False
    total_num_failures = 0

    max_pull_jobs = min(args.jobs, _MAX_PARALLEL_DOWNLOADS)
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

        # NOTE(rbellevi): We batch docker pull operations to maximize
        # parallelism, without letting the disk usage grow unbounded.
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
        if not args.keep:
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


def main():
    global args
    args = argp.parse_args()
    languages = args.language if args.language != ["all"] else _LANGUAGES
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
