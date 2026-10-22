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

import os
import sys
import uuid

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import client_matrix  # noqa: E402
from tools.interop import runner as _runner  # noqa: E402
from tools.interop.runner import (  # noqa: E402,F401
    cleanup_docker_image as _cleanup_docker_image,
    get_test_images_for_lang as _get_test_images_for_lang,
    pull_image_for_lang as _pull_image_for_lang,
    read_test_cases_file as _read_test_cases_file,
    upload_test_results,
)

python_util_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../run_tests/python_utils")
)
if python_util_dir not in sys.path:
    sys.path.append(python_util_dir)
import jobset  # noqa: E402
import report_utils  # noqa: E402

_LANGUAGES = list(client_matrix.LANG_RUNTIME_MATRIX.keys())
_RELEASES = client_matrix.get_all_releases()
_TEST_TIMEOUT_SECONDS = 60
_PULL_IMAGE_TIMEOUT_SECONDS = 15 * 60
_MAX_PARALLEL_DOWNLOADS = 6

argp = _runner.build_matrix_test_argparser()
args = argp.parse_args([])


def _generate_test_case_jobspecs(
    lang,
    runtime,
    release,
    suite_name,
    server_host=None,
    allow_flakes=None,
):
    host = (
        server_host
        if server_host is not None
        else (args.server_host if args is not None else "74.125.206.210")
    )
    flakes = (
        allow_flakes
        if allow_flakes is not None
        else (args.allow_flakes if args is not None else False)
    )
    use_contract = bool(getattr(args, "contract_entrypoints", False))
    return _runner.generate_test_case_jobspecs(
        lang=lang,
        runtime=runtime,
        release=release,
        suite_name=suite_name,
        server_host=host,
        allow_flakes=flakes,
        use_contract_entrypoints=use_contract,
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


def main(argv=None):
    global args
    args = argp.parse_args(argv)
    languages = (
        _LANGUAGES
        if "all" in args.language
        else list(dict.fromkeys(args.language))
    )
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
