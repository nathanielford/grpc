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

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import client_matrix  # noqa: E402
from tools.interop.interop import (  # noqa: E402
    MatrixRunner,
    build_matrix_test_argparser,
    cleanup_docker_image as _cleanup_docker_image,
    generate_test_case_jobspecs,
    get_all_releases,
    get_spec,
    get_test_images_for_lang as _get_test_images_for_lang,
    pull_image_for_lang as _pull_image_for_lang,
    read_test_cases_file as _read_test_cases_file,
    run_matrix_tests_main,
)

_SPEC = get_spec()
_TEST_TIMEOUT_SECONDS = int(
    _SPEC.matrix_config.get("test_timeout_seconds", 60)
)
_PULL_IMAGE_TIMEOUT_SECONDS = int(
    _SPEC.matrix_config.get("pull_image_timeout_seconds", 15 * 60)
)
_MAX_PARALLEL_DOWNLOADS = int(
    _SPEC.matrix_config.get("max_parallel_downloads", 6)
)
_LANGUAGES = list(client_matrix.LANG_RUNTIME_MATRIX.keys())
_RELEASES = get_all_releases(_SPEC)

argp = build_matrix_test_argparser(_SPEC)
args = None


def _generate_test_case_jobspecs(
    lang,
    runtime,
    release,
    suite_name,
    server_host="74.125.206.210",
    allow_flakes=False,
):
    host = args.server_host if args is not None else server_host
    flakes = args.allow_flakes if args is not None else allow_flakes
    return generate_test_case_jobspecs(
        lang=lang,
        runtime=runtime,
        release=release,
        suite_name=suite_name,
        server_host=host,
        allow_flakes=flakes,
        spec=_SPEC,
    )


def _test_release(lang, runtime, release, image, xml_report_tree, skip_tests):
    runner = MatrixRunner(_SPEC)
    return runner.test_release(
        lang=lang,
        runtime=runtime,
        release=release,
        image=image,
        xml_report_tree=xml_report_tree,
        skip_tests=skip_tests,
        server_host=args.server_host if args else "74.125.206.210",
        allow_flakes=args.allow_flakes if args else False,
        jobs=args.jobs if args else 1,
        bq_result_table=args.bq_result_table if args else "",
    )


def _run_tests_for_lang(lang, runtime, images, xml_report_tree):
    runner = MatrixRunner(_SPEC)
    return runner.run_tests_for_lang(
        lang=lang,
        runtime=runtime,
        images=images,
        xml_report_tree=xml_report_tree,
        args=args,
    )


if __name__ == "__main__":
    sys.exit(run_matrix_tests_main())
