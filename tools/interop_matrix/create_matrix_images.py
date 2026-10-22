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
"""Build and upload docker images to Google Container Registry per matrix."""

from __future__ import print_function

import atexit
import multiprocessing
import os
import subprocess
import sys

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import client_matrix  # noqa: E402
from tools.interop import runner as _runner  # noqa: E402

python_util_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../run_tests/python_utils")
)
if python_util_dir not in sys.path:
    sys.path.append(python_util_dir)
import dockerjob  # noqa: E402
import jobset  # noqa: E402

_IMAGE_BUILDER = "tools/run_tests/dockerize/build_interop_image.sh"
_LANGUAGES = list(client_matrix.LANG_RUNTIME_MATRIX.keys())
_RELEASES = client_matrix.get_all_releases()
_BUILD_INFO = "/var/local/build_info"

argp = _runner.build_matrix_build_argparser()
args = argp.parse_args([])
docker_images_cleanup = []


def add_files_to_image(image, with_files, label=None):
    return _runner.add_files_to_image(
        image, with_files, label=label, build_info_dir=_BUILD_INFO
    )


def _infer_lang_from_runtime(runtime, env):
    if env:
        for mlang, var in _runner._MATRIX_ROOT_ENV_VAR.items():
            if var != "GRPC_ROOT" and var in env:
                return mlang
    for mlang, default_runtimes in client_matrix.LANG_RUNTIME_MATRIX.items():
        if runtime in default_runtimes or runtime == mlang or runtime.startswith(mlang):
            return mlang
    for mlang, rel_dict in client_matrix.LANG_RELEASE_MATRIX.items():
        for rel_info in rel_dict.values():
            if runtime in (rel_info.runtimes or []):
                return mlang
    if (
        args
        and getattr(args, "language", None)
        and "all" not in args.language
    ):
        return args.language[0]
    return _LANGUAGES[0]


def build_image_jobspec(runtime, env, gcr_tag, stack_base, lang=None):
    active_lang = lang or _infer_lang_from_runtime(runtime, env)
    docker_path = (
        args.docker_path
        if args
        else "us-docker.pkg.dev/grpc-testing/testing-images-public"
    )
    return _runner.build_matrix_image_jobspec(
        runtime=runtime,
        env=env,
        gcr_tag=gcr_tag,
        stack_base=stack_base,
        lang=active_lang,
        docker_path=docker_path,
        image_builder_script=_IMAGE_BUILDER,
    )


def maybe_apply_patches_on_git_tag(stack_base, lang, release):
    return _runner.maybe_apply_patches_on_git_tag(
        stack_base,
        lang,
        release,
        patches_base_dir=os.path.dirname(os.path.abspath(__file__)),
    )


def checkout_grpc_stack(lang, release):
    return _runner.checkout_grpc_stack(
        lang=lang,
        release=release,
        git_checkout=args.git_checkout,
        git_checkout_root=args.git_checkout_root,
        reuse_git_root=args.reuse_git_root,
        patches_base_dir=os.path.dirname(os.path.abspath(__file__)),
    )


def build_all_images_for_release(lang, release):
    """Build all docker images for a release across all runtimes."""
    docker_images = []
    build_jobs = []

    env = {}
    stack_base = ""
    if args.git_checkout:
        stack_base = checkout_grpc_stack(lang, release)
        var = _runner._MATRIX_ROOT_ENV_VAR.get(lang, "GRPC_ROOT")
        env[var] = stack_base

    for runtime in client_matrix.get_runtimes_for_lang_release(lang, release):
        job = build_image_jobspec(runtime, env, release, stack_base, lang=lang)
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
                add_files_to_image(image, [commit_log], "release=%s" % release)
    return docker_images


def build_all_images_for_lang(lang):
    """Build all docker images for a language across releases and runtimes."""
    if not args.git_checkout:
        if args.release != "master":
            print(
                "Cannot use --release without also enabling --git_checkout.\n"
            )
            sys.exit(1)
        releases = [args.release]
    else:
        if args.release == "all":
            releases = client_matrix.get_release_tags(lang)
        else:
            if args.release not in ["master"] + client_matrix.get_release_tags(
                lang
            ):
                jobset.message(
                    "SKIPPED",
                    "%s for %s is not defined" % (args.release, lang),
                    do_newline=True,
                )
                return []
            releases = [args.release]

    images = []
    for release in releases:
        images += build_all_images_for_release(lang, release)
    jobset.message(
        "SUCCESS",
        "All docker images built for %s at %s." % (lang, releases),
        do_newline=True,
    )
    return images


def cleanup():
    if args and not args.keep:
        for image in docker_images_cleanup:
            dockerjob.remove_image(image, skip_nonexistent=True)


def main(argv=None):
    global args
    args = argp.parse_args(argv)
    atexit.register(cleanup)
    languages = (
        _LANGUAGES
        if "all" in args.language
        else list(dict.fromkeys(args.language))
    )
    for lang in languages:
        docker_images = build_all_images_for_lang(lang)
        for image in docker_images:
            if args.upload_images:
                jobset.message("START", "Uploading %s" % image, do_newline=True)
                assert (
                    image.startswith(args.docker_path) and image.find(":") != -1
                )
                (image_name, tag_name) = image.rsplit(":", 1)
                alternate_image = (
                    "%s:infrastructure-public-image-%s" % (image_name, tag_name)
                )
                subprocess.call(
                    ["docker", "image", "tag", image, alternate_image]
                )
                subprocess.call(["gcloud", "docker", "--", "push", image])
                subprocess.call(
                    ["gcloud", "docker", "--", "push", alternate_image]
                )
            else:
                print(
                    "Not uploading image %s, run with --upload_images to upload."
                    % image
                )
    return 0


if __name__ == "__main__":
    sys.exit(main())
