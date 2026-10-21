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

import argparse
import atexit
import multiprocessing
import os
import shutil
import subprocess
import sys

# Language Runtime Matrix
import client_matrix

python_util_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../run_tests/python_utils")
)
sys.path.append(python_util_dir)
import dockerjob
from interop_capabilities import (
    OCI_LABEL_CAPABILITIES,
    OCI_LABEL_CLIENT_CMD,
    OCI_LABEL_ENV,
    OCI_LABEL_HTTP2_CLIENT_CMD,
    OCI_LABEL_KEYS,
    OCI_LABEL_RELEASE,
    OCI_LABEL_SERVER_CMD,
    OCI_LABEL_WORKDIR,
    Capability,
    OciImageMetadata,
    get_base_name_image_metadata,
    get_matrix_image_metadata,
)
import jobset

_IMAGE_BUILDER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../run_tests/dockerize/build_interop_image.sh",
    )
)
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

# Destination directory inside docker image to keep extra info from build time.
_BUILD_INFO = "/var/local/build_info"

argp = argparse.ArgumentParser(description="Run interop tests.")
argp.add_argument(
    "--docker_path",
    default="us-docker.pkg.dev/grpc-testing/testing-images-public",
    help="Path of docker images",
)

argp.add_argument(
    "--release",
    default="master",
    choices=["all", "master"] + _RELEASES,
    help=(
        "github commit tag to checkout.  When building all "
        'releases defined in client_matrix.py, use "all". Valid only '
        "with --git_checkout."
    ),
)

argp.add_argument(
    "-l",
    "--language",
    choices=["all"] + sorted(_LANGUAGES),
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
    default="/export/hda3/tmp/grpc_matrix",
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

args = argp.parse_args([])
docker_images_cleanup = []


def build_image_jobspec(runtime, env, gcr_tag, stack_base, lang=None):
    """Build interop docker image for a language with runtime in a single pass.

    runtime: a <lang><version> string, for example go1.8.
    env:     dictionary of env to passed to the build script.
    gcr_tag: the tag for the docker image (i.e. v1.3.0).
    stack_base: the local gRPC repo path.
    lang:    optional matrix language key (e.g. 'go', 'csharp').
    """
    basename = "grpc_interop_%s" % runtime
    tag = "%s/%s:%s" % (args.docker_path, basename, gcr_tag)
    if lang:
        oci_metadata = get_matrix_image_metadata(
            lang, runtime, release=gcr_tag
        )
    else:
        oci_metadata = get_base_name_image_metadata(basename, release=gcr_tag)
    oci_labels = oci_metadata.to_oci_labels()

    build_env = {
        "INTEROP_IMAGE": tag,
        "BASE_NAME": basename,
        "INTEROP_RELEASE": gcr_tag,
        "INTEROP_LABEL_CLIENT_CMD": oci_labels[OCI_LABEL_CLIENT_CMD],
        "INTEROP_LABEL_SERVER_CMD": oci_labels[OCI_LABEL_SERVER_CMD],
        "INTEROP_LABEL_HTTP2_CLIENT_CMD": oci_labels[
            OCI_LABEL_HTTP2_CLIENT_CMD
        ],
        "INTEROP_LABEL_WORKDIR": oci_labels[OCI_LABEL_WORKDIR],
        "INTEROP_LABEL_ENV": oci_labels[OCI_LABEL_ENV],
        "INTEROP_LABEL_CAPABILITIES": oci_labels[OCI_LABEL_CAPABILITIES],
        "INTEROP_LABEL_RELEASE": oci_labels[OCI_LABEL_RELEASE],
    }
    if gcr_tag != "master" and stack_base:
        commit_log = os.path.join(stack_base, "commit_log")
        build_env["COMMIT_LOG_FILE"] = commit_log
    build_env.update(env)

    build_job = jobset.JobSpec(
        cmdline=[_IMAGE_BUILDER],
        environ=build_env,
        shortname="build_docker_%s" % runtime,
        timeout_seconds=30 * 60,
    )
    build_job.tag = tag
    build_job.oci_labels = oci_labels
    return build_job


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
            # Build a particular release.
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


def build_all_images_for_release(lang, release):
    """Build all docker images for a release across all runtimes."""
    docker_images = []
    build_jobs = []

    env = {}
    # If we're not using current tree or the sibling for grpc stack, do checkout.
    stack_base = ""
    if args.git_checkout:
        stack_base = checkout_grpc_stack(lang, release)
        var = {
            "go": "GRPC_GO_ROOT",
            "java": "GRPC_JAVA_ROOT",
            "node": "GRPC_NODE_ROOT",
        }.get(lang, "GRPC_ROOT")
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
    return docker_images


def cleanup():
    if not args.keep:
        for image in docker_images_cleanup:
            dockerjob.remove_image(image, skip_nonexistent=True)


def maybe_apply_patches_on_git_tag(stack_base, lang, release):
    files_to_patch = []

    release_info = client_matrix.LANG_RELEASE_MATRIX[lang].get(release)
    if release_info:
        files_to_patch = release_info.patch
    if not files_to_patch:
        return
    patch_file_relative_path = "patches/%s_%s/git_repo.patch" % (lang, release)
    patch_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), patch_file_relative_path)
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


def checkout_grpc_stack(lang, release):
    """Invokes 'git check' for the lang/release and returns directory created."""
    assert args.git_checkout and args.git_checkout_root

    if not os.path.exists(args.git_checkout_root):
        os.makedirs(args.git_checkout_root)

    repo = client_matrix.get_github_repo(lang)
    # Get the subdir name part of repo
    # For example, 'git@github.com:grpc/grpc-go.git' should use 'grpc-go'.
    repo_dir = os.path.splitext(os.path.basename(repo))[0]
    stack_base = os.path.join(args.git_checkout_root, repo_dir)

    # Clean up leftover repo dir if necessary.
    if not args.reuse_git_root and os.path.exists(stack_base):
        jobset.message("START", "Removing git checkout root.", do_newline=True)
        shutil.rmtree(stack_base)

    if not os.path.exists(stack_base):
        subprocess.check_call(
            ["git", "clone", "--recursive", repo],
            cwd=os.path.dirname(stack_base),
        )

    # git checkout.
    jobset.message(
        "START",
        "git checkout %s from %s" % (release, stack_base),
        do_newline=True,
    )
    # We should NEVER do checkout on current tree !!!
    assert not os.path.dirname(__file__).startswith(stack_base)
    output = subprocess.check_output(
        ["git", "checkout", release], cwd=stack_base, stderr=subprocess.STDOUT
    )
    maybe_apply_patches_on_git_tag(stack_base, lang, release)
    commit_log = subprocess.check_output(["git", "log", "-1"], cwd=stack_base)
    jobset.message(
        "SUCCESS",
        "git checkout",
        "%s: %s" % (str(output), commit_log),
        do_newline=True,
    )

    # git submodule update
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

    # Write git log to commit_log so it can be packaged with the docker image.
    with open(os.path.join(stack_base, "commit_log"), "wb") as f:
        f.write(commit_log)
    return stack_base


def main(cli_args=None):
    global args
    atexit.register(cleanup)
    args = argp.parse_args(cli_args)
    languages = args.language if args.language != ["all"] else _LANGUAGES
    for lang in languages:
        docker_images = build_all_images_for_lang(lang)
        for image in docker_images:
            if args.upload_images:
                jobset.message("START", "Uploading %s" % image, do_newline=True)
                # docker image name must be in the format <docker_path>/<image>:<gcr_tag>
                assert (
                    image.startswith(args.docker_path) and image.find(":") != -1
                )
                # Add a tag to exclude the image from the GCP Vulnerability Scanner.
                (image_name, tag_name) = image.rsplit(":", 1)
                alternate_image = (
                    f"{image_name}:infrastructure-public-image-{tag_name}"
                )
                subprocess.call(
                    ["docker", "image", "tag", image, alternate_image]
                )
                subprocess.call(["gcloud", "docker", "--", "push", image])
                subprocess.call(
                    ["gcloud", "docker", "--", "push", alternate_image]
                )
            else:
                # Uploading (and overwriting images) by default can easily break things.
                print(
                    "Not uploading image %s, run with --upload_images to upload."
                    % image
                )


if __name__ == "__main__":
    main()
