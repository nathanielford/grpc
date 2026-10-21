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
"""Declarative backward-compatibility release matrix builder and runner."""

from collections import OrderedDict
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

from .builder import DockerImageBuilder, add_files_to_image
from .spec import InteropSpec, ReleaseInfo, get_spec

_PYTHON_UTILS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../run_tests/python_utils")
)
if _PYTHON_UTILS_DIR not in sys.path:
    sys.path.append(_PYTHON_UTILS_DIR)

import dockerjob  # noqa: E402
import jobset  # noqa: E402
import report_utils  # noqa: E402

try:
    import upload_test_results
except ImportError:
    upload_test_results = None


_SPEC = get_spec()
LANG_RUNTIME_MATRIX: Dict[str, List[str]] = _SPEC.lang_runtime_matrix
LANG_RELEASE_MATRIX: Dict[str, "OrderedDict[str, ReleaseInfo]"] = (
    _SPEC.lang_release_matrix
)


def get_github_repo(lang: str, spec: Optional[InteropSpec] = None) -> str:
    active_spec = spec or get_spec()
    return active_spec.matrix_repos.get(lang, "https://github.com/grpc/grpc.git")


def get_release_tags(lang: str, spec: Optional[InteropSpec] = None) -> List[str]:
    """Returns list of known releases for given language."""
    active_spec = spec or get_spec()
    return list(active_spec.lang_release_matrix[lang].keys())


def get_runtimes_for_lang_release(
    lang: str, release: str, spec: Optional[InteropSpec] = None
) -> List[str]:
    """Get list of valid runtimes for given release of lang."""
    active_spec = spec or get_spec()
    runtimes = list(active_spec.lang_runtime_matrix[lang])
    release_info = active_spec.lang_release_matrix[lang].get(release)
    if release_info and release_info.runtimes:
        runtimes = list(release_info.runtimes)
    return runtimes


def should_build_docker_interop_image_from_release_tag(
    lang: str, spec: Optional[InteropSpec] = None
) -> bool:
    active_spec = spec or get_spec()
    return active_spec.matrix_build_from_release_tag.get(lang, True)


def get_all_releases(spec: Optional[InteropSpec] = None) -> List[str]:
    active_spec = spec or get_spec()
    return sorted(
        list(
            set(
                release
                for release_dict in active_spec.lang_release_matrix.values()
                for release in release_dict.keys()
            )
        )
    )


def resolve_testcases_filename(
    lang: str,
    runtime: str,
    release: str,
    spec: Optional[InteropSpec] = None,
) -> str:
    """Resolves the testcase profile/file name for a (lang, runtime, release) tuple."""
    active_spec = spec or get_spec()
    testcases_file = None
    release_info = active_spec.lang_release_matrix.get(lang, {}).get(release)
    if release_info:
        testcases_file = release_info.testcases_file
    if not testcases_file:
        testcases_file = "%s__master" % lang

    overrides = active_spec.matrix_config.get(
        "runtime_testcases_prefix_overrides", {}
    )
    if runtime in overrides:
        rule = overrides[runtime]
        if rule.get("lang") == lang:
            testcases_file = testcases_file.replace(
                rule["replace_from"], rule["replace_to"]
            )
    return testcases_file


def read_test_cases_file(
    lang: str,
    runtime: str,
    release: str,
    spec: Optional[InteropSpec] = None,
    testcases_dir: Optional[str] = None,
) -> List[str]:
    """Returns the list of matrix test commands for (lang, runtime, release) from InteropSpec."""
    active_spec = spec or get_spec()
    testcases_file = resolve_testcases_filename(
        lang, runtime, release, spec=active_spec
    )

    if testcases_file in active_spec.matrix_testcase_profiles:
        profile = active_spec.matrix_testcase_profiles[testcases_file]
        servers = active_spec.matrix_config.get(
            "servers",
            [
                "grpc-test.sandbox.googleapis.com",
                "grpc-test4.sandbox.googleapis.com",
            ],
        )
        test_cases = active_spec.matrix_config.get(
            "test_cases",
            [
                tc.name
                for tc in active_spec.standard_test_cases_spec
                if tc.cloud_to_prod_eligible
            ],
        )
        return profile.render_commands(servers, test_cases)

    base_dir = testcases_dir or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../interop_matrix/testcases")
    )
    testcases_filepath = os.path.join(base_dir, testcases_file)
    lines = []
    with open(testcases_filepath) as f:
        for line in f.readlines():
            line = re.sub(r"\#.*$", "", line)
            line = line.strip()
            if line and not line.startswith("echo"):
                lines.append(line)
    return lines


def get_test_images_for_lang(
    lang: str,
    release_arg: str,
    image_path_prefix: str,
    spec: Optional[InteropSpec] = None,
) -> Dict[str, List[Tuple[str, str]]]:
    """Find docker images for a language across releases and runtimes."""
    active_spec = spec or get_spec()
    known_releases = get_release_tags(lang, spec=active_spec)
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

    images: Dict[str, List[Tuple[str, str]]] = {}
    for tag in releases:
        for runtime in get_runtimes_for_lang_release(
            lang, tag, spec=active_spec
        ):
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


def generate_test_case_jobspecs(
    lang: str,
    runtime: str,
    release: str,
    suite_name: str,
    server_host: str = "74.125.206.210",
    allow_flakes: bool = False,
    spec: Optional[InteropSpec] = None,
) -> List[jobset.JobSpec]:
    """Returns the list of test case JobSpecs for (lang, runtime, release)."""
    active_spec = spec or get_spec()
    testcase_lines = read_test_cases_file(
        lang, runtime, release, spec=active_spec
    )
    timeout_sec = int(
        active_spec.matrix_config.get("test_timeout_seconds", 60)
    )
    flake_retries = (
        int(active_spec.defaults.get("matrix_flake_retries", 5))
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


def pull_image_for_lang(
    lang: str,
    image: str,
    release: str,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    """Pull an image for a given language from the image registry."""
    active_spec = spec or get_spec()
    pull_timeout = int(
        active_spec.matrix_config.get("pull_image_timeout_seconds", 15 * 60)
    )
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


def cleanup_docker_image(image: str) -> None:
    jobset.message("START", "Cleanup docker image %s" % image, do_newline=True)
    dockerjob.remove_image(image, skip_nonexistent=True)


def build_matrix_image_jobspec(
    runtime: str,
    env: Dict[str, str],
    gcr_tag: str,
    stack_base: str,
    lang: str,
    docker_path: str = "us-docker.pkg.dev/grpc-testing/testing-images-public",
    direct_build: bool = False,
    commit_log_path: Optional[str] = None,
    spec: Optional[InteropSpec] = None,
) -> jobset.JobSpec:
    """Build interop docker image for a language with runtime."""
    active_spec = spec or get_spec()
    basename = "grpc_interop_%s" % runtime
    tag = "%s/%s:%s" % (docker_path, basename, gcr_tag)
    timeout_sec = int(
        active_spec.matrix_config.get("build_image_timeout_seconds", 30 * 60)
    )

    if direct_build:
        builder = DockerImageBuilder(active_spec)
        job = builder.create_direct_build_jobspec(
            runtime,
            tag=tag,
            release=gcr_tag,
            commit_log_path=commit_log_path,
            context_dir=stack_base if stack_base else None,
            timeout_seconds=timeout_sec,
        )
        job.environ.update(env)
        return job

    build_env = {"INTEROP_IMAGE": tag, "BASE_NAME": basename}
    build_env.update(env)
    image_builder_path = active_spec.defaults.get(
        "legacy_image_builder_script",
        "tools/run_tests/dockerize/build_interop_image.sh",
    )
    if should_build_docker_interop_image_from_release_tag(
        lang, spec=active_spec
    ):
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
    stack_base: str,
    lang: str,
    release: str,
    patches_base_dir: Optional[str] = None,
    spec: Optional[InteropSpec] = None,
) -> None:
    active_spec = spec or get_spec()
    files_to_patch = []
    release_info = active_spec.lang_release_matrix[lang].get(release)
    if release_info:
        files_to_patch = release_info.patch
    if not files_to_patch:
        return
    base_dir = patches_base_dir or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../interop_matrix")
    )
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
    lang: str,
    release: str,
    git_checkout: bool,
    git_checkout_root: str,
    reuse_git_root: bool = False,
    patches_base_dir: Optional[str] = None,
    spec: Optional[InteropSpec] = None,
) -> str:
    """Invokes 'git checkout' for the lang/release and returns directory created."""
    active_spec = spec or get_spec()
    assert git_checkout and git_checkout_root

    if not os.path.exists(git_checkout_root):
        os.makedirs(git_checkout_root)

    repo = get_github_repo(lang, spec=active_spec)
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
        spec=active_spec,
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


class MatrixRunner:
    """Runs or dry-runs backward-compatibility matrix test suites."""

    def __init__(self, spec: Optional[InteropSpec] = None):
        self.spec = spec or get_spec()

    def test_release(
        self,
        lang: str,
        runtime: str,
        release: str,
        image: str,
        xml_report_tree,
        skip_tests: bool,
        server_host: str,
        allow_flakes: bool,
        jobs: int,
        bq_result_table: str = "",
    ) -> int:
        total_num_failures = 0
        suite_name = "%s__%s_%s" % (lang, runtime, release)
        job_spec_list = generate_test_case_jobspecs(
            lang,
            runtime,
            release,
            suite_name,
            server_host=server_host,
            allow_flakes=allow_flakes,
            spec=self.spec,
        )

        if not job_spec_list:
            jobset.message(
                "FAILED", "No test cases were found.", do_newline=True
            )
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
                self.spec.suites["matrix"].get(
                    "junit_package", "grpc_interop_matrix"
                ),
                suite_name,
                str(uuid.uuid4()),
            )
        return total_num_failures

    def run_tests_for_lang(
        self,
        lang: str,
        runtime: str,
        images: Sequence[Tuple[str, str]],
        xml_report_tree,
        args,
    ) -> int:
        skip_tests = False
        total_num_failures = 0

        max_downloads = int(
            self.spec.matrix_config.get("max_parallel_downloads", 6)
        )
        max_pull_jobs = min(args.jobs, max_downloads)
        max_chunk_size = max_pull_jobs
        chunk_count = (len(images) + max_chunk_size) // max_chunk_size

        for chunk_index in range(chunk_count):
            chunk_start = chunk_index * max_chunk_size
            chunk_size = min(max_chunk_size, len(images) - chunk_start)
            chunk_end = chunk_start + chunk_size
            pull_specs = []
            if not skip_tests:
                for release, image in images[chunk_start:chunk_end]:
                    pull_specs.append(
                        pull_image_for_lang(
                            lang, image, release, spec=self.spec
                        )
                    )

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
                total_num_failures += self.test_release(
                    lang,
                    runtime,
                    release,
                    image,
                    xml_report_tree,
                    skip_tests,
                    server_host=args.server_host,
                    allow_flakes=args.allow_flakes,
                    jobs=args.jobs,
                    bq_result_table=args.bq_result_table,
                )
            if not args.keep:
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

    def execute(self, args) -> int:
        all_langs = list(self.spec.lang_runtime_matrix.keys())
        languages = args.language if args.language != ["all"] else all_langs
        total_num_failures = 0
        xml_report_tree = report_utils.new_junit_xml_tree()
        for lang in languages:
            docker_images = get_test_images_for_lang(
                lang, args.release, args.docker_path, spec=self.spec
            )
            for runtime in sorted(docker_images.keys()):
                total_num_failures += self.run_tests_for_lang(
                    lang,
                    runtime,
                    docker_images[runtime],
                    xml_report_tree,
                    args,
                )

        report_utils.create_xml_report_file(xml_report_tree, args.report_file)
        return 1 if total_num_failures else 0
