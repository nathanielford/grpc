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
"""Unified declarative gRPC interop specification, image builder, and test runner."""

from .builder import DockerImageBuilder, add_files_to_image
from .cli import (
    build_interop_argparser,
    build_matrix_build_argparser,
    build_matrix_test_argparser,
    main,
    run_interop_cli,
    run_matrix_test_cli,
)
from .matrix import (
    LANG_RELEASE_MATRIX,
    LANG_RUNTIME_MATRIX,
    MatrixRunner,
    build_matrix_image_jobspec,
    checkout_grpc_stack,
    cleanup_docker_image,
    generate_test_case_jobspecs,
    get_all_releases,
    get_github_repo,
    get_release_tags,
    get_runtimes_for_lang_release,
    get_test_images_for_lang,
    maybe_apply_patches_on_git_tag,
    pull_image_for_lang,
    read_test_cases_file,
    should_build_docker_interop_image_from_release_tag,
)
from .runner import (
    ExecutionPlan,
    InteropRunner,
    aggregate_http2_results,
    auth_options,
    bash_cmdline,
    cloud_to_cloud_jobspec,
    cloud_to_prod_jobspec,
    compute_engine_creds_required,
    docker_run_cmdline,
    manual_cmdline,
    server_jobspec,
    set_default_allow_flakes,
    write_cmdlog_maybe,
)
from .spec import (
    AuthTestCaseSpec,
    CommandSpec,
    DEFAULT_SPEC_PATH,
    DockerBuildSpec,
    HealthcheckSpec,
    Http2ClientTestCaseSpec,
    Http2ServerTestCaseSpec,
    InteropSpec,
    LanguageSpec,
    MatrixTestcaseProfile,
    ReleaseInfo,
    ServerDockerConfig,
    SpecValidationError,
    StandardTestCaseSpec,
    get_spec,
    load_spec,
    validate_spec_data,
)

__all__ = [
    "AuthTestCaseSpec",
    "CommandSpec",
    "DEFAULT_SPEC_PATH",
    "DockerBuildSpec",
    "DockerImageBuilder",
    "ExecutionPlan",
    "HealthcheckSpec",
    "Http2ClientTestCaseSpec",
    "Http2ServerTestCaseSpec",
    "InteropRunner",
    "InteropSpec",
    "LANG_RELEASE_MATRIX",
    "LANG_RUNTIME_MATRIX",
    "LanguageSpec",
    "MatrixRunner",
    "MatrixTestcaseProfile",
    "ReleaseInfo",
    "ServerDockerConfig",
    "SpecValidationError",
    "StandardTestCaseSpec",
    "add_files_to_image",
    "aggregate_http2_results",
    "auth_options",
    "bash_cmdline",
    "build_interop_argparser",
    "build_matrix_build_argparser",
    "build_matrix_image_jobspec",
    "build_matrix_test_argparser",
    "checkout_grpc_stack",
    "cleanup_docker_image",
    "cloud_to_cloud_jobspec",
    "cloud_to_prod_jobspec",
    "compute_engine_creds_required",
    "docker_run_cmdline",
    "generate_test_case_jobspecs",
    "get_all_releases",
    "get_github_repo",
    "get_release_tags",
    "get_runtimes_for_lang_release",
    "get_spec",
    "get_test_images_for_lang",
    "load_spec",
    "main",
    "manual_cmdline",
    "maybe_apply_patches_on_git_tag",
    "pull_image_for_lang",
    "read_test_cases_file",
    "run_interop_cli",
    "run_matrix_test_cli",
    "server_jobspec",
    "set_default_allow_flakes",
    "should_build_docker_interop_image_from_release_tag",
    "validate_spec_data",
    "write_cmdlog_maybe",
]
