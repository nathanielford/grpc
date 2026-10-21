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
"""Docker Buildx Bake and Compose Native Orchestrator for gRPC Interop Tests."""

from .bake_manifest import DEFAULT_BASE_IMAGES
from .bake_manifest import LANGUAGE_TO_SAFENAME
from .bake_manifest import deduplicate_language_builds
from .bake_manifest import generate_bake_hcl
from .bake_manifest import is_buildx_bake_available
from .bake_manifest import read_current_version
from .bake_manifest import resolve_repo_contexts
from .bake_manifest import safename_for_language
from .bake_manifest import safename_to_bake_target
from .compose_manifest import DEFAULT_COMPOSE_NETWORK
from .compose_manifest import DEFAULT_SERVER_PORT
from .compose_manifest import generate_compose_dict
from .compose_manifest import generate_compose_yaml
from .compose_manifest import server_docker_healthcheck_args
from .compose_manifest import server_has_runnable_tests
from .compose_manifest import server_healthcheck_cmd
from .compose_manifest import server_service_name

__all__ = [
    "DEFAULT_BASE_IMAGES",
    "DEFAULT_COMPOSE_NETWORK",
    "DEFAULT_SERVER_PORT",
    "LANGUAGE_TO_SAFENAME",
    "deduplicate_language_builds",
    "generate_bake_hcl",
    "generate_compose_dict",
    "generate_compose_yaml",
    "is_buildx_bake_available",
    "read_current_version",
    "resolve_repo_contexts",
    "safename_for_language",
    "safename_to_bake_target",
    "server_docker_healthcheck_args",
    "server_has_runnable_tests",
    "server_healthcheck_cmd",
    "server_service_name",
]
