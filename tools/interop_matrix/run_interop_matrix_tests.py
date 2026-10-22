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
"""Compatibility shim for running matrix tests via tools/interop/harness.py."""

import os
import sys
import types

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tools.interop import harness as _harness

for _k, _v in _harness.__dict__.items():
    if not _k.startswith("__"):
        globals()[_k] = _v

_LANGUAGES = _harness.MATRIX_LANGUAGES
argp = _harness.build_run_matrix_arg_parser()
args = argp.parse_args([])
main = _harness.run_interop_matrix_tests_main


class _ShimModule(types.ModuleType):
    def __getattr__(self, name):
        return getattr(_harness, name)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if not name.startswith("__") and hasattr(_harness, name):
            setattr(_harness, name, value)


sys.modules[__name__].__class__ = _ShimModule

if __name__ == "__main__":
    main()
