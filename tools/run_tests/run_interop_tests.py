#!/usr/bin/env python3
# Copyright 2015 gRPC authors.
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
"""Thin compatibility shim delegating to tools/interop/harness.py."""

import os
import sys
import types

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tools.interop import harness as _harness
from tools.interop.harness import *  # noqa: F401,F403

for _k, _v in _harness.__dict__.items():
    if not _k.startswith("__"):
        globals()[_k] = _v


class _ShimModule(types.ModuleType):
    def __getattr__(self, name):
        return getattr(_harness, name)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if not name.startswith("__") and hasattr(_harness, name):
            setattr(_harness, name, value)


sys.modules[__name__].__class__ = _ShimModule

if __name__ == "__main__":
    _harness.main()
