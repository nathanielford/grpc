#!/usr/bin/env python3
# Copyright 2026 gRPC authors.
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
"""Hot-swap check: executes the legacy and the new interop harness and compares.

test_interop.py proves that both harnesses *generate* the same JobSpecs. This
tool *executes* both end to end with Docker, so it also covers everything a
JobSpec does not capture: image builds, server start-up, per-test outcomes and
reports. Migration tooling: delete it together with test_interop.py's
MigrationParityTest once the legacy harness is gone.

Stages (run in order with `all`; each writes <work_dir>/results/<stage>.json):
  setup         Standalone clones of --last_ref (legacy harness) and
                --next_ref (this harness), both with submodules, plus sibling
                repos (grpc-go/java/node/dart/dotnet) cloned once and shared
                by both harnesses through GRPC_<LANG>_ROOT. All in 'files' ref
                format, which the git in the legacy build images requires.
  kokoro        The Linux Kokoro tocloud + toprod (non-auth) + http2 scenario,
                built and run by each harness; per-test outcomes compared.
  matrix-test   Matrix tests for representative releases (one per client
                entrypoint profile; images pulled from Artifact Registry).
  matrix-build  Matrix image builds for representative releases with each
                harness; each image then runs its matrix test cases.
  report        Markdown summary of all stage results.

Requires Linux, Docker with buildx, network access to GitHub, Artifact Registry
and the grpc-test sandbox, and roughly 60 GB of free disk. Takes hours. Not
covered: the ALTS job (needs GCE) and the cloud_to_prod_auth job (needs the
Kokoro service account key). After every harness run it removes the images,
containers and BuildKit cache records the run left behind, so every run builds
cold, as on a fresh Kokoro VM.
"""

import argparse
import datetime
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
_SIBLINGS = ("go", "java", "node", "dart", "dotnet")
# Recent host git creates reftable repositories by default; the git inside the
# legacy build images cannot read them (create_matrix_images.py passes the same
# flag for its clones).
_FILES_REF_FORMAT = ["-c", "init.defaultRefFormat=files"]

# Linux Kokoro jobs (tools/internal_ci/linux/grpc_interop_{tocloud,toprod}.cfg)
# folded into one invocation; toprod without --cloud_to_prod_auth.
_KOKORO_ARGS = [
    "-l", "all", "-s", "all", "--use_docker", "--http2_interop",
    "--cloud_to_prod", "--prod_servers", "default", "gateway_v4",
    "-t", "-j", "8",
]  # fmt: skip

# --split: the same scenario as several smaller runs, so peak disk use fits a
# workstation. The legacy builder commits each image from its build container
# (twice the image size while committing), and the C-core based images are
# 3-8 GB each, so all of them at once need about 40 GB. Group a has c++ as the
# common peer (and the java-mcs test); the others use go, which builds in a
# minute. Every image, server, http2 and cloud_to_prod test is still
# exercised; only cross-group client/server pairs are not (JobSpec parity
# covers those statically).
_KOKORO_GROUPS = {
    "a": (
        ["c++", "go", "java", "javaokhttp", "dart"],
        ["c++", "go", "java", "dart"],
    ),
    "b1": (["go", "python"], ["go", "python"]),
    "b2": (["go", "pythonasyncio"], ["go", "pythonasyncio"]),
    "b3": (["go", "ruby", "php8"], ["go", "ruby", "php8"]),
    "c": (
        ["go", "node", "nodepurejs", "aspnetcore"],
        ["go", "node", "aspnetcore"],
    ),
}
_KOKORO_GROUP_FLAGS = [
    "--use_docker", "--http2_interop", "--cloud_to_prod",
    "--prod_servers", "default", "gateway_v4", "-t", "-j", "8",
]  # fmt: skip

# Tests only the legacy harness runs, by design: the new harness never starts
# a pythonasyncio server because pythonasyncio has no server test cases.
_EXPECTED_ONLY_LAST = ("_server_pythonasyncio", "pythonasyncio_server")

# One release per client entrypoint profile in config.json, plus the newest
# release of every language.
_MATRIX_TEST_RELEASES = [
    "cxx:v1.0.1", "cxx:v1.20.0", "cxx:v1.83.0",
    "go:v1.0.5", "go:v1.20.0", "go:v1.84.0",
    "java:v1.0.3", "java:v1.85.1",
    "python:v1.0.x", "python:v1.11.1", "python:v1.18.0", "python:v1.41.1",
    "python:v1.44.0", "python:v1.83.0",
    "node:v1.0.1", "node:v1.1.4", "node:v1.12.4",
    "ruby:v1.0.1", "ruby:v1.1.4", "ruby:v1.83.0",
    "php:v1.0.1", "php:v1.80.0",
    "csharp:v1.1.4", "csharp:v1.3.9", "csharp:v1.18.0", "csharp:v1.20.0",
    "csharp:v1.46.2",
]  # fmt: skip

# cxx covers each .dockerignore era of release checkouts: none (<= v1.57),
# root-only .git (v1.58 - v1.85) and '**/.git' (upstream master, i.e. v1.86+).
# go/java/node are built from HEAD Dockerfiles, which differ between harnesses.
_MATRIX_BUILD_RELEASES = [
    "cxx:v1.57.0", "cxx:v1.83.0", "cxx:master", "python:v1.83.0",
    "go:v1.84.0", "java:v1.85.1", "node:v1.12.4",
]  # fmt: skip

_HARNESS = {
    "last": {
        "interop": ["tools/run_tests/run_interop_tests.py"],
        "matrix-test": ["tools/interop_matrix/run_interop_matrix_tests.py"],
        "matrix-build": ["tools/interop_matrix/create_matrix_images.py"],
    },
    "next": {
        "interop": ["tools/interop/runner.py"],
        "matrix-test": ["tools/interop/runner.py", "matrix-test"],
        "matrix-build": ["tools/interop/runner.py", "matrix-build"],
    },
}


class Work:
    """Paths under --work_dir."""

    def __init__(self, work_dir):
        self.root = os.path.abspath(work_dir)
        self.logs = os.path.join(self.root, "logs")
        self.results = os.path.join(self.root, "results")
        self.siblings = os.path.join(self.root, "siblings")
        for d in (self.logs, self.results, self.siblings):
            os.makedirs(d, exist_ok=True)

    def tree(self, flavor):
        return os.path.join(self.root, flavor, "grpc")

    def result(self, name):
        return os.path.join(self.results, name)


def _log(msg):
    stamp = datetime.datetime.now().strftime("%H:%M:%S")
    print("[%s] %s" % (stamp, msg), flush=True)


_RUNNING = []  # process groups to kill if this tool is terminated
# Set by main(); *_before are set by _snapshot() while a harness runs.
_GUARD = {
    "path": None,
    "min_free_gb": 0,
    "work": None,
    "images_before": None,
    "cache_before": None,
    "dangling_before": None,
}
# Both harnesses tag the images they build grpc_interop_<lang>:<uuid4>.
_HARNESS_IMAGE_RE = re.compile(
    r"\bgrpc_interop_[a-z0-9_]+:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b"
)


def _kill_group(proc):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, sig)
            proc.wait(timeout=30)
            return
        except ProcessLookupError:
            return
        except subprocess.TimeoutExpired:
            continue


def _terminate(signum, unused_frame):
    for proc in list(_RUNNING):
        _kill_group(proc)
    if _GUARD["work"]:
        for flavor in ("last", "next"):
            _cleanup_after(_GUARD["work"], flavor, "", _GUARD["images_before"])
        if _GUARD["dangling_before"] is not None:
            _remove_new_dangling_images(_GUARD["dangling_before"])
        if _GUARD["cache_before"] is not None:
            _prune_build_cache(_GUARD["cache_before"])
    sys.exit(128 + signum)


def _run(cmd, cwd=None, env=None, log_path=None, timeout=None, check=True):
    """Runs cmd in its own process group with stdin closed.

    Output is appended to log_path. Returns the exit code, or "timeout" /
    "low_disk" if the whole group had to be killed.
    """
    _log("$ (cd %s) %s" % (cwd or ".", " ".join(cmd)))
    out = open(log_path, "a") if log_path else subprocess.DEVNULL
    if log_path:
        out.write("\n### %s\n### cwd=%s\n" % (" ".join(cmd), cwd))
        out.flush()
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=out,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    _RUNNING.append(proc)
    start = time.time()
    try:
        while True:
            try:
                code = proc.wait(timeout=15)
                break
            except subprocess.TimeoutExpired:
                pass
            if timeout and time.time() - start > timeout:
                code = "timeout"
            elif (
                _GUARD["path"]
                and _free_gb(_GUARD["path"]) < _GUARD["min_free_gb"]
            ):
                code = "low_disk"
            else:
                continue
            _log("killing %s: %s" % (code, " ".join(cmd)))
            _kill_group(proc)
            break
    finally:
        _RUNNING.remove(proc)
        if log_path:
            out.close()
    if check and code != 0:
        raise RuntimeError("command failed (%s): %s" % (code, " ".join(cmd)))
    return code


def _harness_images():
    out = subprocess.check_output(
        ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], text=True
    )
    return set(_HARNESS_IMAGE_RE.findall(out))


def _build_cache_ids():
    out = subprocess.run(
        ["docker", "buildx", "du", "--builder", "default", "--verbose"],
        capture_output=True,
        text=True,
    ).stdout
    return set(re.findall(r"^ID:\s+(\S+)", out, re.M))


def _prune_build_cache(ids_before):
    """Prunes the BuildKit cache records created since ids_before.

    The new harness builds with BuildKit, which keeps a cache; the legacy
    builder commits containers and keeps none. Pruning after every run keeps
    disk use bounded and makes every run build cold, as on a fresh Kokoro VM.
    Two passes: a parent record is only reclaimable once its children are gone.
    """
    for _ in range(2):
        for cache_id in sorted(_build_cache_ids() - ids_before):
            subprocess.call(
                [
                    "docker",
                    "buildx",
                    "prune",
                    "--builder",
                    "default",
                    "-f",
                    "--filter",
                    "id=%s" % cache_id,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


def _snapshot():
    """Records the harness images and build cache present before a run."""
    _GUARD["images_before"] = _harness_images()
    _GUARD["cache_before"] = _build_cache_ids()
    _GUARD["dangling_before"] = _dangling_images()


def _dangling_images():
    out = subprocess.check_output(
        ["docker", "images", "-q", "--no-trunc", "--filter", "dangling=true"],
        text=True,
    )
    return set(out.split())


def _remove_new_dangling_images(dangling_before):
    """Removes untagged images created since dangling_before.

    create_matrix_images.py (and matrix-build, which runs the same JobSpecs)
    labels a committed image by building a child with the same tag, which
    leaves the 2-8 GB committed image untagged. Without -f, so images that
    are still in use or have children are left alone.
    """
    for image_id in sorted(_dangling_images() - dangling_before):
        subprocess.call(
            ["docker", "rmi", image_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def _cleanup_run(work, flavor, log_path, mount_root=None):
    """Removes what the run since _snapshot() left behind."""
    _cleanup_after(work, flavor, log_path, _GUARD["images_before"], mount_root)
    if _GUARD["dangling_before"] is not None:
        _remove_new_dangling_images(_GUARD["dangling_before"])
    if _GUARD["cache_before"] is not None:
        _prune_build_cache(_GUARD["cache_before"])
    _GUARD["images_before"] = _GUARD["cache_before"] = None
    _GUARD["dangling_before"] = None


def _cleanup_after(work, flavor, log_path, images_before, mount_root=None):
    """Removes the containers and images a harness run left behind.

    That is everything a killed run built, the legacy harness's java-mcs
    server, which it starts but never stops, and containers of failed legacy
    builds, which build_interop_image.sh does not remove. images_before: the
    harness image tags present before the run (None: only remove containers
    that mount mount_root, by default the flavor's tree).
    """
    leaked = set()
    if images_before is not None:
        leaked = _harness_images() - images_before
    if log_path and os.path.isfile(log_path):
        with open(log_path) as f:
            leaked |= set(_HARNESS_IMAGE_RE.findall(f.read()))
    mount_root = mount_root or work.tree(flavor)
    image_ids = set()
    for cid in subprocess.check_output(
        ["docker", "ps", "-aq"], text=True
    ).split():
        fields = subprocess.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{.Config.Image}}|{{.Image}}|{{range .Mounts}}{{.Source}} {{end}}",
                cid,
            ],
            capture_output=True,
            text=True,
        ).stdout.split("|", 2)
        if len(fields) != 3:
            continue  # gone meanwhile
        ref, image_id, mounts = fields
        if ref in leaked or mount_root in mounts:
            subprocess.call(
                ["docker", "rm", "-f", cid], stdout=subprocess.DEVNULL
            )
            if ref in leaked:
                image_ids.add(image_id)
    for image in sorted(leaked):
        subprocess.call(
            ["docker", "rmi", "-f", image],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    # Images the harness untagged while a container still used them. Without
    # -f, so an ID that still has another tag is left alone.
    for image_id in sorted(image_ids):
        subprocess.call(
            ["docker", "rmi", image_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def _git(*args, cwd=None):
    return subprocess.check_output(
        ["git"] + list(args), cwd=cwd, text=True
    ).strip()


def _ref_format(path):
    try:
        return _git("rev-parse", "--show-ref-format", cwd=path)
    except subprocess.CalledProcessError:
        return "files"  # git without reftable support


def _repos(path):
    """path and the working trees of all its (recursive) submodules."""
    out = _git(
        "submodule", "foreach", "--quiet", "--recursive", "pwd", cwd=path
    )
    return [path] + [line for line in out.splitlines() if line]


def _non_files_repos(path):
    return [r for r in _repos(path) if _ref_format(r) != "files"]


def _free_gb(path):
    return shutil.disk_usage(path).free / 1e9


def _require_disk(work, gb, wait_minutes=30):
    """Waits up to wait_minutes for gb of free disk under the work dir."""
    deadline = time.time() + 60 * wait_minutes
    while True:
        free = _free_gb(work.root)
        if free >= gb:
            return
        if time.time() > deadline:
            raise RuntimeError(
                "%.0f GB free under %s, need %d GB" % (free, work.root, gb)
            )
        _log("waiting for disk: %.0f GB free, need %d GB" % (free, gb))
        time.sleep(60)


def _harness_env(work):
    env = dict(os.environ)
    for name in _SIBLINGS:
        env["GRPC_%s_ROOT" % name.upper()] = os.path.join(
            work.siblings, "grpc-%s" % name
        )
    return env


# The legacy matrix runner imports the BigQuery client libraries
# unconditionally (Kokoro workers have them; they are only used by
# --bq_result_table). Import-only stand-ins for whichever are missing.
_BQ_STUBS = {
    "apiclient": {
        "__init__.py": "",
        "discovery.py": "def build(*args, **kwargs):\n    raise NotImplementedError\n",
        "errors.py": "class HttpError(Exception):\n    pass\n",
    },
    "httplib2": {"__init__.py": ""},
    "oauth2client": {
        "__init__.py": "",
        "client.py": "class GoogleCredentials:\n    pass\n",
    },
}


# The legacy matrix runner pulls with `gcloud docker -- pull`, which current
# Cloud SDKs reject (exit 120) for Docker clients newer than 18.03; this shim
# runs the docker command gcloud used to run. The new harness runs
# `docker pull` itself (a known, intended difference).
_GCLOUD_SHIM = """#!/bin/bash
if [ "$1" = docker ]; then
  shift
  if [ "$1" = -- ]; then shift; fi
  exec docker "$@"
fi
exec %s "$@"
"""


def _legacy_env(work):
    env = _harness_env(work)
    real_gcloud = shutil.which("gcloud")
    if real_gcloud:
        shim_dir = os.path.join(work.root, "gcloud_shim")
        os.makedirs(shim_dir, exist_ok=True)
        shim = os.path.join(shim_dir, "gcloud")
        with open(shim, "w") as f:
            f.write(_GCLOUD_SHIM % real_gcloud)
        os.chmod(shim, 0o755)
        env["PATH"] = shim_dir + os.pathsep + env.get("PATH", "")
    stub_root = os.path.join(work.root, "bq_stubs")
    for module, files in _BQ_STUBS.items():
        found = subprocess.call(
            [sys.executable, "-c", "import %s" % module],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if found == 0:
            continue
        os.makedirs(os.path.join(stub_root, module), exist_ok=True)
        for name, content in files.items():
            with open(os.path.join(stub_root, module, name), "w") as f:
                f.write(content)
    if os.path.isdir(stub_root):
        env["PYTHONPATH"] = os.pathsep.join(
            p for p in (stub_root, env.get("PYTHONPATH")) if p
        )
    return env


def _save(work, name, data):
    with open(work.result(name), "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)


def _load(work, name):
    path = work.result(name)
    if not os.path.isfile(path):
        return None
    with open(path) as f:
        return json.load(f)


def junit_outcomes(path):
    """Returns {testcase name: PASSED|FAILED|TIMEOUT|SKIPPED} from a junit report."""
    outcomes = {}
    if not os.path.isfile(path):
        return outcomes
    for case in ET.parse(path).getroot().iter("testcase"):
        state = "PASSED"
        if case.find("failure") is not None:
            state = "FAILED"
        elif case.find("error") is not None:
            state = "TIMEOUT"
        elif case.find("skipped") is not None:
            state = "SKIPPED"
        name = case.get("name")
        # Keep the worst outcome if a name repeats.
        if outcomes.get(name, "PASSED") == "PASSED":
            outcomes[name] = state
    return outcomes


def compare_outcomes(last, nxt, expected_only_last=()):
    """Compares two {name: state} maps."""
    only_last = sorted(set(last) - set(nxt))
    expected = [n for n in only_last if any(e in n for e in expected_only_last)]
    unexpected_only_last = [n for n in only_last if n not in expected]
    only_next = sorted(set(nxt) - set(last))
    differ = sorted(n for n in set(last) & set(nxt) if last[n] != nxt[n])
    counts = lambda m: {
        s: list(m.values()).count(s) for s in sorted(set(m.values()))
    }
    return {
        "last_counts": counts(last),
        "next_counts": counts(nxt),
        "common": len(set(last) & set(nxt)),
        "same_outcome": len(set(last) & set(nxt)) - len(differ),
        "differ": [
            {"name": n, "last": last[n], "next": nxt[n]} for n in differ
        ],
        "only_last_expected": expected,
        "only_last": unexpected_only_last,
        "only_next": only_next,
        "match": not differ and not unexpected_only_last and not only_next,
    }


# --------------------------------------------------------------------------
# setup
# --------------------------------------------------------------------------


def stage_setup(work, args):
    info = {"refs": {}, "siblings": {}}
    for flavor, ref in (("last", args.last_ref), ("next", args.next_ref)):
        sha = _git("rev-parse", "%s^{commit}" % ref, cwd=_REPO_ROOT)
        path = work.tree(flavor)
        # Standalone clones, as on Kokoro: a worktree's .git file points at an
        # absolute host path, which the legacy builder's containers cannot see.
        if os.path.isfile(os.path.join(path, ".git")):
            _run(["git", "worktree", "remove", "--force", path], cwd=_REPO_ROOT)
        if not os.path.isdir(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            _run(
                ["git"]
                + _FILES_REF_FORMAT
                + ["clone", "--quiet", "--no-checkout", _REPO_ROOT, path]
            )
        _run(
            [
                "git",
                "fetch",
                "--quiet",
                _REPO_ROOT,
                "+refs/heads/*:refs/remotes/src/*",
            ],
            cwd=path,
        )
        _run(
            ["git", "checkout", "--quiet", "--force", "--detach", sha], cwd=path
        )
        _run(["git", "clean", "-ffdxq"], cwd=path)
        if _non_files_repos(path):
            _run(["git", "submodule", "deinit", "--force", "--all"], cwd=path)
            git_dir = _git("rev-parse", "--absolute-git-dir", cwd=path)
            shutil.rmtree(os.path.join(git_dir, "modules"), ignore_errors=True)
        _run(
            ["git"]
            + _FILES_REF_FORMAT
            + ["submodule", "update", "--init", "--jobs", "8"],
            cwd=path,
            log_path=os.path.join(work.logs, "setup.log"),
        )
        info["refs"][flavor] = {"ref": ref, "sha": sha, "path": path}
    for name in _SIBLINGS:
        path = os.path.join(work.siblings, "grpc-%s" % name)
        if os.path.isdir(path) and _non_files_repos(path):
            shutil.rmtree(path)
        if not os.path.isdir(path):
            url = "https://github.com/grpc/grpc-%s" % name
            _run(
                ["git"]
                + _FILES_REF_FORMAT
                + ["clone", "--recursive", url, path],
                log_path=os.path.join(work.logs, "setup.log"),
            )
        info["siblings"][name] = _git("rev-parse", "HEAD", cwd=path)
    # The next tree has submodules checked out, so this also covers third_party.
    _run(
        [
            sys.executable,
            "tools/interop/test_interop.py",
            "InteropInvariantTest.test_invariant_docker_context_keeps_tracked_files",
        ],
        cwd=work.tree("next"),
        log_path=os.path.join(work.logs, "setup.log"),
    )
    bad = []
    for path in [work.tree("last"), work.tree("next")] + [
        os.path.join(work.siblings, "grpc-%s" % name) for name in _SIBLINGS
    ]:
        bad += _non_files_repos(path)
    if bad:
        raise RuntimeError("repositories not in 'files' ref format: %s" % bad)
    _save(work, "setup.json", info)
    _log("setup: %s" % json.dumps(info["refs"], sort_keys=True))


# --------------------------------------------------------------------------
# kokoro
# --------------------------------------------------------------------------


def stage_kokoro(work, args):
    if not args.split:
        return _kokoro_run(work, args, "kokoro", list(args.kokoro_args))
    ok = True
    for name in args.groups or sorted(_KOKORO_GROUPS):
        clients, servers = _KOKORO_GROUPS[name]
        argv = ["-l"] + clients + ["-s"] + servers + _KOKORO_GROUP_FLAGS
        ok = _kokoro_run(work, args, "kokoro_%s" % name, argv) and ok
    return ok


def _kokoro_run(work, args, label, argv):
    runs = {}
    env = _harness_env(work)
    for flavor in ("last", "next"):
        tree = work.tree(flavor)
        meta_path = work.result("%s_%s.meta.json" % (label, flavor))
        meta = {"sha": _git("rev-parse", "HEAD", cwd=tree), "args": argv}
        if flavor == "last" and args.reuse_last and os.path.isfile(meta_path):
            with open(meta_path) as f:
                saved = json.load(f)
            if {k: saved.get(k) for k in meta} == meta:
                runs[flavor] = dict(
                    saved,
                    tests=junit_outcomes(
                        work.result("%s_%s_tests.xml" % (label, flavor))
                    ),
                    builds=junit_outcomes(
                        work.result("%s_%s_builds.xml" % (label, flavor))
                    ),
                )
                _log("%s %s: reusing run of %s" % (label, flavor, meta["sha"]))
                continue
        for d in ("interop_test", "interop_docker_build"):
            shutil.rmtree(os.path.join(tree, d), ignore_errors=True)
        _require_disk(work, args.min_free_gb)
        log_path = os.path.join(work.logs, "%s_%s.log" % (label, flavor))
        start = time.time()
        _snapshot()
        code = _run(
            [sys.executable] + _HARNESS[flavor]["interop"] + argv,
            cwd=tree,
            env=env,
            log_path=log_path,
            timeout=args.kokoro_timeout,
            check=False,
        )
        _cleanup_run(work, flavor, log_path)
        reports = {}
        for kind, rel in (
            ("tests", "interop_test/sponge_log.xml"),
            ("builds", "interop_docker_build/sponge_log.xml"),
        ):
            dst = work.result("%s_%s_%s.xml" % (label, flavor, kind))
            if os.path.exists(dst):
                os.remove(dst)
            if os.path.isfile(os.path.join(tree, rel)):
                shutil.copy2(os.path.join(tree, rel), dst)
            reports[kind] = junit_outcomes(dst)
        runs[flavor] = {
            "exit_code": code,
            "minutes": round((time.time() - start) / 60, 1),
            "tests": reports["tests"],
            "builds": reports["builds"],
        }
        meta.update(exit_code=code, minutes=runs[flavor]["minutes"])
        _save(work, os.path.basename(meta_path), meta)
        _log(
            "%s %s: exit=%s tests=%d builds=%s"
            % (label, flavor, code, len(reports["tests"]), reports["builds"])
        )
        if code == "low_disk":
            break
    if "next" not in runs:
        result = {"args": argv, "aborted": "low_disk", "match": False}
        _save(work, "%s.json" % label, result)
        return False
    result = {
        "args": argv,
        "runs": {
            f: {k: v for k, v in r.items() if k != "tests"}
            for f, r in runs.items()
        },
        "tests": compare_outcomes(
            runs["last"]["tests"], runs["next"]["tests"], _EXPECTED_ONLY_LAST
        ),
    }
    builds_ok = all(
        r["builds"] and set(r["builds"].values()) == {"PASSED"}
        for r in runs.values()
    )
    result["match"] = (
        result["tests"]["match"]
        and builds_ok
        and runs["last"]["exit_code"] == runs["next"]["exit_code"]
    )
    _save(work, "%s.json" % label, result)
    return result["match"]


# --------------------------------------------------------------------------
# matrix-test
# --------------------------------------------------------------------------


def stage_matrix_test(work, args):
    previous = _load(work, "matrix_test.json") or {}
    envs = {"last": _legacy_env(work), "next": _harness_env(work)}
    for item in args.matrix_test_releases:
        if item in previous and not args.force:
            continue
        lang, release = item.split(":", 1)
        entry = {}
        for flavor in ("last", "next"):
            report = work.result(
                "matrix_test_%s_%s_%s.xml" % (lang, release, flavor)
            )
            if os.path.exists(report):
                os.remove(report)
            _require_disk(work, args.min_free_gb)
            cmd = [sys.executable] + _HARNESS[flavor]["matrix-test"] + [
                "--language", lang, "--release", release, "--allow_flakes",
                "--report_file", report, "-j", "8",
            ]  # fmt: skip
            if flavor == "last":
                cmd.append("--keep")  # the next run reuses the pulled image
            code = _run(
                cmd,
                cwd=work.tree(flavor),
                env=envs[flavor],
                log_path=os.path.join(work.logs, "matrix_test_%s.log" % flavor),
                timeout=3600,
                check=False,
            )
            entry[flavor] = {"exit_code": code, "tests": junit_outcomes(report)}
        cmp = compare_outcomes(entry["last"]["tests"], entry["next"]["tests"])
        cmp["match"] = (
            cmp["match"]
            and bool(entry["last"]["tests"])
            and (entry["last"]["exit_code"] == entry["next"]["exit_code"])
        )
        previous[item] = {
            "exit_codes": {f: e["exit_code"] for f, e in entry.items()},
            "compare": cmp,
        }
        _save(work, "matrix_test.json", previous)
        _log(
            "matrix-test %s: match=%s %s"
            % (item, cmp["match"], cmp["last_counts"])
        )
    return all(v["compare"]["match"] for v in previous.values())


# --------------------------------------------------------------------------
# matrix-build
# --------------------------------------------------------------------------


def _image_exists(image):
    return (
        subprocess.call(
            ["docker", "image", "inspect", image],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        == 0
    )


def _runtimes(work, lang, release):
    """Matrix runtimes for lang/release, per the new harness's config."""
    out = subprocess.check_output(
        [
            sys.executable, "-c",
            "import sys; sys.path.insert(0, '.');"
            "from tools.interop import runner;"
            "print(' '.join(runner.get_runtimes_for_lang_release(%r, %r)))" % (lang, release),
        ],
        cwd=work.tree("next"),
        text=True,
    )  # fmt: skip
    return out.split()


def stage_matrix_build(work, args):
    previous = _load(work, "matrix_build.json") or {}
    env = _harness_env(work)
    for item in args.matrix_build_releases:
        if item in previous and not args.force:
            continue
        lang, release = item.split(":", 1)
        runtimes = _runtimes(work, lang, release)
        entry = {"runtimes": runtimes, "build": {}, "cases": {}}
        for flavor in ("last", "next"):
            _require_disk(work, args.min_free_gb)
            prefix = "hotswap-%s" % flavor
            checkout_root = os.path.join(work.root, "matrix_build", flavor)
            _snapshot()
            code = _run(
                [sys.executable] + _HARNESS[flavor]["matrix-build"] + [
                    "--git_checkout", "--git_checkout_root", checkout_root,
                    "--release", release, "--language", lang,
                    "--docker_path", prefix,
                ],
                cwd=work.tree(flavor),
                env=env,
                log_path=os.path.join(work.logs, "matrix_build_%s.log" % flavor),
                timeout=3 * 3600,
                check=False,
            )  # fmt: skip
            entry["build"][flavor] = {"exit_code": code, "images": {}}
            for runtime in runtimes:
                image = "%s/grpc_interop_%s:%s" % (prefix, runtime, release)
                built = _image_exists(image)
                entry["build"][flavor]["images"][runtime] = built
                if not built:
                    continue
                out = work.result(
                    "matrix_build_cases_%s_%s_%s_%s.json"
                    % (lang, runtime, release, flavor)
                )
                if os.path.exists(out):
                    os.remove(out)
                _run(
                    [
                        sys.executable, os.path.abspath(__file__), "_run_matrix_cases",
                        "--next_tree", work.tree("next"), "--lang", lang,
                        "--runtime", runtime, "--release", release,
                        "--image", image, "--out", out,
                    ],
                    cwd=work.tree("next"),
                    log_path=os.path.join(work.logs, "matrix_build_cases.log"),
                    timeout=3600,
                    check=False,
                )  # fmt: skip
                cases = {}
                if os.path.isfile(out):
                    with open(out) as f:
                        cases = json.load(f)
                entry["cases"].setdefault(runtime, {})[flavor] = cases
                subprocess.call(
                    ["docker", "rmi", "-f", image], stdout=subprocess.DEVNULL
                )
            # Also images whose commit finished after a build timed out.
            for runtime in runtimes:
                subprocess.call(
                    [
                        "docker",
                        "rmi",
                        "-f",
                        "%s/grpc_interop_%s:%s" % (prefix, runtime, release),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            _cleanup_run(work, flavor, "", mount_root=checkout_root)
        cmps = {}
        for runtime in runtimes:
            cases = entry["cases"].get(runtime, {})
            cmps[runtime] = compare_outcomes(
                cases.get("last", {}), cases.get("next", {})
            )
        built_same = all(
            entry["build"]["last"]["images"].get(r)
            == entry["build"]["next"]["images"].get(r)
            for r in runtimes
        )
        previous[item] = {
            "build": entry["build"],
            "compare": cmps,
            "match": built_same and all(c["match"] for c in cmps.values()),
        }
        _save(work, "matrix_build.json", previous)
        _log(
            "matrix-build %s: match=%s build=%s"
            % (item, previous[item]["match"], entry["build"])
        )
    return all(v["match"] for v in previous.values())


def run_matrix_cases(args):
    """Internal: runs one release's matrix test cases against a local image."""
    sys.path.insert(0, args.next_tree)
    from tools.interop import runner  # pylint: disable=import-outside-toplevel

    suite = "%s__%s_%s" % (args.lang, args.runtime, args.release)
    specs = runner.generate_test_case_jobspecs(
        args.lang, args.runtime, args.release, suite
    )
    _, resultset = runner.jobset.run(
        specs,
        newline_on_success=True,
        maxjobs=8,
        add_env={"docker_image": args.image},
    )
    outcomes = {name: results[-1].state for name, results in resultset.items()}
    with open(args.out, "w") as f:
        json.dump(outcomes, f, indent=1, sort_keys=True)
    return 0


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------


def _verdict(ok):
    return "MATCH" if ok else "**MISMATCH**"


def stage_report(work, unused_args):
    lines = ["# Hot-swap check report", ""]
    setup = _load(work, "setup.json") or {}
    for flavor, info in sorted(setup.get("refs", {}).items()):
        lines.append(
            "- %s: `%s` @ `%s`" % (flavor, info["ref"], info["sha"][:10])
        )
    if setup.get("siblings"):
        lines.append(
            "- siblings: "
            + ", ".join(
                "grpc-%s@%s" % (k, v[:10])
                for k, v in sorted(setup["siblings"].items())
            )
        )
    ok = True
    labels = sorted(
        f[: -len(".json")]
        for f in os.listdir(work.results)
        if re.match(r"kokoro(_\w+)?\.json$", f)
    )
    for label in labels:
        kokoro = _load(work, label + ".json")
        lines += ["", "## %s: %s" % (label, _verdict(kokoro["match"])), ""]
        lines.append("`%s`" % " ".join(kokoro["args"]))
        if kokoro.get("aborted"):
            lines.append("Aborted: %s." % kokoro["aborted"])
            ok = False
            continue
        t = kokoro["tests"]
        lines += [
            "",
            "| harness | exit | minutes | image builds |",
            "|---|---|---|---|",
        ]
        for flavor in ("last", "next"):
            r = kokoro["runs"][flavor]
            builds = ", ".join(
                "%s=%s" % kv for kv in sorted(r["builds"].items())
            )
            lines.append(
                "| %s | %s | %s | %s |"
                % (flavor, r["exit_code"], r["minutes"], builds)
            )
        lines += [
            "",
            "Tests: %d common, %d same outcome; last %s; next %s."
            % (
                t["common"],
                t["same_outcome"],
                t["last_counts"],
                t["next_counts"],
            ),
            "Expected only-in-last (no pythonasyncio server in next): %d."
            % len(t["only_last_expected"]),
        ]
        for key in ("differ", "only_last", "only_next"):
            if t[key]:
                lines.append(
                    "- %s (%d): %s"
                    % (key, len(t[key]), json.dumps(t[key][:20]))
                )
        ok = ok and kokoro["match"]
    for stage, title in (
        ("matrix_test.json", "matrix-test"),
        ("matrix_build.json", "matrix-build"),
    ):
        data = _load(work, stage)
        if not data:
            continue
        stage_ok = True
        rows = ["| release | result | details |", "|---|---|---|"]
        for item, v in sorted(data.items()):
            if stage == "matrix_test.json":
                c = v["compare"]
                match = c["match"]
                detail = "last %s, next %s, exit %s" % (
                    c["last_counts"],
                    c["next_counts"],
                    v["exit_codes"],
                )
                if c["differ"] or c["only_last"] or c["only_next"]:
                    detail += "; diff %s" % json.dumps(
                        c["differ"][:5]
                        + c["only_last"][:5]
                        + c["only_next"][:5]
                    )
            else:
                match = v["match"]
                detail = "; ".join(
                    "%s: built last=%s next=%s, cases last %s next %s"
                    % (
                        rt,
                        v["build"]["last"]["images"].get(rt),
                        v["build"]["next"]["images"].get(rt),
                        c["last_counts"],
                        c["next_counts"],
                    )
                    for rt, c in sorted(v["compare"].items())
                )
            stage_ok = stage_ok and match
            rows.append("| %s | %s | %s |" % (item, _verdict(match), detail))
        lines += ["", "## %s: %s" % (title, _verdict(stage_ok)), ""] + rows
        ok = ok and stage_ok
    lines += ["", "## Overall: %s" % _verdict(ok), ""]
    path = work.result("REPORT.md")
    with open(path, "w") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))
    return ok


# --------------------------------------------------------------------------


_STAGES = {
    "setup": stage_setup,
    "kokoro": stage_kokoro,
    "matrix-test": stage_matrix_test,
    "matrix-build": stage_matrix_build,
    "report": stage_report,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "stage", choices=list(_STAGES) + ["all", "_run_matrix_cases"]
    )
    parser.add_argument(
        "--work_dir",
        default=os.path.join(os.path.dirname(_REPO_ROOT), "grpc_hot_swap"),
    )
    parser.add_argument("--last_ref", default="master")
    parser.add_argument("--next_ref", default="HEAD")
    parser.add_argument(
        "--kokoro_args", nargs=argparse.REMAINDER, default=_KOKORO_ARGS
    )
    parser.add_argument("--kokoro_timeout", type=int, default=6 * 3600)
    parser.add_argument(
        "--matrix_test_releases", nargs="+", default=_MATRIX_TEST_RELEASES
    )
    parser.add_argument(
        "--matrix_build_releases", nargs="+", default=_MATRIX_BUILD_RELEASES
    )
    parser.add_argument("--min_free_gb", type=int, default=25)
    parser.add_argument(
        "--abort_below_gb",
        type=int,
        default=8,
        help="Kill a running harness if free disk drops below this.",
    )
    parser.add_argument(
        "--reuse_last",
        action="store_true",
        help="kokoro: reuse a finished legacy-harness run of the same commit.",
    )
    parser.add_argument(
        "--split",
        action="store_true",
        help="kokoro: run the scenario as smaller language groups (less disk).",
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=sorted(_KOKORO_GROUPS),
        help="kokoro --split: run only these groups (default: all).",
    )
    parser.add_argument(
        "--force", action="store_true", help="Redo finished matrix items."
    )
    # _run_matrix_cases (internal)
    for flag in (
        "--next_tree",
        "--lang",
        "--runtime",
        "--release",
        "--image",
        "--out",
    ):
        parser.add_argument(flag)
    args = parser.parse_args(argv)
    if args.stage == "_run_matrix_cases":
        return run_matrix_cases(args)
    work = Work(args.work_dir)
    _GUARD.update(path=work.root, min_free_gb=args.abort_below_gb, work=work)
    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)
    stages = (
        ["setup", "kokoro", "matrix-test", "matrix-build", "report"]
        if args.stage == "all"
        else [args.stage]
    )
    ok = True
    for stage in stages:
        _log("=== %s ===" % stage)
        result = _STAGES[stage](work, args)
        if stage not in ("setup",):
            ok = ok and bool(result)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
