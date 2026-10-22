# Overview

This directory contains scripts that facilitate building and running gRPC interoperability tests for combinations of language/runtimes (known as matrix).

The setup builds gRPC docker images for each language/runtime and upload it to Artifact Registry (AR). These images, encapsulating gRPC stack
from specific releases/tag, are used to test version compatibility between gRPC release versions.

## Step-by-step instructions for adding an AR docker image for a new release for compatibility test

We have continuous nightly test setup to test gRPC backward compatibility between old clients and latest server.
When a gRPC developer creates a new gRPC release, s/he is also responsible to add the just-released gRPC client to the nightly test.
The steps are:
- Grant permissions:
  - `gcloud auth login` (this will open a browser window; non-browser auth methods are available; use `--help`)
  - `gcloud auth configure-docker us-docker.pkg.dev`
- Add (or update) an entry in `tools/interop/config.json` (`matrix.languages.<lang>.releases`) to reference the github tag for the release.
- Build new client docker image(s).  For example, for C and wrapper languages release `v1.9.9`, do
  - `tools/interop/runner.py matrix-build --git_checkout --release=v1.9.9 --upload_images --language cxx python ruby php`
- Verify that the new docker image was built successfully and uploaded to AR.  For example,
  - `gcloud artifacts docker images list us-docker.pkg.dev/grpc-testing/testing-images-public` lists available images.
  - `gcloud artifacts docker images list us-docker.pkg.dev/grpc-testing/testing-images-public/grpc_interop_java --include-tags` should show an image entry with tag `v1.9.9`.
  - images can also be viewed in https://pantheon.corp.google.com/artifacts/docker/grpc-testing/us/testing-images-public
- Verify the just-created docker client image would pass backward compatibility test (it should).  For example,
  - `tools/interop/runner.py matrix-test --language java --release=v1.9.9`
- Commit the change and create a PR to upstream/master.
- Trigger an adhoc run of interop matrix tests: https://fusion.corp.google.com/projectanalysis/summary/KOKORO/prod:grpc%2Fcore%2Fexperimental%2Flinux%2Fgrpc_interop_matrix_adhoc
- Once tests pass, request a PR review.
- (Optional) clean up the tmp directory to where grpc source is cloned at `/export/hda3/tmp/grpc_matrix/`.
For more details on each step, refer to sections below.

## Instructions for adding new language/runtimes

- Create `tools/dockerfile/interoptest/grpc_interop_<runtime>/Dockerfile` with an `AS base` toolchain stage and an `AS interop` build stage, following existing language/runtimes examples.
- Add language/runtimes to `tools/interop/config.json` (`matrix.languages` and `matrix.custom_entrypoints`) following existing language/runtimes examples.
- Run `tools/interop/runner.py matrix-build` which will build (and upload) images to AR.

## Instructions for running test cases against AR docker images

- Run `tools/interop/runner.py matrix-test`.  Useful options:
  - `--release` specifies a git release tag.  Defaults to `--release=all`.  Make sure the AR images with the tag have been created using `tools/interop/runner.py matrix-build` above.
  - `--language` specifies a language.  Defaults to `--language=all`.
  For example, To test all languages for all gRPC releases across all runtimes, do `tools/interop/runner.py matrix-test --release=all`.
- The output for all the test cases is recorded in a junit style xml file (defaults to 'report.xml').

## Instructions for running test cases against an AR image manually

- Run `tools/interop/runner.py matrix-test` targeting a specific language and release (with `--keep_local_images` if you already pulled or built the image locally):
  - `tools/interop/runner.py matrix-test --language go --release=v1.16.0 --keep_local_images`

Note:
- File paths starting with `tools/` are relative to the grpc repo root dir.
- Creating and referencing images in AR require read and write permission to AR path us-docker.pkg.dev/grpc-testing.
