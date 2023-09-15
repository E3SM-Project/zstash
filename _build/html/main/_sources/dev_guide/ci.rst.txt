Understanding CI
========================

In this guide, we'll cover:

* Build Workflow
* Release Workflow
* GitHub Actions

Build Workflow
--------------

The build workflow is at https://github.com/E3SM-Project/zstash/blob/main/.github/workflows/build_workflow.yml. See comments in the file for in-depth explanations of each step.

Release Workflow
----------------

The release workflow is at https://github.com/E3SM-Project/zstash/blob/main/.github/workflows/release_workflow.yml. See comments in the file for in-depth explanations of each step.

GitHub Actions
--------------
Both of these workflows are run by GitHub Actions. See https://github.com/E3SM-Project/zstash/actions.

When a pull request is made, the build workflow is run automatically on the pushed branch. When the pull request is merged, the build workflow is once again run, but this time on the ``main`` branch.
