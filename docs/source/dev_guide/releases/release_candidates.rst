.. _release_candidates:

Preparing a release candidate
=============================

Step 1: Testing
---------------

Be sure to run the entire integration test suite before making a release candidate.

Step 2: Determine what the new version should be
-------------------------------------------------

This step only needs to be done for rc1.

Review `zstash commits <https://github.com/E3SM-Project/zstash/commits/main/>`_. Identify what changes since the last production release would require a patch, minor, or major version update (as in ``vMAJOR.MINOR.PATCH``).

The highest-level version update will be used. For example, if you found 1 commit requiring a patch update, 1 commit requiring a minor update, and 0 commits requiring a major update, then you'd want to increment the minor version.

Step 3: Confluence
------------------

This step only needs to be done for rc1.

Update the `E3SM Unified version tracking page <https://e3sm.atlassian.net/wiki/spaces/DOC/pages/129732419/Packages+in+the+E3SM+Unified+conda+environment>`_ with the new ``zstash`` version number. Be sure to update the section for the next E3SM Unified environment, not the current one.

Step 4: tbump
-------------

In this example, ``v1.2.3rc4`` is used -- so, the major version is 1, the minor version is 2, and the patch version is 3. The release candidate number is 4.

   .. code-block:: bash

        cd zstash
        git status # Make sure you don't have any uncommitted changes
        git fetch upstream main # This assumes you've named your remote for the main repo as "upstream"
        git checkout -b v1.2.3rc4 upstream/main
        conda activate env-name # Activate any zstash dev environment you have; we just need `tbump`
        tbump 1.2.3rc4 --no-tag
        # This creates a commit, but won't push it (because the branch isn't named `main`)
        git diff HEAD^ HEAD | cat
        # Review the change `tbump` added
        git push upstream v1.2.3rc4
        # Create, and "Update version" label" to, and merge the PR; delete the branch on GitHub

Step 5: Tag the RC on the zstash repo
--------------------------------------

   .. code-block:: bash

        git checkout main
        git fetch upstream
        git reset --hard upstream/main
        git tag -a v1.2.3rc4 -m "v1.2.3rc4" # Add the tag for this RC
        # Delete the branch from the tbump step. Otherwise, the push command won't work.
        git branch -D v1.2.3rc4
        git push upstream v1.2.3rc4 # Push the new RC tag to GitHub

``v1.2.3rc4`` should now appear on `Tags <https://github.com/E3SM-Project/zstash/tags>`__, but _not_ on `Releases <https://github.com/E3SM-Project/zstash/releases>`_.

Step 6: zstash-feedstock repo
------------------------------

If you don't have a fork of zstash-feedstock, first go to the `zstash-feedstock repo <https://github.com/conda-forge/zstash-feedstock>`__, and click the "Fork" button in the top right.

   .. code-block:: bash

        # If you don't have a clone of zstash-feedstock, first run:
        # Clone using the SSH from the green "Code" button in the top right of the repo home page.
        git clone git@github.com:conda-forge/zstash-feedstock.git
        cd zstash-feedstock
        git remote -v # See your remotes
        # Optional; this lets you use upstream to refer to the main repo:
        git remote add upstream git@github.com:conda-forge/zstash-feedstock.git
        # Required; this allows you to push to your fork:
        # Copy the SSH path from your fork's green "Code" button and run:
        git remote add your-fork-name git@github.com:your-fork-name/zstash-feedstock.git

        # If you already have your clone set up, just run:
        cd zstash-feedstock

        # In all cases:
        curl -sL https://github.com/E3SM-Project/zstash/archive/v1.2.3rc4.tar.gz | openssl sha256
        # SHA2-256(stdin)= long hex string
        git status # Check for uncommitted changes
        git fetch upstream dev # Make sure you fetch the dev branch, not the main branch!
        git checkout -b v1.2.3rc4 upstream/dev
        emacs recipe/meta.yaml
        # Update the version and sha256 (and the build number if needed):
        # {% set version = "1.2.3rc4" %}
        # sha256: ... # The sha256 from a few commands earlier
        # number: 0 # build >>> number should always be 0

        # Check the diff since the last release/RC. Examples:
        # - https://github.com/E3SM-Project/zstash/compare/v1.2.2...v1.2.3rc1
        # - https://github.com/E3SM-Project/zstash/compare/v1.2.3rc3...v1.2.3rc4
        # If there are changes in dependencies there (e.g., in `conda/dev.yml`),
        # you'll want to include them in this feedstock PR too.

        git add -A
        git commit -m "v1.2.3rc4"
        git push your-fork-name v1.2.3rc4

Then, create a pull request to the ``dev`` branch. Do _not_ set the pull request to merge to ``main``, as that will create a production release! RC packages get the ``zstash_dev`` label.

Follow any further directions given by the ``conda-forge/zstash-feedstock`` PR template. Once that PR is reviewed and merged and CI completes, check https://anaconda.org/conda-forge/zstash for the new package (allow ~15 minutes for mirroring).
