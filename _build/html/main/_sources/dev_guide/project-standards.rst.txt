Project Standards
=================

Development Environment
-----------------------
If you haven't already, please visit :ref:`"(b) Development Environment" <dev-env>`
to set up your local development environment.

Version Control (VC)
--------------------

The repository uses a fork-based Git workflow with tag releases.

.. figure:: git-flow.svg
   :alt: Git Flow Diagram

Guidelines for VC
~~~~~~~~~~~~~~~~~
1. ``main`` must always be **deployable**
2. All changes are made through **support** branches on forks
3. **Rebase** with ``main`` to avoid/resolve conflicts
4. Make sure ``pre-commit`` checks pass when committing (enforced in CI/CD build)
5. Open a pull-request (PR) early for discussion
6. Once the CI/CD build passes and PR is approved, **squash and rebase** your
   commits
7. Merge PR into ``main`` and **delete the branch**

Things to Avoid in VC
~~~~~~~~~~~~~~~~~~~~~
1. Don't merge in broken or commented out code
2. Don't commit onto ``main`` directly
3. Don't merge with conflicts (handle conflicts upon rebasing)

Source: https://gist.github.com/jbenet/ee6c9ac48068889b0912

Pre-commit
~~~~~~~~~~
The repository uses the ``pre-commit`` package to manage pre-commit hooks.
These hooks help enforce quality assurance standards and identify simple issues
at the commit level before submitting code reviews.

.. figure:: pre-commit-flow.svg
   :alt: Pre-commit Flow Diagram

   ``pre-commit`` Flow

Helpful ``pre-commit`` Commands
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install into your cloned repo ::

    conda activate zstash_dev
    pre-commit install

Automatically run all pre-commit hooks (just commit) ::

    # Tip: If there is an issue with pre-commit, you can bypass with the `--no-verify` flag. Please do NOT use this on a regular basis.
    git commit -m '...'

.. figure:: ../_static/pre-commit-passing.png
   :alt: pre-commit Output

   ``pre-commit`` Output

Manually run all pre-commit hooks ::

    pre-commit run --all-files

Run individual hook ::

    # Available hook ids: trailing-whitespace, end-of-file-fixer, check-yaml, black, isort, flake8, mypy
    pre-commit run <hook_id>

Squash and Rebase Commits
~~~~~~~~~~~~~~~~~~~~~~~~~

Before you merge a support branch back into ``main``, the branch is typically
squashed down to a single* buildable commit, and then rebased on top of the main repo's ``main`` branch.

\* *In some cases, it might be logical to have multiple squashed commits, as long as each commit passes the CI/CD build*

Why squash and rebase commits?

- Ensures build passes from the commit
- Cleans up Git history for easy navigation
- Makes collaboration and review process more efficient
- Makes handling conflicts from rebasing simple since there are fewer commits to fix conflicts on
- Makes ``git bisect`` easier and more effective to use. For example, it will show the exact commit that introduced a bug since the commit contains a relatively small changeset. On the otherhand, a merge commit bringing in many commits makes it much harder since it includes a large changeset. A changeset is a collection of commits that are logically grouped together.


How to squash and rebase commits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming that you followed :ref:`"(b) Development Environment" <dev-env>`:

1. Sync ``main`` with the main repo's ``main`` ::

    git checkout main
    git rebase <upstream-origin>/main
    git push -f <fork-origin> main

2. Get the SHA of the commit OR number of commits to rebase to ::

    git log --graph --decorate --pretty=oneline --abbrev-commit

3. Squash commits::

    git rebase -i [SHA]

    # OR

    git rebase -i HEAD~[NUMBER OF COMMITS]

4. Make sure your squashed commit messages are refined

5. Rebase branch onto ``main`` ::

    git checkout <branch-name>
    git rebase main
    git push -f <fork-origin> <branch-name>

6. Force push to remote branch ::

    # You have to force push because the rebase rewrites the commit SHAs
    git push -f <fork-origin> <branch-name>

Source:
https://blog.carbonfive.com/always-squash-and-rebase-your-git-commits/

Code Quality Assurance
----------------------

This project uses several tools for code formatting, linting, and type checking listed below.

- Code Formatting: `black <https://black.readthedocs.io/en/stable/>`__
- Linting: `flake8 <https://github.com/PyCQA/flake8#flake8>`__, `isort <https://pycqa.github.io/isort/>`__
- Optional Type Checking: `mypy <http://mypy-lang.org/>`__

You can run them as hooks manually/automatically when committing using ``pre-commit``, or manually through the terminal or IDE/text editor.

Helpful Commands
~~~~~~~~~~~~~~~~

Run a tool
    ::

       # Available tool names: black, flake8, isort, mypy
       <tool_name> .

.. _ci-cd:

Continuous Integration / Continuous Delivery (CI/CD)
----------------------------------------------------

This project uses `GitHub Actions <https://github.com/E3SM-Project/zstash/actions>`_ to run two CI/CD workflows.

1. CI/CD Build Workflow

  This workflow is triggered by Git ``pull_request`` and ``push`` (merging PRs) events to the the main repo's ``main``.

  Jobs:

    1. Run ``pre-commit`` for formatting, linting, and type checking
    2. Run test suite in a conda environment
    3. Publish latest `main` docs (depends on jobs 1 and 2)

2. CI/CD Release Workflow

  This workflow is triggered by the Git ``publish`` event, which occurs when a new release is tagged.

  Jobs:

    1. Publish new release docs
    2. Publish Anaconda package

Style Guide
-----------

This project follows the Black code style. Please read about it more `here <https://black.readthedocs.io/en/stable/the_black_code_style.html>`__.

API Documentation
-----------------

In most cases, code should be self-documenting.

If necessary, documentation should explain **why** something is done, its purpose, and its goal.
The code shows **how** it is done, so commenting on this can be redundant.

Guidelines For Documenting
~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Embrace documentation as an integral part of the overall
   development process
-  Treat documentation as code and follow principles such as *Don't
   Repeat Yourself* and *Easier to Change*
-  Use comments and docstrings to explain ambigiuity, complexity,
   or to avoid confusion
-  Co-locate API documentation with related code
-  Use Python type annotations and type comments where helpful

Things to Avoid with Documenting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Don't write comments as a crutch for poor code
-  Don't comment *every* function, data structure, type declaration
