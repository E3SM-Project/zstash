**********************************
Contributing to This Documentation
**********************************

.. highlight:: none

Getting Started
==========================

This documentation is created using 
`Sphinx <http://www.sphinx-doc.org/en/stable>`_. Sphinx is an open-source tool 
that makes it easy to create intelligent and beautiful documentation, written 
by Georg Brandl and licensed under the BSD license.

The documentation is maintained in the ``master`` branch of the GitHub repository.
You can include code and its corresponding documentation updates in a single pull request (PR).

After merging a PR, GitHub Actions automates the documentation building process.
It pushes the HTML build to the ``gh-pages`` branch, which is hosted on GitHub Pages.

Edit Documentation
===============================

Sphinx uses `reStructuredText <http://docutils.sourceforge.net/rst.html>`_
as its markup language. For more information on how to write documentation
using Sphinx, you can refer to

* `First Steps with Sphinx <http://www.sphinx-doc.org/en/stable/tutorial.html>`_
* `reStructuredText Primer <http://www.sphinx-doc.org/en/stable/rest.html#external-links>`_

1. Make sure that you have conda in your path. On NERSC machines, you can load it with: ::

   $ module load python/3.7-anaconda-2019.10

2. Clone the repository and checkout a branch from `master`: ::

   $ cd <myDir>
   $ git clone https://github.com/<your-github-username>/zstash.git
   $ cd zstash
   $ git checkout -b <branch-name> master

3. Create and activate the conda development environment ::

   $ cd <myDir>/zstash
   $ conda env create -f conda/dev.yml
   $ conda activate zstash_env_dev

4. To modify the documentation, simply edit the files under ``/docs/source``.

5. To see the changes you made to the documentation, rebuild the web pages ::

   $ cd <myDir>/zstash/docs
   $ make html
 
6. View them locally in a web browser at ``file:///<myDir>/zstash/docs/_build/html/index.html``.
 - Sometimes the browser caches Sphinx docs, so you might need to delete your cache to view changes.

7. Once you are satisfied with your modifications, commit and push them back to the repository: ::

    $ cd <myDir>/zstash
    $ # `/docs/_build` is ignored by git since it does not need to be pushed
    $ git add .
    $ git commit
    $ git push origin <branch-name>

8. <`OPTIONAL`> If you want to generate and view versioned docs: ::

    $ # After commiting to your branch
    $ cd <myDir>/zstash/docs
    $ sphinx-multiversion source _build/html
    $ # Check the `_build/html` folder for all generated versioned docs
    $ # Open `_build/html/<your-branch>/index.html` to view in browser

   .. figure:: _static/docs-version-selector.png
      :alt: Docs version selector

      Docs version selector dropdown in the bottom left-hand corner

9. Create a pull request from ``your-fork/zstash/branch-name`` to ``E3SM-Project/zstash/master``.

Once this pull request is merged and GitHub Actions finishes building the docs, changes will be available on the
`zstash documentation page <https://e3sm-project.github.io/zstash/>`_.

How Documentation is Versioned
----------------------------
The `sphinx-multiversion <https://github.com/Holzhaus/sphinx-multiversion>`_ package manages documentation versioning.

``sphinx-multiversion`` is configured to generate versioned docs for available tags and branches on local, ``origin`` and ``upstream``.

Branches or tags that don’t contain both the sphinx ``source`` directory and the ``conf.py`` file will be skipped automatically.

    - Skipped versions include releases ``<= v1.0.1`` since the documention source was not included in those tagged releases.
    - Run ``sphinx-multiversion source _build/html --dump-metadata`` to see which tags/branches matched.

Initial setup (for reference only)
==================================

The instructions below only apply for the initial configuration of the
Sphinx documentation on the Github repository. They are documented here
for reference only. Do not follow them unless you are setting up documentation
for a new repository. (Adapted from `Sphinx documentation on GitHub 
<http://datadesk.latimes.com/posts/2012/01/sphinx-on-github>`_.)

Create Sphinx conda environment (see above).

Create a new git branch (gh-pages): ::

  $ git branch gh-pages
  $ git checkout gh-pages

Clear out any­thing from the master branch and start fresh ::

  $ git symbolic-ref HEAD refs/heads/gh-pages
  $ rm .git/index
  $ git clean -fdx

Create documentation ::

  $ sphinx-quickstart

accept suggested default options, except ::

  Separate source and build directories (y/N) [n]: y

Edit Makefile and change BUILDIR ::

  BUILDDIR = docs

Remove old build directory ::

  $ rmdir build

Change the Sphinx theme to 'ReadTheDocs'. Edit 'source/conf.py and change ::

  html_theme = 'alabaster'

to ::

  import sphinx_rtd_theme
  html_theme = "sphinx_rtd_theme"
  html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

Try building documentation ::

  $ make html

Create an empty .nojekyll file to indicate to Github.com that this
is not a Jekyll static website: ::

  $ touch .nojekyll

Create a top-level re-direction file: ::

  $ vi index.html

with the following: ::

  <meta http-equiv="refresh" content="0; url=./docs/html/index.html" />

Commit and push back to Github: ::

  $ git add .
  $ git commit
  $ git push origin gh-pages

