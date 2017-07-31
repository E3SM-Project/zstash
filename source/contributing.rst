**********************************
Contributing to this documentation
**********************************

.. highlight:: none

Create a conda environment
==========================

This documentation is created using 
`Sphinx <http://www.sphinx-doc.org/en/stable>`_. Sphinx is an open-source tool 
that makes it easy to create intelligent and beautiful documentation, written 
by Georg Brandl and licensed under the BSD license.

First, make sure that you have conda for Python 2.7 in your 
path. On NERSC machines, you can load it with: ::

   $ module load python/2.7-anaconda-4.4

Create a new conda environment, install Sphinx as well as the 
Sphinx `readthedocs theme <https://github.com/rtfd/sphinx_rtd_theme>`_.
Use Sphinx version 1.6.2 as errors were encountered with 1.6.3 ::

   $ conda create -n sphinx
   $ source activate sphinx
   $ conda install -c anaconda sphinx=1.6.2
   $ pip install sphinx_rtd_theme


Checkout and edit documentation
===============================

The documentation is maintained in the Github repository under a separate
branch named 'gh-pages'. This special branch is used by Github.com to directly
serve static web pages (see `GitHub Pages <https://pages.github.com/>`_).

Clone the repository and checkout the 'gh-pages' branch: ::

   $ cd <myDir>
   $ git clone git@github.com:ACME-Climate/zstash.git zstash
   $ cd zstash
   $ git checkout gh-pages

You should now see two sub-directories: `source` contains the documentation
source files, and `docs` the html web pages created by Sphinx.

To modify the documentation, simply edit the files under `source`.
Sphinx uses `reStructuredText <http://docutils.sourceforge.net/rst.html>`_ 
as its markup language. For more information on how to write documentation 
using Sphinx, you can refer to

* `First Steps with Sphinx <http://www.sphinx-doc.org/en/stable/tutorial.html>`_
* `reStructuredText Primer <http://www.sphinx-doc.org/en/stable/rest.html#external-links>`_

To see the changes you made to the documentation, rebuild the web pages ::

   $ cd <myDir>/zstash
   $ make html
 
and view them locally in a web browser at `file:///<myDir>/zstash/index.html`.

Sphinx may occasionally not build the new files properly. If that is the case,
try removing the `docs` sub-directory (be careful not to remove `source`)
and rebuild entirely: ::

   $ cd <myDir>/zstash
   $ rm -r docs
   $ make html
 

Once you are satisfied with your modifications, commit and push them back to 
the repository: ::

   $ cd <myDir>/zstash
   $ git add .
   $ git commit
   $ git push origin gh-pages
   
Your changes will then be available on the 
`zstash documentation page <https://acme-climate.github.io/zstash/>`_.

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

