###################
Globus Introduction
###################

This page is intended for users who want to run ``zstash`` on a machine that
does not have direct HPSS access, or who prefer to move zstash archives through
the `Globus <https://www.globus.org/>`_ transfer service.

When to use Globus
==================

Use a Globus destination when ``--hpss`` is set to a URL of the form::

   globus://<endpoint>/<path>

Examples include:

* ``globus://nersc/~/my_archive``
* ``globus://alcf/~/my_archive``
* ``globus://9cd89cfd-6d04-11e5-ba46-22000b92c6ec/~/my_archive`` (NERSC HPSS Globus endpoint)

The names ``nersc`` and ``alcf`` are built-in shortcuts for the NERSC HPSS and
ALCF HPSS Globus endpoints.

First-time setup
================

For a first Globus-based archive, the safest approach is:

1. Identify the local Globus collection for the machine where you will run
   ``zstash``.
2. Identify the destination collection and destination path.
3. Activate both collections in the Globus web interface before you run
   ``zstash``.
4. Start with a small archive so you can confirm that authentication, endpoint
   activation, and path selection are correct.

If you are creating a new archive, a minimal first test looks like

  .. code-block:: bash

      zstash create --hpss=globus://nersc/~/test/my_archive .

After the transfer completes, verify it with ``zstash check`` or retrieve a
small file with ``zstash extract``.

Authentication flow
===================

The first time zstash needs Globus credentials, it will print an authorization
URL and ask you to paste back the returned code. After a successful login,
zstash stores refresh-token state in ``~/.zstash_globus_tokens.json`` so future
Globus transfers between the same machines usually do not need another interactive login.

zstash also checks ``~/.zstash.ini`` for the local endpoint UUID. See
:doc:`configuration` for details on when that file needs to be created or
edited manually.

Choosing endpoint paths
=======================

The destination portion of ``globus://<endpoint>/<path>`` should name the
remote directory that will hold the zstash archive contents:

* ``index.db``
* one or more tar files such as ``000000.tar``, ``000001.tar``, and so on

As with HPSS paths, use a destination directory that is dedicated to one
zstash archive.

Operational notes
=================

* ``zstash create`` and ``zstash update`` create tar files locally first, then
  transfer them through Globus.
* ``zstash check``, ``zstash extract``, and ``zstash ls`` still rely on the
  archive's ``index.db`` to locate files and tars.
* ``--non-blocking`` applies only to Globus transfers. Use it when you want
  zstash to continue building later tar files before the current transfer has
  finished.

Troubleshooting
===============

If a Globus workflow fails unexpectedly:

* Re-activate the source and destination collections in the Globus web
  interface.
* Confirm that ``~/.zstash.ini`` points to the correct local endpoint UUID.
* If zstash reports token problems after you switch endpoints or machines,
  remove ``~/.zstash_globus_tokens.json`` and retry so zstash can request a new
  login.
* If the destination path does not exist, create it first and rerun the
  command.
