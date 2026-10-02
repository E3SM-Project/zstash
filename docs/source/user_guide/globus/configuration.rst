##################
Configuration File
##################

zstash may create and read a configuration file at ``~/.zstash.ini`` when you
use Globus-backed archives.

What ``.zstash.ini`` stores
===========================

The main setting currently used by zstash is the local Globus endpoint UUID.
The file uses an INI format such as:

.. code-block:: ini

   [local]
   globus_endpoint_uuid = 6bdc7956-fc0f-4ad2-989c-7aa5ee643a79 # NERSC_PERLMUTTER_ENDPOINT

If the file does not exist, zstash creates it the first time it needs Globus
configuration.

When you may need to edit it
============================

Most users do not need to edit ``~/.zstash.ini``. Manual updates are useful
when:

* zstash cannot infer the local endpoint UUID from the machine hostname
* you want to override the default local endpoint for a machine
* the machine's Globus collection changed and the old UUID is still recorded
* you are testing on a new system before hostname-based auto-detection has been
  added to zstash

If the UUID is wrong or blank, Globus transfers may fail before they start.

Finding the right UUID
======================

Use the UUID for the local collection that should serve as the source or
destination of the transfer on the machine where you are running ``zstash``.
You can usually find that UUID from the Globus web interface for the
collection.

After updating ``~/.zstash.ini``, rerun the zstash command that needs Globus.

Related state files
===================

Two other files can affect Globus behavior:

* ``~/.zstash_globus_tokens.json`` stores refresh tokens from previous Globus
  logins
* ``~/.globus-native-apps.cfg`` may be left over from older Globus-based
  workflows

If zstash reports that a stored refresh token is invalid, deleting
``~/.zstash_globus_tokens.json`` is the usual way to force a fresh login.
