********
Database
********

.. highlight:: none

.. warning::

   Directly interacting with the database is not recommened
   unless you understand the consequences of what you are doing!

Structure
=========

Metadata from zstash archives is stored in a sqlite3 database
in `zstash/index.db`. The database schema consists of
two tables, one to store configuration parameters (``config``) 
and the other one for metadata for each archived file (``files``). ::

    $ sqlite3 zstash/index.db
    SQLite version 3.7.17 2013-05-20 00:56:22
    Enter ".help" for instructions
    Enter SQL statements terminated with a ";"

    sqlite> .schema
    CREATE TABLE config (
      arg text primary key,
      value text
    );
    CREATE TABLE files (
      id integer primary key,
      name text,
      size integer,
      mtime timestamp,
      md5 text,
      tar text,
      offset integer
    );

    sqlite> .quit


For each file, **metadata** consists of

* **name**: relative path and file name
* **size**: file size in bytes
* **mtime**: modification time
* **md5**: file md5 checksum
* **tar**: tar file in which file is archived (e.g. 00000a.tar)
* **offset**: offset in bytes where the file is located within its tar file.
 
Exploring content
=================

Direct interaction with the database can be useful to explore content
of an archive, beyond what might be available with :ref:`zstash list<zstash-list>`.

To list **all the files** in an archive: ::

   $ cd <mydir>
   $ sqlite3 zstash/index.db "select * from files;"

For each file, the following information will be printed ::

   file # | path | size | modification time |md5 checksum |tar archive | offset (within tar)

To list **files matching a specified pattern** (for example \*/run/\*.nc): ::

   $ sqlite3 zstash/index.db "select * from files where name glob '*/run/*.nc';"

To list **all the files in a specific tar fole** (for example 00000a.tar): ::

   $ sqlite3 zstash/index.db "select * from files where tar is '00000a.tar';"

