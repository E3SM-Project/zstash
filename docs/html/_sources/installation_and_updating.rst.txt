*************************
Installation and Updating
*************************

.. highlight:: none

Installation using conda
========================

First, make sure that you're using ``bash``. ::

   $ bash

You must have Anaconda installed as well. Create a new Anaconda environment 
with zstash installed and activate it. ::

   $ conda create -n zstash_env -c e3sm -c conda-forge zstash
   $ source activate zstash_env

Or you can install zstash in an existing environment. ::

   $ conda install zstash -c e3sm -c conda-forge 

Installation on NERSC
=====================

First, make sure that you're using ``bash``. ::

   $ bash

On NERSC Edison and Cori machines, load the Anaconda module instead of 
installing it yourself. :: 

   $ module load python/2.7-anaconda-4.4

Create a new Anaconda environment with zstash installed and activate it. ::

   $ conda create -n zstash_env -c e3sm -c conda-forge zstash
   $ source activate zstash_env

Or you can install zstash in an existing environment. ::

   $ conda install zstash -c e3sm -c conda-forge 

After installing on Edison or Cori, you may see improved performance 
running **zstash on the data transfer nodes** (dtn{01..15}.nersc.gov). However, modules are
not directly available there, so you will need to manually activate Anaconda: ::

   $ . /global/common/edison/software/python/2.7-anaconda-4.4/etc/profile.d/conda.sh
   $ export PATH="/global/common/edison/software/python/2.7-anaconda-4.4/bin:$PATH"


Installation from source
========================

If you want to get the latest code of zstash from the master branch, do the following.

First, follow the instructions in the previous section ("Installation") to create an
Anaconda environment with zstash.
Make sure you're in the zstash environment before executing the below instructions.

Then, use the command below to remove just zstash, keeping all of the dependencies
in the environment.
We'll be manually installing the latest zstash from master soon. ::

   $ conda remove zstash --force

Clone the zstash repository. ::

   $ git clone https://github.com/E3SM-Project/zstash.git

Install the latest zstash. ::

   $ cd zstash/
   $ python setup.py install


Updating
========

To update zstash, if you **installed via Anaconda**, do the following:  ::

    conda update zstash -c e3sm -c conda-forge

Otherwise, if you've installed from source, checkout the
tag of the version you want and install from that.
