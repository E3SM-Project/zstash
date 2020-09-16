.. _getting-started:

***************
Getting started
***************

.. highlight:: none


Using E3SM unified environment
==============================

For E3SM users, the simplest way to use zstash is to load the E3SM unified
environment. On NERSC machines, for bash shell: ::

   $ source /global/cfs/cdirs/e3sm/software/anaconda_envs/load_latest_e3sm_unified.sh

or for csh ::

   $ source /global/cfs/cdirs/e3sm/software/anaconda_envs/load_latest_e3sm_unified.csh

For more details on the E3SM unified environment, please refer to the `E3SM Diagnostics and Analysis Quickstart <https://acme-climate.atlassian.net/wiki/spaces/EIDMG/pages/780271950/Diagnostics+and+Analysis+Quickstart>`_.  

For archiving E3SM simulations, we recommend following the :ref:`Best practices for E3SM`.


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

   $ module load python/3.7-anaconda-2019.10
   $ source /global/common/cori_cle7/software/python/3.7-anaconda-2019.10/etc/profile.d/conda.sh

Create a new Anaconda environment with zstash installed and activate it. ::

   $ conda create -n zstash_env -c e3sm -c conda-forge zstash
   $ conda activate zstash_env

Or you can install zstash in an existing environment. ::

   $ conda install zstash -c e3sm -c conda-forge 

After installing on Cori, you may see improved performance 
running **zstash on the data transfer nodes** (dtn{01..15}.nersc.gov). However, modules are
not directly available there, so you will need to manually activate Anaconda: ::

   $ bash
   $ source /global/common/cori_cle7/software/python/3.7-anaconda-2019.10/etc/profile.d/conda.sh
   $ conda activate zstash_env


Installation on compy
=====================

First, make sure that you're using ``bash``. ::

   $ bash

On compy, load the Anaconda module instead of 
installing it yourself. :: 

   $ module load anaconda3/2019.03

You will be prompted to ::

   $ source /share/apps/anaconda3/2019.03/etc/profile.d/conda.sh

Create a new Anaconda environment with zstash installed and activate it. ::

   $ conda create -n zstash_env -c e3sm -c conda-forge zstash
   $ conda activate zstash_env

Or you can install zstash in an existing environment. ::

   $ conda install zstash -c e3sm -c conda-forge 


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
