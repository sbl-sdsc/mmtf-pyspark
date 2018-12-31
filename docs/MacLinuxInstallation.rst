Installation on MacOS and Linux
-------------------------------

Install Anaconda
~~~~~~~~~~~

`Download the Python 3.7 Anaconda installer <https://www.anaconda.com/download>`__ and install Anaconda.


Install Git
~~~~~~~~~~~

The Git version control system is used to download respositories from
Github.

To check if you have git installed, type the following line on your
terminal:

::

    git --version

`Download and install Git <https://git-scm.com/downloads>`__


Create a Conda Environment for mmtf-pyspark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A `conda environment <https://conda.io/docs/user-guide/concepts.html>`__ is a directory that contains a specific collection of conda packages that you have installed. If you change one environment, your other environments are not affected. You can easily activate or deactivate environments, which is how you switch between them.

::

    git clone https://github.com/sbl-sdsc/mmtf-pyspark.git

    cd mmtf-pyspark

    conda env create -f binder/environment.yml


Activate the Conda Environment
~~~~~~~~~~~~~~~~~~~~

::

   conda activate mmtf-pyspark


Test the Installation
~~~~~~~~~~~~~~~~~~~~

::

   python test_mmtfPyspark.py


If the metadata for 1AQ1 are printed, you have successfully installed
mmtf-pyspark.

Launch Jupyter Notebook
~~~~~~~~~~~~~~~~~~~~~~~

::

   jupyter notebook

In Jupyter Notebook, open ``DataAnalysisExample.ipynb`` and run it.

Notebooks that demonstrate the use of the  mmtf-pypark API are available in the ``demos`` directory.

Deactivate the Conda Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

   conda deactivate

Actvate the environment again if you want to use mmtf-pyspark.


Remove the Conda Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To permanently remove the environment type:

::

    conda remove -n mmtf-pyspark --all


Download Hadoop Sequence Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`


The entire PDB can be downloaded as an MMTF Hadoop sequence file and
environmental variables can be set by running the following command

::

    curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/download_mmtf_files.sh -o download_mmtf_files.sh
    . ./download_mmtf_files.sh

The default download location is in the user's home directory. The specify another directory, use the -o flag:

::

    curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/download_mmtf_files.sh -o download_mmtf_files.sh
    . ./download_mmtf_files.sh -o {YOUR_LOCAL_DIRECTORY}
