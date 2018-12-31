Installation on Windows
-----------------------

Prerequisites
-------------

The following libraries and tools are required to install mmtfPyspark.
Choose an installation directory, for example your home directory ``C:\Users\USER_NAME``. This directory is a placeholder for a location of your choice.

Install Anaconda
~~~~~~~~~~~

`Download the Python 3.7 Anaconda installer <https://www.anaconda.com/download>`__ and install Anaconda.


Install Git
~~~~~~~~~~~

The Git version control system is used to download repositories from
Github.

`Download
Git <https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe>`__
and run the installer (choose all default options)


Install Gow
~~~~~~~~~~~

Gow installs Linux command line tools on Windows. For this install, we
will use the curl, gzip, mkdir, mv, and tar tools.

   `Download
   GOW <https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe>`__


Install Apache Spark
-------------

As an example in following steps, ``_YOUR_DIRECTORY_`` could be
``C:\spark``, ``_YOUR_SPARK_VERSION_`` could be
``spark-2.3.2-bin-hadoop2.7``.

NOTE, Spark 2.4.0 does not run on Windows due to a bug!

Launch the Anaconda Prompt command window from the Start Menu and follow the instructions.

1. | Download Apache Spark 2.3.2
   | Go to the Apache Spark website
     `link <https://spark.apache.org/downloads.html>`__

   a) Choose Spark version 2.3.2

   b) Choose a package type: Pre-build for Apache Hadoop 2.7 and later

   c) Click on the Download Spark link

   d) Unzip the file in your directory:

   ::

       mkdir _YOUR_DIRECTORY_

       mv _YOUR_SPARK_VERSION_.tgz _YOUR_DIRECTORY_

       cd _YOUR_DIRECTORY_

       gzip -d _YOUR_SPARK_VERSION_.tgz

       tar xvf _YOUR_SPARK_VERSION_.tar

2. Download winutils.exe into
   ``_YOUR_DIRECTORY_\_YOUR_SPARK_VERSION_\bin`

   ::

       cd _YOUR_SPARK_VERSION_\bin

       curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe?raw=true

       cd ..

4. Next, set the following environmental variables.

   ::

       setx SPARK_HOME _YOUR_DIRECTORY_\_YOUR_SPARK_VERSION_

       setx HADOOP_HOME _YOUR_DIRECTORY_\_YOUR_SPARK_VERSION_

    
Check the Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Close and reopen the Anaconda Prompt to update the environment variables.** Type the following commands to check the environment variables. 

::

       echo %SPARK_HOME%

       echo %HADOOP_HOME%


Install mmtf-pyspark
--------------------

Create a Conda Environment for mmtf-pyspark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A `conda environment <https://conda.io/docs/user-guide/concepts.html>`__ is a directory that contains a specific collection of conda packages that you have installed. If you change one environment, your other environments are not affected. You can easily activate or deactivate environments, which is how you switch between them.

::

    cd _YOUR_DIRECTORY_

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

Notebooks that demonstrate the use of the  mmtf-pypark API are available in the demos directory.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The entire PDB can be downloaded as an MMTF Hadoop sequence file and
environmental variables can be set by running the following command:

::

    cd _YOUR_DIRECTORY_

    curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
    tar -xvf full.tar

    curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
    tar -xvf reduced.tar

Set environmental variables:

::

    setx MMTF_FULL _YOUR_DIRECTORY_\full

    setx MMTF_REDUCED _YOUR_DIRECTORY_\reduced

**Close and reopen the Anaconda Prompt to update the environment variables.** 
