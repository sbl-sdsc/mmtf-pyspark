Installation on Windows
-----------------------

Prerequisites
-------------

The following libraries and tools are required to install mmtfPyspark.
Except for Java, you need to choose an installation directory, for
example your home directory ``C:\Users\USER_NAME``. This directory is a
placeholder for a location of your choice.

Install Git
-----------

The Git version control system is used to download repositories from
Github.

`Download
Git <https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe>`__
and run the installer (choose all default options)

Install Spark
-------------

As an example in following steps, ``_YOUR_DIRECTORY_`` could be
``C:\spark``, ``_YOUR_SPARK_VERSION_`` could be
``spark-2.3.2-bin-hadoop2.7``.

NOTE, Spark 2.4.0 does not run on Windows!

If **Anaconda** is installed, skip step 1 and run all of the commands on
**Anaconda prompt**. Else, open your command prompt and follow the
instructions from step 1.

1. Download GOW:

   GOW allows you to use linux commands on windows. In this install, we
   will need curl, gzip, tar which GOW provides.

   `Download
   GOW <https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe>`__

2. | Download Apache Spark 2.3.2
   | Go to the Apache Spark website
     `link <http://spark.apache.org/downloads.html>`__

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

3. Download winutils.exe into
   ``_YOUR_DIRECTORY_\_YOUR_SPARK_VERSION_\bin`` using the following
   command.

   ::

       cd _YOUR_SPARK_VERSION_\bin

       curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe?raw=true

       cd ..

4. Next, set the following environmental variables.

   ::

       setx SPARK_HOME _YOUR_DIRECTORY_\_YOUR_SPARK_VERSION_

       setx HADOOP_HOME _YOUR_DIRECTORY_\_YOUR_SPARK_VERSION_


Create a Conda Environment
~~~~~~~~~~~~~~~~~~~

::

    cd _YOUR_DIRECTORY_

    git clone https://github.com/sbl-sdsc/mmtf-pyspark.git

    cd mmtf-pyspark

    conda env create -f binder/environment.yml


Activate Conda Environment
~~~~~~~~~~~~~~~~~~~~

::

   conda activate mmtf-pyspark


Testing installation
~~~~~~~~~~~~~~~~~~~~

Before testing the installation, close and reopen your Anaconda/Command
prompt.

If the metadata of 1AQ1 is printed, you have successfully intalled
mmtfPyspark.

Launch Jupyter Notebook
~~~~~~~~~~~~~~~~~~~~~~~

::

   jupyter notebook

In Jupyter Notebook, open the file ``DataAnalysisExample.ipynb`` and run it.

More notebooks that demonstarte use the mmtf-pypark API are available in the
demos directory.


OPTIONAL] Hadoop Sequence Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MMTF Hadoop sequence files of all PDB structures can be downloaded and
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

    setx MMTF_REDUCED _YOUR_DIRECTORY_\reduced``
