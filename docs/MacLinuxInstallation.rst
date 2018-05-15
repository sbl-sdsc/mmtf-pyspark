Installation on MacOS and Linux
-------------------------------

Install Java SE Development Toolkit (JDK 1.8)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To check if you have JDK installed, type the following on your terminal:

::

    javac -version

If you do not have JDK, or if you are using any version other than 1.8,
please install JDK 1.8.

`Download JDK
1.8 <http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>`__
and run the installer.

Install Git
~~~~~~~~~~~

The Git version control system is used to download respositories from
Github.

To check if you have git installed, type the following line on your
terminal:

::

    git --version

`Download and install Git <https://git-scm.com/downloads>`__

Install Spark
~~~~~~~~~~~~~

To install and setup Apache Spark 2.3 and Hadoop 2.7, run the following
commands on your terminal:

::

    curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/install_spark.sh -o install_spark.sh
    . ./install_spark.sh

The default installation location is in the user's home directory. The specify another directory, use the -o flag:

::

    curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/install_spark.sh -o install_spark.sh
    . ./install_spark.sh - o /YOUR_DIRECTORY



Install mmtfPyspark
~~~~~~~~~~~~~~~~~~~

If you do not have anaconda installed, all the following ``pip ...``
should be replaced with ``python -m pip ...``

If you have both python 2 and python 3 installed, replace all the
following ``python ...`` with ``python3 ...``

To install mmtfPyspark, make sure you have pip installed:

::

    pip --version

To install pip on MacOS:

::

    sudo easy_install pip

For linux machines, please visit the following website:

`Install pip on
linux <https://packaging.python.org/guides/installing-using-linux-tools/>`__

mmtfPyspark can be installed in the following way: \*
`PyPI <https://pypi.org/project/mmtfPyspark/>`__ install (from the
python packaging index):

``pip install mmtfPyspark``

If there are any errors installing mmtfPyspark, try upgrading pip by:

``pip install --upgrade pip``

By cloning the Github repository using the pip install method, sample
jupyter notebooks and tutorials can be found in the *mmtf-pyspark/demos*
directory.

Testing installation
~~~~~~~~~~~~~~~~~~~~

To test if the installation is successful:

::

    curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/test_mmtfPyspark.py -o test_mmtfPyspark.py

    python test_mmtfPyspark.py

If the metadata of 1AQ1 is printed, you have successfully installed
mmtfPyspark.

[OPTIONAL] Hadoop Sequence Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MMTF Hadoop sequence files of all PDB structures can be downloaded and
environmental variables can be set by running the following command:

::

    . ./download_mmtf_files.sh

The default download location is in the user's home directory. The specify another directory, use the -o flag:

::
    . ./download_mmtf_files.sh -o /YOUR_DIRECTORY

Tips:
~~~~~

Exception: Python in worker has different version than in driver.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Find the path to your python3 by the following command:

::

    which python3

Add the following lines to your ``~/.basrc`` file

::

    export PYSPARK_PYTHON = <path to your python3>
    export PYSPARK_DRIVER_PYTHON = <path to your python3>

[SSL: CERTIFICATE\_VERIFY\_FAILED]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On your Mac, go to ``Applications`` > ``Python 3.6`` and click on
``InstallCertificates.command``

If you are not using the root account
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add the --user flag during pip install:

::

    pip install --user mmtfPyspark

Again, we strongly recommend you to have **Anaconda** installed.
