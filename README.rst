MMTF PySpark
============

|Build Status| |GitHub license| |Version| |Download MMTF| |Download MMTF
Reduced| |Binder| |Twitter URL|

mmtfPyspark is a python package that provides APIs and sample
applications for distributed analysis and scalable mining of 3D
biomacromolecular structures, such as the Protein Data Bank (PDB)
archive. mmtfPyspark uses Big Data technologies to enable
high-performance parallel processing of macromolecular structures.
mmtfPyspark use the following technology stack:

- `Apache Spark <https://spark.apache.org/>`__ a fast and general engine for large-scale distributed data processing.
- `MMTF <https://mmtf.rcsb.org/>`__ the Macromolecular Transmission Format for compact data storage, transmission and high-performance parsing
- `Hadoop Sequence File <https://wiki.apache.org/hadoop/SequenceFile>`__ a Big Data file format for parallel I/O
- `Apache Parquet <https://parquet.apache.org/>`__ a columnar data format to store dataframes

This project is still currently under development.

Installation
------------

Python
~~~~~~

We strongly recommend that you have
`anaconda <https://docs.continuum.io/anaconda/install/>`__ and we
require at least python 3.6 installed. To check your python version:

::

    python --version

If **Anaconda** is installed, and if you have python 3.6, the above
command should return:

::

    Python 3.6.4 :: Anaconda, Inc.

mmtfPyspark and dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since mmtfPyspark uses parallel computing to ensure high-performance, it
requires additional dependencies such as Apache Spark. Therefore, please
read follow the installation instructions for your OS carefully:

`MacOS and LINUX <http://mmtf-pyspark.readthedocs.io/en/latest/MacLinuxInstallation.html>`_

`Windows <http://mmtf-pyspark.readthedocs.io/en/latest/WindowsInstallation.html>`_

Hadoop Sequence Files
---------------------

The MMTF Hadoop sequence files of all PDB structures can be downloaded
by:

::

    curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
    tar -xvf full.tar

    curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
    tar -xvf reduced.tar

For Mac and Linux, the Hadoop sequence files can be downloaded and saved
as environmental variables by running the following command:

::

    curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/download_mmtf_files.sh -o download_mmtf_files.sh
    . ./download_mmtf_files.sh

.. |Build Status| image:: https://travis-ci.org/sbl-sdsc/mmtf-pyspark.svg?branch=master
   :target: https://travis-ci.org/sbl-sdsc/mmtf-pyspark
.. |GitHub license| image:: https://img.shields.io/github/license/sbl-sdsc/mmtf-pyspark.svg
   :target: https://github.com/sbl-sdsc/mmtf-pyspark/blob/master/LICENSE
.. |Version| image:: http://img.shields.io/badge/version-0.2.4-yellowgreen.svg?style=flat
   :target: https://github.com/sbl-sdsc/mmtf-pyspark
.. |Download MMTF| image:: http://img.shields.io/badge/download-MMTF_full-yellow.svg?style=flat
   :target: https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
.. |Download MMTF Reduced| image:: http://img.shields.io/badge/download-MMTF_reduced-orange.svg?style=flat
   :target: https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
.. |Binder| image:: https://mybinder.org/badge.svg
   :target: https://mybinder.org/v2/gh/sbl-sdsc/mmtf-pyspark/master
.. |Twitter URL| image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social
   :target: https://twitter.com/mmtf_spec
