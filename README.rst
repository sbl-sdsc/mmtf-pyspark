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

This project is under development.

Documentation
-------------

`Documentation <http://mmtf-pyspark.readthedocs.io/en/latest/>`_

`In Depth Tutorial <https://github.com/sbl-sdsc/mmtf-workshop-2018/>`_

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

This project uses the PDB archive in the form of MMTF Hadoop Sequence File. The files can be downloaded
by:

::

    curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
    tar -xvf full.tar

    curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
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
.. |Version| image:: http://img.shields.io/badge/version-0.3.0-yellowgreen.svg?style=flat
   :target: https://github.com/sbl-sdsc/mmtf-pyspark
.. |Download MMTF| image:: http://img.shields.io/badge/download-MMTF_full-yellow.svg?style=flat
   :target: https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
.. |Download MMTF Reduced| image:: http://img.shields.io/badge/download-MMTF_reduced-orange.svg?style=flat
   :target: https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
.. |Binder| image:: https://mybinder.org/badge.svg
   :target: https://mybinder.org/v2/gh/sbl-sdsc/mmtf-pyspark/master
.. |Twitter URL| image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social
   :target: https://twitter.com/mmtf_spec

How to Cite this Work
---------------------

Bradley AR, Rose AS, Pavelka A, Valasatava Y, Duarte JM, Prlić A, Rose PW (2017) MMTF - an efficient file format for the transmission, visualization, and analysis of macromolecular structures. PLOS Computational Biology 13(6): e1005575. doi: `10.1371/journal.pcbi.1005575 <https://doi.org/10.1371/journal.pcbi.1005575>`_

Valasatava Y, Bradley AR, Rose AS, Duarte JM, Prlić A, Rose PW (2017) Towards an efficient compression of 3D coordinates of macromolecular structures. PLOS ONE 12(3): e0174846. doi: `10.1371/journal.pone.01748464 <https://doi.org/10.1371/journal.pone.0174846>`_

Rose AS, Bradley AR, Valasatava Y, Duarte JM, Prlić A, Rose PW (2018) NGL viewer: web-based molecular graphics for large complexes, Bioinformatics, bty419. doi: `10.1093/bioinformatics/bty419 <https://doi.org/10.1093/bioinformatics/bty419>`_

Rose AS, Bradley AR, Valasatava Y, Duarte JM, Prlić A, Rose PW (2016) Web-based molecular graphics for large complexes. In Proceedings of the 21st International Conference on Web3D Technology (Web3D '16). ACM, New York, NY, USA, 185-186. doi: `10.1145/2945292.2945324 <https://doi.org/10.1145/2945292.2945324>`_

Funding
-------

This project is supported by the National Cancer Institute of the National Institutes of Health under Award Number U01CA198942. The content is solely the responsibility of the authors and does not necessarily represent the official views of the National Institutes of Health.
