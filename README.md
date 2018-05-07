# MMTF PySpark

[![Build Status](https://travis-ci.org/sbl-sdsc/mmtf-pyspark.svg?branch=master)](https://travis-ci.org/sbl-sdsc/mmtf-pyspark)
[![GitHub license](https://img.shields.io/github/license/sbl-sdsc/mmtf-pyspark.svg)](https://github.com/sbl-sdsc/mmtf-pyspark/blob/master/LICENSE)
[![Version](http://img.shields.io/badge/version-0.2.4-yellowgreen.svg?style=flat)](https://github.com/sbl-sdsc/mmtf-pyspark)
[![Download MMTF](http://img.shields.io/badge/download-MMTF_full-yellow.svg?style=flat)](https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar)
[![Download MMTF Reduced](http://img.shields.io/badge/download-MMTF_reduced-orange.svg?style=flat)](https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar)
[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-pyspark/master)
[![Twitter URL](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/mmtf_spec)

mmtfPyspark is a python package that provides APIs and sample applications for distributed analysis and scalable mining of 3D biomacromolecular structures, such as the Protein Data Bank (PDB) archive. mmtfPyspark uses Big Data technologies to enable high-performance parallel processing of macromolecular structures. mmtfPyspark use the following technology stack:
* [Apache Spark](https://spark.apache.org/) a fast and general engine for large-scale distributed data processing.
* [MMTF](https://mmtf.rcsb.org/) the Macromolecular Transmission Format for compact data storage, transmission and high-performance parsing
* [Hadoop Sequence File](https://wiki.apache.org/hadoop/SequenceFile) a Big Data file format for parallel I/O
* [Apache Parquet](https://parquet.apache.org/) a columnar data format to store dataframes

This project is still currently under development.

## Demos
To try out our jupyter notebook demos without any installation, please click on the pink "launch binder" icon on top and navigate to the /demos folder (An example data analysis notebook, "DataAnalysisWithDataFrameExample", can be found in the main directory).

It can take a few minutes for Binder to setup the environment.

## Installation
### Python
We strongly recommend that you have [anaconda](https://docs.continuum.io/anaconda/install/) and we require at least python 3.6 installed. To check your python version:
```
python --version
```

If **Anaconda** is installed, and if you have python 3.6, the above command should return:
```
Python 3.6.4 :: Anaconda, Inc.
```

### mmtfPyspark and dependencies
Since mmtfPyspark uses parallel computing to ensure high-performance, it requires additional dependencies such as Apache Spark. Therefore, please read follow the installation instructions for your OS carefully:

[MacOS and LINUX](docs/MacLinuxInstallation.md)

[Windows](docs/WindowsInstallation.md)


## Hadoop Sequence Files

The MMTF Hadoop sequence files of all PDB structures can be downloaded by:
```
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar

curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
tar -xvf reduced.tar
```

For Mac and Linux, the Hadoop sequence files can be downloaded and saved as environmental variables by running the following command:
```
curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/download_mmtf_files.sh -o download_mmtf_files.sh
. ./download_mmtf_files.sh
```
