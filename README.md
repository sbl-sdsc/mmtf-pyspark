# Mmtf-pyspark

[![Build Status](https://travis-ci.org/sbl-sdsc/mmtf-pyspark.svg?branch=master)](https://travis-ci.org/sbl-sdsc/mmtf-pyspark)
[![GitHub license](https://img.shields.io/github/license/sbl-sdsc/mmtf-pyspark.svg)](https://github.com/sbl-sdsc/mmtf-pyspark/blob/master/LICENSE)
[![Version](http://img.shields.io/badge/version-0.2.0-yellowgreen.svg?style=flat)](https://github.com/sbl-sdsc/mmtf-pyspark)
[![Download MMTF](http://img.shields.io/badge/download-MMTF_full-yellow.svg?style=flat)](https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar)
[![Download MMTF Reduced](http://img.shields.io/badge/download-MMTF_reduced-orange.svg?style=flat)](https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar)
[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/sbl-sdsc/mmtf-pyspark/master)
[![Twitter URL](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/mmtf_spec)

Methods for parallel and distributed analysis and mining of the Protein Data Bank using MMTF and Apache Spark.

This project is still currently under development.

## Demos
To try out our jupyter notebook demos without any installation, please click on the pink "launch binder" icon on top and navigate to the /demos folder (An example data analysis notebook, "DataAnalysisWithDataFrameExample", can be found in the main directory).

It can take a few minutes for Binder to setup the environment.

## Installation
1. Clone this repository to a local directory

2. Install mmtf-pyspark with pip:

```
pip install mmtf-pyspark/
```

## Hadoop Sequence Files

The Hadoop sequence files can be downloaded with:
```
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
```

## Requirements

### anaconda
We strongly recommend that you have [anaconda](https://docs.continuum.io/anaconda/install/) and the lastest version of python installed

### Pyspark

#### Mac

1. Download Spark from Apache website [link](http://spark.apache.org/downloads.html)

2. Go to the downloaded file's directory and unzip the file

For example, if your file is in **Downloads**
```
cd Downloads/
tar -zxvf spark-2.6.0-bin-hadoop2.6.tgz
```

3. Install py4j, run

If Anaconda is installed :

```
conda install py4j
```

Else, use pip install:

```
pip install py4j
```

4. Add pyspark path in your bash_profile

Open bash_profile with your favorite editor, eg:

```
cd ~
vim ~/.bash_profile

```

Add the following lines in your bash_profile:

```
export SPARK_HOME=~/spark-2.2.0-bin-hadoop2.7  <Path to your spark>
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
export PYSPARK_PYTHON=python3
```

5. Source your bash_profile

Either reopen your terminal, or run the follwing command:

```
source ~/.bash_profile
```

6. Check installation by importing pyspark

open **python** or **ipython**, and try:

```
import pyspark
```

#### Windows

0.  If you already have GOW installed, skip to step 2.

1.  Using GOW(recommended):

    Basically, GOW allows you to use linux commands on windows. In this install, we will need curl, gzip, tar which GOW provides.

    [Download GOW](https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe)

2.  Go to the Apache Spark website [link](http://spark.apache.org/downloads.html)

    a) Choose a Spark release

    b) Choose a package type

    c) Choose a download type: (Direct Download)

    d) Download Spark

    e) Move the file to where you want to unzip it

    f) Unzip the file. Use the commands below

    ```
    gzip -d YOUR_SPARK_VERSION.tgz

    tar xvf YOUR_SPARK_VERSION.tar
    ```

3.  Download winutils.exe into "_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_\\bin" using following command.

    As an example in following steps, _YOUR_DIRECTORY_ could be "C:\opt\spark", _YOUR_SPARK_VERSION_ could be "spark-2.2.0-bin-hadoop2.6".

```
curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.0/bin/winutils.exe?raw=true
```

4.  Make sure you have Java 7+ installed on your machine.

5.  Next, we will edit our environmental variables so we can open a spark notebook in any directory.

    **Find environmental variables:**

	a) In Search, search for and then select: System (Control Panel)

	b) Click the Advanced system settings link.

	c) Click Environment Variables.

	d) In the section System Variables find the environment variables and select it. Click Edit. If the environment variable does not exist, click New.

    **Set environmental variables:**

	a) set "SPARK_HOME" to "_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_"

	b) set "HADOOP_HOME" to "_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_"

	c) set "PYSPARK_DRIVER_PYTHON" to "ipython"

	d) set "PYSPARK_DRIVER_PYTHON_OPTS" to "notebook"

	e) Add ";_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_\\bin" to your "PATH".

6.  Close your terminal and open a new one. Type the command below to test if pyspark has been installed.

```
pyspark --master local[2]
```

#### Linux [link](https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0)

### JDK

Depending on your OS, Java Development Kit might need to be installed
