# Mmtf-pyspark
Methods for parallel and distributed analysis and mining of the Protein Data Bank using MMTF and Apache Spark.

# Requirements

## anaconda
We strongly recommend that you have [anaconda](https://docs.continuum.io/anaconda/install/) and the lastest version of python installed


## biopython
Please install [biopython](http://biopython.org/wiki/Download)

## mmtf-python

The most up-to-date mmtf-python is not on pip or Anaconda, please clone it from the following [github link](https://github.com/rcsb/mmtf-python)


## pyspark

The following links are tutorials to install pyspark on differnt OS systems

### Mac

Using Homebrew(recommended):

1. Install Homebrew, go to terminal and run

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

2. Install apache-spark, run

```
brew install apache-spark
```

3. Install py4j, run

```
conda install py4j
```
[Manual download](https://medium.com/@GalarnykMichael/install-spark-on-mac-pyspark-453f395f240b)

### Windows

0.  If you already have anaconda and GOW installed, skip to step 2.

1.  Using Anaconda and GOW(recommended):

    [Download Anaconda](https://docs.continuum.io/anaconda/install/windows)

    Basically, GOW allows you to use linux commands on windows. In this install, we will need curl, gzip, tar which GOW provides.

    [Download GOW](https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe)

2.  Close and open a new command line (CMD).

3.  Go to the Apache Spark website [link](http://spark.apache.org/downloads.html)

    a) Choose a Spark release

    b) Choose a package type

    c) Choose a download type: (Direct Download)

    d) Download Spark

    e) Move the file to where you want to unzip it

    f) Unzip the file. Use the bolded commands below

    ```
    gzip -d spark-2.1.0-bin-hadoop2.7.tgz

    tar xvf spark-2.1.0-bin-hadoop2.7.tar
    ```
4.  Download winutils.exe into your spark-2.1.0-bin-hadoop2.7\bin

```
curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.0/bin/winutils.exe?raw=true
```

5.  Make sure you have Java 7+ installed on your machine.

6.  Next, we will edit our environmental variables so we can open a spark notebook in any directory.

    a) In Search, search for and then select: System (Control Panel)

    b) Click the Advanced system settings link.

    c) Click Environment Variables.

    d) In the section System Variables find the environment variables and select it.

        Click Edit. If the environment variable does not exist, click New.

    (a) set "SPARK_HOME" to "C:\YOUR_DIRECTORY\spark\spark-2.1.0-bin-hadoop2.7"

    (b) set "HADOOP_HOME" to "C:\YOUR_DIRECTORY\spark\spark-2.1.0-bin-hadoop2.7"

    (c) set "PYSPARK_DRIVER_PYTHON" to "ipython"

    (d) set "PYSPARK_DRIVER_PYTHON_OPTS" to "notebook"

    (e) Add ";C:\YOUR_DIRECTORY\spark\spark-2.1.0-bin-hadoop2.7\bin" to your "PATH".


### Linux [link](https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0)

## JDK

Depending on your OS, Java Development Kit might need to be installed


# Usage

You can either create a scipt/jupyter-notebook in the *mmtf-pyspark/mmtfPyspark* and follow the examples from *mmtf-pyspark/mmtfPyspark/DataAnalysisWithDataFrameExample*.

Or you can import mmtf-pyspark to your code from another directory with the following lines:

```python
import sys
sys.path.append("<path to your mmtf-pyspark folder")
sys.path.append("<path to your mmtf-python folder")
import mmtfPyspark
from mmtfPyspark.src.main import filters
```

# Examples

Filters usage examples can be found in *mmtf-pyspark/mmtfPyspark/src/demos/filters*

# Hadoop Sequence Files

The Hadoop sequence files can be downloaded with:
```
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
```
