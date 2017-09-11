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

1.  Using Anaconda and GOW(recommended)

If you already have anaconda and GOW installed, skip this step.

[Download Anaconda](https://docs.continuum.io/anaconda/install/windows)

Basically, GOW allows you to use linux commands on windows. In this install, we will need curl, gzip, tar which GOW provides.

[Download GOW](https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe)

2.  Close and open a new command line (CMD).

3.  Go to the Apache Spark website

[Download Apache Spark](http://spark.apache.org/downloads.html)

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
