# Installation on Windows

## Prerequisites
The following libraries and tools are required to install mmtf-spark. Except for Java, you need to choose an installation directory, for example your home directory C:\Users\USER_NAME. This directory is a placeholder for a location of your choice.


### Install Java SE Development Toolkit (JDK 1.8)
If you do not have JDK, or if you are using any version other than 1.8, please install JDK 1.8.

[Download JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and run the installer.


## Install Git
The Git version control system is used to download repositories from Github.

[Download Git](https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe) and run the installer (choose all default options)


## Install Spark

As an example in following steps, _YOUR_DIRECTORY_ could be "C:\opt\spark", _YOUR_SPARK_VERSION_ could be "spark-2.3.0-bin-hadoop2.7".

1.  Using GOW:

    GOW allows you to use linux commands on windows. In this install, we will need curl, gzip, tar which GOW provides.

    [Download GOW](https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe)

2. Download Apache Spark 2.3  
    Go to the Apache Spark website [link](http://spark.apache.org/downloads.html)

    a) Choose a Spark version 2.3

    b) Choose a package type

    c) Choose a download type: (Direct Download)

    d) Download Spark

    e) Move the file to YOUR_DIRECTORY

    f) Unzip the file with the following commands:

    ```
    cd _YOUR_DIRECTORY_

    gzip -d YOUR_SPARK_VERSION.tgz

    tar xvf YOUR_SPARK_VERSION.tar
    ```

3.  Download winutils.exe into "_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_\\bin" using following command.

    ```
    curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe?raw=true
    ```

4.  Next, we will edit our environmental variables.

    **Find environmental variables:**

	a) In Search, search for and then select: System (Control Panel)

	b) Click the Advanced system settings link.

	c) Click Environment Variables.

	d) In the section System Variables find the environment variables and select it. Click Edit. If the environment variable does not exist, click New.

    **Set environmental variables:**

	a) set "SPARK_HOME" to "_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_"

	b) set "HADOOP_HOME" to "_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_"

	c) Add ";_YOUR_DIRECTORY_\\_YOUR_SPARK_VERSION_\\bin" to your "PATH".


### Install mmtfPyspark
To install mmtfPyspark, make sure you have pip:

```
pip --version
```

Pip should be included if you have python 3.4+

mmtfPyspark can be installed using two different ways:
 * PyPI install (from the python packaging index):

    ```
    pip install mmtfPyspark
    ```

 * pip install (cloning github repository and do a local installation):

    ```
    git clone https://github.com/sbl-sdsc/mmtf-pyspark.git
    pip install ./mmtf-pyspark/
    ```

If there are any errors installing package, try upgrading your pip by:

    ```
    pip install --upgrade pip    
    ```

By cloning the Github repository using the pip install method, sample jupyter notebooks and tutorials can be found in the *mmtf-pyspark/demos* directory. All the demos requires jupyter notebooks 5.4. To check if you have jupyter 5.4 installed:

    ```
    jupyter notebook --version    
    ```

If you do not have jupyter intalled:
    ```
    pip install jupyter    
    ```

If you have a version lower than 5.4:
    ```
    pip install --upgrade jupyter    
    ```

### Testing installation
To test if the installation is successful:

```
curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/test_mmtfPyspark.py -o test_mmtfPyspark.py

python test_mmtfPyspark.py
```

If the metadata of 1AQ1 is printed, you have successfully intalled mmtfPyspark.


### [OPTIONAL] Hadoop Sequence Files
Hadoop sequence files of all PDB structures can be downloaded and environmental variables can be set by running the following command:
```
curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/download_mmtf_files.sh -o download_mmtf_files.sh
. ./download_mmtf_files.sh
```
