# Change Log
All notable changes to this project will be documented in this file, following the suggestions of [Keep a CHANGELOG](http://keepachangelog.com/). This project adheres to [Semantic Versioning](http://semver.org/).

## v0.2.7 - 2018-11-24
- Bug fixes
  - Fixed MMTF decoding error


- New features
  - Added method to retrieve PTMs from dbPTM
  - Added method to retrieve PTMs from PDB
  - Added method to retrieve PDB to Uniprot chain and residue level mappings
  - Added support for ligandIds in advanced search results
  - Added method to create dataset for advanced search results
  - Added pyarrow 0.8.0 dependency
  - Added xlrd 1.1.0 dependency to support Excel spreadsheets with pandas
  
  
- Changes
  - Use https instead of ftp to retrieve BlastClust data
  - Upgraded py3dmol to 0.8.0
  - Moved binder related files to binder directory
  
  
  
## v0.2.6 - 2018-05-21
### Upgrade to mmtf-python 1.1.2 
- use latest version of mmtf-python that uses msgpack 0.5.6
- update to msgpack>=0.5.6 from msgpack-python (PyPi package name has changed!)
- update py4j to 0.10.6 as required by pyspark 2.3.0
 
## v0.2.5 - 2018-05-18
### Remove SparkContext as parameter
- remove sc from mmtfReader and mmtfWriter functions parameters
- fix windows durgbank decoding issue

## v0.2.4 - 2018-05-06
### Final updates before MMTF Workshop 2018
- Included all modules in __init__ files

## v0.2.2 - 2018-05-04
### Using Wheel for better packaging
- Using wheel to package

## v0.2.1 - 2018-05-04
### Update version for MMTF Workshop 2018
- New functions, classes and demo notebooks

## v0.2.0 - 2018-03-05
### Alpha Version Released on PyPI
- Finished all classes, functions and unit tests for alpha release

## v0.1.0 - 2017-11-27
### Development Version
- Created setup.py for local pip install
