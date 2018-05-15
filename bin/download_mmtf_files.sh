#!/bin/bash

OUTPUT=''

# Taking in directory specified for installation
while test $# -gt 0; do
        case "$1" in
                -h|--help)
                        echo " "
                        echo "options:"
                        echo "-h, --help                show brief help"
                        echo "-o, --output-dir=DIR      specify a directory for installation"
                        exit 0
                        ;;
                -o)
                        shift
                        if test $# -gt 0; then
                                export OUTPUT=$1
                        fi
                        shift
                        ;;
                *)
                        break
                        ;;
        esac
done

if [[ $OUTPUT = '' ]]; then
  OUTPUT=$HOME
  echo "Download location not specified, using default home directory: $OUTPUT"
fi 

if [ ! -d "$OUTPUT" ]; then
  OUTPUT=$HOME
  echo 'Directory does not exist, using default home directory: ' $OUTPUT 
fi


cd $OUTPUT

# download mmtf full and reduced file
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
tar -xvf reduced.tar

# Set environmental variables
echo "export MMTF_REDUCED=$OUTPUT/reduced/" >> ~/.bashrc
echo "export MMTF_FULL=$OUTPUT/full/" >> ~/.bashrc

cd ~
source ~/.bashrc
