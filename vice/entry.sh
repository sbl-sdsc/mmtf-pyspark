#!/bin/bash

echo '{"irods_host": "data.cyverse.org", "irods_port": 1247, "irods_user_name": "$IPLANT_USER", "irods_zone_name": "iplant"}' | envsubst > $HOME/.irods/irods_environment.json

# Move mmtf-pyspark with notebooks to default location
cd $HOME/vice
mv $HOME/mmtf-pyspark $HOME/vice/

# Download MMTF Hadoop Sequence File: full.tar
path=/iplant/home/pwrose/MMTF_Files/full.tar
iget -t KcGKzCIXviJRhxR $path
tar -xvf full.tar

exec jupyter lab --no-browser
