#!/bin/bash

echo '{"irods_host": "data.cyverse.org", "irods_port": 1247, "irods_user_name": "anonymous", "irods_zone_name": "iplant"}' | envsubst > $HOME/.irods/irods_environment.json

# Download MMTF Hadoop Sequence File: full.tar
path=/iplant/home/pwrose/MMTF_Files/full.tar
iget -t KcGKzCIXviJRhxR $path
tar -xvf full.tar
rm full.tar

# The full directry ends up in $HOME/vice, move it back to $HOME
mv $HOME/vice/full $HOME/

# Move mmtf-pyspark with notebooks to default location
cd $HOME/vice
mv $HOME/mmtf-pyspark $HOME/vice/

# Move demo notebooks
mv $HOME/*.ipynb $HOME/vice/

echo '{"irods_host": "data.cyverse.org", "irods_port": 1247, "irods_user_name": "$IPLANT_USER", "irods_zone_name": "iplant"}' | envsubst > $HOME/.irods/irods_environment.json

exec jupyter lab --no-browser GettingStarted.ipynb
