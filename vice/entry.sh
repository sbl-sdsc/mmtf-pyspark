#!/bin/bash

echo '{"irods_host": "data.cyverse.org", "irods_port": 1247, "irods_user_name": "$IPLANT_USER", "irods_zone_name": "iplant"}' | envsubst > $HOME/.irods/irods_environment.json
cd $HOME/vice
mv $HOME/mmtf-pyspark $HOME/vice/

path=/iplant/home/pwrose/MMTF_Files/full.tar
ticket=$(iticket create -r "$path" | cut -d ":" -f 2)
iget -t $ticket $path
tar -xvf full.tar

exec jupyter lab --no-browser
