#!/usr/bin/env bash

echo "drop database digest;" | psql -h localhost -U postgres

./pengu-ddt -c example_config/example.cfg create_db -d example_config/PenGU.sql

./pengu-ddt -c example_config/example.cfg add_isolates -i example_data/isolate_data.csv

./pengu-ddt -c example_config/example.cfg add_sequencing_metadata -s example_data/sequencing_data.csv

./pengu-ddt -c example_config/example.cfg update_mlst_db -p https://pubmlst.org/data/profiles/cdifficile.txt -n cdifficile -m example_data/mlst_data.csv

./pengu-ddt -c example_config/example.cfg update_distance_refdb -r example_data/distance_reference_data.csv

./pengu-ddt -c example_config/example.cfg update_distance_db -d example_data/distance_data.csv

./pengu-ddt -c example_config/example.cfg update_clustercode_db -i example_data/snp_address_isolate_data.txt -a example_config/example_snapperdb_conf.txt -o example_output/snp_address_isolate_output.csv
