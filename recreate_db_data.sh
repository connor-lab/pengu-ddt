#!/usr/bin/env bash

echo "drop database digest;" | psql -h localhost -U postgres

./ddt.py -c example_config/example.cfg create_db -d example_config/PenGU.sql

./ddt.py -c example_config/example.cfg add_isolates -i example_data/isolate_data.csv

./ddt.py -c example_config/example.cfg add_sequencing_metadata -s example_data/sequencing_data.csv

./ddt.py -c example_config/example.cfg update_mlst_refdb -p https://pubmlst.org/data/profiles/cdifficile.txt -n cdifficile

./ddt.py -c example_config/example.cfg update_mlst_db -m example_data/mlst_data.csv

./ddt.py -c example_config/example.cfg update_distance_refdb -r example_data/distance_reference_data.csv

./ddt.py -c example_config/example.cfg update_distance_db -d example_data/distance_data.csv
