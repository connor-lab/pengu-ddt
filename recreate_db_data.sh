#!/usr/bin/env bash

echo "drop database digest;" | psql -h localhost -U postgres

./pengu-ddt -c example_config/example.cfg create_db -d example_config/PenGU.sql
./pengu-ddt -c example_config/example.cfg add_isolates -i isolates.csv
#./pengu-ddt -c example_config/example.cfg add_sequencing_metadata -s example_data/sequencing_data.csv
#./pengu-ddt -c example_config/example.cfg update_mlst_db -p https://pubmlst.org/data/profiles/cdifficile.txt -n cdifficile -m M04531_176_000000000-CJ773_MLST.tab
./pengu-ddt -c example_config/example.cfg update_distance_refdb -r example_data/distance_reference_data.csv
#./pengu-ddt -c example_config/example.cfg update_distance_db -d example_data/distance_data.csv
./pengu-ddt -c example_config/example.cfg update_clustercode_db \
            -a "host=localhost user=snapperdb password=password dbname=RT015_TL174" \
            -g RT015_TL174 \
	    -i example_data/snp_address_isolate_data.txt \
	    -o example_output/snp_address_isolate_output.csv
#./pengu-ddt -c example_config/example.cfg dump_all_clustercodes -oa example_output/all_snp_addresses.csv
