import sys

from penGU.db.penguDB import check_or_create_db

from penGU.isolate.db_record import update_isolate_database

from penGU.sequencing.db_record import update_sequencing_database

from penGU.mlst.update_refdb import generate_mlst_refdb
from penGU.mlst.db_record import update_mlst_database

from penGU.distance.update_refdb import update_distance_refdb
from penGU.distance.db_record import update_distance_database

from penGU.snpaddress.clustercode_isolate import get_snpaddresses_from_snapperdb, update_isolate_clustercode_db, get_all_clustercode_data
from penGU.snpaddress.clustercode_database import update_clustercode_database
from penGU.snpaddress.clustercode_history import update_clustercode_history

from penGU.input.utils import check_config, read_lines_from_isolate_data

from penGU.output.sample_data import get_all_sample_data
from penGU.output.utils import write_updated_records_to_csv, write_all_records_to_csv, write_sample_xml, flatten_sample_dict, write_sample_data_to_csv, add_qc_pass_fail_to_sample_data


def run_commmand(config_dict, args):
    if args.command is None:
        print("Please use a subcommand. Subcommand options can be viewed with " + sys.argv[0] + " --help\n")

    elif 'create_db' in args.command:
        check_or_create_db(config_dict, args.sql_scriptfile)
        
    elif 'add_isolates' in args.command:
        update_isolate_database(config_dict, args.isolate_csv)
    
    elif 'add_sequencing_metadata' in args.command:
        sequencing_meta_dict = { "accession" : args.accession, 
                           "sequencing_run" : args.run_id,
                        "ref_coverage_mean" : args.depth,
                      "ref_coverage_stddev" : args.stddev,
                             "z_score_fail" : args.zscore_fail }
        
        update_sequencing_database(config_dict, sequencing_meta_dict)

    elif 'update_mlst_db' in args.command:
        generate_mlst_refdb(config_dict, args.mlst_scheme_name, args.pubmlst_url)
        update_mlst_database(config_dict, args.mlst_csv)

    elif 'update_distance_refdb' in args.command:
        update_distance_refdb(config_dict, args.dist_ref_csv)
    
    elif 'update_distance_db' in args.command:
        update_distance_database(config_dict, args.dist_csv)

    elif 'dump_all_clustercodes' in args.command:
        all_snapperdb_snpaddresses = get_all_clustercode_data(config_dict)
        write_all_records_to_csv(all_snapperdb_snpaddresses, args.output_all)

    elif 'update_clustercode_db' in args.command:
        # Check config
        snapperdb_config = check_config(args.snapperdb_conf, config_type="snapperdb")

        # Get all snp addresses from a snapperDB instance
        all_snapperdb_snpaddresses = get_snpaddresses_from_snapperdb(snapperdb_config, args.snapperdb_refgenome)

        # Update the clustercode database, returning { clustercode : wg_number }
        clustercode_wgnumber = update_clustercode_database(config_dict, all_snapperdb_snpaddresses)

        # Assign wg_numbers to all records
        for record in all_snapperdb_snpaddresses:
            record['wg_number'] = clustercode_wgnumber.get(record['clustercode'])

        # Find any records with any data points updated and return a list of dicts of updated records
        updated_records = update_isolate_clustercode_db(config_dict, args.snapperdb_refgenome, args.isolate_file, all_snapperdb_snpaddresses)
        
        # Update isolate history table and write updated samples to a csv 
        if updated_records:
            update_clustercode_history(config_dict, updated_records)
            write_updated_records_to_csv(updated_records, args.output_csv)

    elif 'output_xml' in args.command:
        sample_data = get_all_sample_data(config_dict, args.accession)
        sample_data = add_qc_pass_fail_to_sample_data(sample_data)
        
        write_sample_xml(sample_data, args.output_xml)
    
    elif 'output_summary_csv' in args.command:
        isolates = read_lines_from_isolate_data(args.isolate_file)

        csv_rows = []

        for isolate in isolates:
            sample_data = get_all_sample_data(config_dict, isolate)

            if args.report_qc:
                sample_data = add_qc_pass_fail_to_sample_data(sample_data)

            csv_rows.append(flatten_sample_dict(sample_data))

        write_sample_data_to_csv(csv_rows, args.output_csv)
        
