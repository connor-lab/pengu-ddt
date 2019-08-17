import argparse

def parse_commandline_args():

    parser = argparse.ArgumentParser(prog='pengu-ddt')

    # Make config file required for all subcommands
    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument('-c', '--dbconfig', dest='config_file', required=True, help="Path to database config file")

    # Build subcommands
    subparsers = parser.add_subparsers(title='SUBCOMMANDS', dest="command")

    # Create database
    createdb_parser = subparsers.add_parser('create_db')
    createdb_parser.add_argument('-d', '--dbsql', dest='sql_scriptfile', required=True, help="Path to SQL script for blank DB")

    # Add csv of isolates to database
    add_isolate_parser = subparsers.add_parser('add_isolates')
    add_isolate_parser.add_argument('-i', '--isolate_csv', dest='isolate_csv', required=True, help="csv containing project specific ID and episode number")

    # Add sequencing csv to database
    add_sequencing_metadata_parser = subparsers.add_parser('add_sequencing_metadata')
    add_sequencing_metadata_parser.add_argument('-s', '--sequencing_csv', dest='sequencing_csv', required=True, help="csv containing sequencing run metadata")
    
    # Update mlst database
    update_mlst_db_parser = subparsers.add_parser('update_mlst_db')
    update_mlst_db_parser.add_argument('-p', '--pubmlst_url', dest='pubmlst_url', required=False, help="PubMLST URL to download MLST ST definitions (We'll use the previous one if not provided)")
    update_mlst_db_parser.add_argument('-n', '--mlst_scheme', dest='mlst_scheme_name', required=True, help="MLST scheme name")
    update_mlst_db_parser.add_argument('-m', '--mlst_csv', dest='mlst_csv', required=True, help="csv containing MLST and ST calls for isolates (from MLST")
    
    # Update mash distance reference database
    update_distance_refdb_parser = subparsers.add_parser('update_distance_refdb')
    update_distance_refdb_parser.add_argument('-r', '--reference_csv', dest='dist_ref_csv', required=True, help="csv containing reference genome metadata")
    
    # Update distance isolate database
    update_distance_db_parser = subparsers.add_parser('update_distance_db')
    update_distance_db_parser.add_argument('-d', '--distance_csv', dest='dist_csv', required=True, help="csv containing isolate mash distance data")

    # Update clustercode database
    update_clustercode_db_parser = subparsers.add_parser('update_clustercode_db')
    update_clustercode_db_parser.add_argument('-a', '--snapperdb_conf', dest='snapperdb_conf', required=True, help="Snapper3 connection string 'host= dbname= user= password= '")
    update_clustercode_db_parser.add_argument('-i', '--isolate_file', dest='isolate_file', required=True, help="textfile of y_numbers for isolate")
    update_clustercode_db_parser.add_argument('-g', '--reference_name', dest='snapperdb_refgenome', required=True, help="Name of reference genome in snapperDB")
    update_clustercode_db_parser.add_argument('-o', '--output_csv', dest='output_csv', required=True, help="output csv containing updated snp addresses")
    update_clustercode_db_parser.add_argument('-oa', '--output_all', dest='output_all', required=False, help="Output csv for all SNP addresses in database")
    

    # Parse args
    args = parser.parse_args()

    return args