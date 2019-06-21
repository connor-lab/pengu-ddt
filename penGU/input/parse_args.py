import argparse

def parse_commandline_args():

    parser = argparse.ArgumentParser(prog='ddt.py')

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
    add_isolate_parser = subparsers.add_parser('add_sequencing_metadata')
    add_isolate_parser.add_argument('-s', '--sequencing_csv', dest='sequencing_csv', required=True, help="csv containing sequencing run metadata")
    
    # Update mlst reference database
    add_isolate_parser = subparsers.add_parser('update_mlst_refdb')
    add_isolate_parser.add_argument('-p', '--pubmlst_url', dest='pubmlst_url', required=False, help="PubMLST URL to download MLST ST definitions (We'll use the previous one if not provided)")
    add_isolate_parser.add_argument('-n', '--mlst_scheme', dest='mlst_scheme_name', required=True, help="MLST scheme name")
    
    # Update mlst isolate database
    add_isolate_parser = subparsers.add_parser('update_mlst_db')
    add_isolate_parser.add_argument('-m', '--mlst_csv', dest='mlst_csv', required=True, help="csv containing MLST and ST calls for isolates")
    
    # Update mash distance reference database
    add_isolate_parser = subparsers.add_parser('update_distance_refdb')
    add_isolate_parser.add_argument('-r', '--reference_csv', dest='dist_ref_csv', required=True, help="csv containing reference genome metadata")
    
    # Update distance isolate database
    add_isolate_parser = subparsers.add_parser('update_distance_db')
    add_isolate_parser.add_argument('-d', '--distance_csv', dest='dist_csv', required=True, help="csv containing isolate mash distance data")

    # Parse args
    args = parser.parse_args()

    return args