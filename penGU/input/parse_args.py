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
    add_sequencing_metadata_parser.add_argument('-a', '--accession', dest='accession', required=True, help="accession")
    add_sequencing_metadata_parser.add_argument('-r', '--run_id', dest='run_id', required=True, help="Sequencer run ID")
    add_sequencing_metadata_parser.add_argument('-d', '--depth', dest='depth', required=False, help="Sequencing depth")
    add_sequencing_metadata_parser.add_argument('-s', '--stddev', dest='stddev', required=False, help="Sequencing depth stddev")
    add_sequencing_metadata_parser.add_argument('-z', '--zscore_fail', dest='zscore_fail', required=False, help="Z-score fail (true/false)")

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

    # Dump all clustercode data
    dump_all_clustercodes_parser = subparsers.add_parser('dump_all_clustercodes')
    dump_all_clustercodes_parser.add_argument('-oa', '--output_all', dest='output_all', required=True, help="Output csv for all SNP addresses in database")
    
    # Update clustercode database
    update_clustercode_db_parser = subparsers.add_parser('update_clustercode_db')
    update_clustercode_db_parser.add_argument('-a', '--snapperdb_conf', dest='snapperdb_conf', required=True, help="Snapper3 connection string 'host= dbname= user= password= '")
    update_clustercode_db_parser.add_argument('-i', '--isolate_file', dest='isolate_file', required=True, help="textfile of accession numbers for isolates")
    update_clustercode_db_parser.add_argument('-g', '--reference_name', dest='snapperdb_refgenome', required=True, help="Name of reference genome in snapperDB")
    update_clustercode_db_parser.add_argument('-o', '--output_csv', dest='output_csv', required=True, help="output csv containing updated snp addresses")
    
    # Dump data as xml file
    output_xml_parser = subparsers.add_parser('output_xml')
    output_xml_parser.add_argument('-ox', '--output_xml', dest='output_xml', required=True, help="XML containing output data")
    output_xml_parser.add_argument('-a', '--accession', dest='accession', required=True, help="Sample accession to generate XML for")

    # Dump CSV of everything we know about a list of isolates
    output_summary_csv_parser = subparsers.add_parser('output_summary_csv')
    output_summary_csv_parser.add_argument('-oc', '--output_csv', dest='output_csv', required=True, help="CSV containing output data")
    output_summary_csv_parser.add_argument('-i', '--isolate_file', dest='isolate_file', required=True, help="Text file containing sample names, one per line")
    output_summary_csv_parser.add_argument('-qc', '--report_qc', dest='report_qc', action="store_true", help="Report QC pass/fail in output")
    
    # Parse args
    args = parser.parse_args()

    return args