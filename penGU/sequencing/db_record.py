from datetime import datetime
import psycopg2

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import string_to_bool

def update_sequencing_database(config_dict, metadata_dict):

    sequencing_run_metadata = {}

    sequencing_run_metadata["pipeline_start_date"] = datetime.now()]

    sequencing_run_list = metadata_dict["sequencing_run"].split("_")

    if len(sequencing_run_list) > 1:
        try:
            sequencing_start_date = datetime.strptime(sequencing_run_list[0], '%y%m%d' )
            sequencing_run_metadata["sequencing_start_date"] = sequencing_start_date

        except ValueError:
            sequencing_start_date = datetime.strptime(sequencing_run_list[0], '%y%m%d' )
            sequencing_run_metadata["sequencing_start_date"] = None

        sequencing_run_metadata["sequencing_instrument"] = sequencing_run_list[1]

    else:
         sequencing_run_metadata["sequencing_instrument"] = None
         sequencing_run_metadata["sequencing_start_date"] = None

    metadata_dict.update( sequencing_run_metadata )

    try:
        # Get database connection
        NGSdb = NGSDatabase(config_dict)
        conn = NGSdb._connect_to_db()
        cur = conn.cursor()
        print("Adding {accession} | {sequencing_run} to the sequencing database".format(**metadata_dict))
        sql = """INSERT INTO sequencing 
                       (fk_isolate_ID, 
                       sequencing_instrument, 
                       sequencing_run, 
                       sequencing_start_date, 
                       pipeline_start_date,
                       ref_coverage_mean, 
                       ref_coverage_stddev,
                       z_score_fail) 
                       VALUES
                       ((SELECT pk_ID FROM isolate WHERE accession = %(accession)s), 
                       %(sequencing_instrument)s, 
                       %(sequencing_run)s, 
                       %(sequencing_start_date)s, 
                       %(pipeline_start_date)s,
                       %(ref_coverage_mean)s, 
                       %(ref_coverage_stddev)s,
                       %(z_score_fail)s )"""
        cur.execute(sql, (metadata_dict))
        
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.IntegrityError as error:
        print(error)