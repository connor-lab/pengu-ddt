from datetime import datetime
import psycopg2

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import string_to_bool

def split_run_id(sequencing_data_list):
    for record in sequencing_data_list:
        record["sequencing_start_date"] = datetime.strptime(record["sequencing_start_date"], '%Y-%m-%d')
        record["sequencing_end_date"] = datetime.strptime(record["sequencing_end_date"], '%Y-%m-%d')
        record["qc_pass"] = string_to_bool(record["qc_pass"])
    return sequencing_data_list


def update_sequencing_database(config_dict, metadata_dict):

    sequencing_run_list = metadata_dict["sequencing_run"].split("_")

    sequencing_run_metadata = {"sequencing_start_date": datetime.strptime(sequencing_run_list[0], '%y%m%d' ),
                               "sequencing_instrument": sequencing_run_list[1],
                                "pipeline_start_date" : datetime.now() }

    metadata_dict.update( sequencing_run_metadata )

    try:
        # Get database connection
        NGSdb = NGSDatabase(config_dict)
        conn = NGSdb._connect_to_db()
        cur = conn.cursor()
        print("Adding {y_number} | {sequencing_run} to the sequencing database".format(**metadata_dict))
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
                       ((SELECT pk_ID FROM isolate WHERE y_number = %(y_number)s), 
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