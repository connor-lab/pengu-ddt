from datetime import datetime
import psycopg2

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import read_data_from_csv
from penGU.input.utils import string_to_bool

def cast_sequencing_data_types(sequencing_data_list):
    for record in sequencing_data_list:
        record["sequencing_start_date"] = datetime.strptime(record["sequencing_start_date"], '%Y-%m-%d')
        record["sequencing_end_date"] = datetime.strptime(record["sequencing_end_date"], '%Y-%m-%d')
        record["qc_pass"] = string_to_bool(record["qc_pass"])
    return sequencing_data_list


def update_sequencing_database(config_dict, csv_file):
    try:
        # Add csv data in one chunk then commit once and close connection
        sequencing_data_list = read_data_from_csv(csv_file)
        sequencing_data_clean = cast_sequencing_data_types(sequencing_data_list)
                
        # Get database connection
        NGSdb = NGSDatabase(config_dict)
        conn = NGSdb._connect_to_db()
        cur = conn.cursor()

        for row in sequencing_data_clean:
            print("Adding {y_number} | {sequencing_run} to the sequencing database".format(**row))

            sql = """INSERT INTO sequencing 
                           (fk_isolate_ID, 
                           sequencing_instrument, 
                           sequencing_run, 
                           sequencing_start_date, 
                           sequencing_end_date,
                           calculated_magnitude, 
                           trimmed_readlength,
                           mean_insert_size,
                           qc_pass) 
                           VALUES
                           ((SELECT pk_ID FROM isolate WHERE y_number = %(y_number)s), 
                           %(sequencing_instrument)s, 
                           %(sequencing_run)s, 
                           %(sequencing_start_date)s, 
                           %(sequencing_end_date)s,
                           %(calculated_magnitude)s, 
                           %(trimmed_readlength)s,
                           %(mean_insert_size)s,
                           %(qc_pass)s)"""

            cur.execute(sql, (row))
            
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.IntegrityError as error:
        print(error)