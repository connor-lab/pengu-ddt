import csv
import os
import psycopg2

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import read_data_from_csv


def update_isolate_database(config_dict, isolate_csv):
    try:
        # Get database connection
        NGSdb = NGSDatabase(config_dict)
        conn = NGSdb._connect_to_db()
        cur = conn.cursor()
    
        # Add csv data in one chunk then commit once and close connection
        isolate_data_list = read_data_from_csv(isolate_csv)
        
        for record in isolate_data_list:

            print("Adding {y_number} | {episode_number} to the isolate database".format(**record))

            sql = """INSERT INTO isolate (y_number, episode_number) VALUES (%(y_number)s, %(episode_number)s)"""
            
            cur.execute(sql, record)
        
        conn.commit()
        cur.close()
        conn.close()
    
    except psycopg2.IntegrityError as error:
        print(error)