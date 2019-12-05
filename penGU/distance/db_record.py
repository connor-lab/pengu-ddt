import os
import psycopg2
import re

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import read_data_from_csv

def parse_dist_csv(dist_dict):
    for row in dist_dict:
        row["reference_name"] = os.path.splitext(os.path.basename(row["reference_name"]))[0]
        row["accession"] = os.path.splitext(os.path.basename(row["accession"]))[0]
        if row["accession"].startswith("DIGCD"):
            m = re.search('DIGCD-(.*)_S\\d+$', row["accession"])
            row["accession"] = m.group(1)
    return dist_dict

def update_distance_database(config_dict, dist_csv):
    dist_cols = ["reference_name",
                 "accession",
                 "reference_mash_distance",
                 "reference_mash_p_value",
                 "reference_common_kmers"]
    dist_data = read_data_from_csv(dist_csv, header=dist_cols)
    dist_data_clean = parse_dist_csv(dist_data)    

    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()
    try:
        for row in dist_data_clean:
            
            sql = """INSERT INTO reference_distance
                       (fk_isolate_ID,
                       fk_reference_ID, 
                       reference_mash_distance, 
                       reference_mash_p_value,
                       reference_common_kmers)
                       VALUES ((SELECT pk_ID from isolate WHERE accession = %(accession)s),
                       (SELECT pk_ID from reference_metadata WHERE reference_name = %(reference_name)s), 
                       %(reference_mash_distance)s, %(reference_mash_p_value)s, 
                       %(reference_common_kmers)s);"""

            cur.execute(sql, row)

        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.IntegrityError as e:
            print(e)