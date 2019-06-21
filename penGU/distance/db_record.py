import os
import psycopg2
import re

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import read_data_from_csv

def parse_dist_csv(dist_dict):
    for row in dist_dict:
        row["reference_name"] = os.path.splitext(os.path.basename(row["reference_name"]))[0]
        if row["y_number"].startswith("DIGCD"):
            row["y_number"] = os.path.splitext(os.path.basename(row["y_number"]))[0]
            m = re.search('DIGCD-(.*)_S\\d+$', row["y_number"])
            row["y_number"] = m.group(1)
    return dist_dict

def update_distance_database(config_dict, dist_csv):
    dist_cols = ["reference_name",
                 "y_number",
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
            cur.execute("""INSERT INTO reference_distance
                       (y_number,
                       reference_name, 
                       reference_mash_distance, 
                       reference_mash_p_value,
                       reference_common_kmers)
                       VALUES (%(y_number)s, %(reference_name)s, %(reference_mash_distance)s, %(reference_mash_p_value)s, %(reference_common_kmers)s);
                    """, row)
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.IntegrityError as e:
            print(e)