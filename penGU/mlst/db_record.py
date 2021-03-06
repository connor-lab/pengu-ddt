import datetime
import os
import re
import sys

import psycopg2

from penGU.input.utils import read_data_from_csv
from penGU.db.NGSDatabase import NGSDatabase


def parse_mlst_csv(csv_file):
    mlst_cols = ["accession", 
                 "scheme", 
                 "ST", 
                 "locus_1", 
                 "locus_2", 
                 "locus_3", 
                 "locus_4", 
                 "locus_5", 
                 "locus_6", 
                 "locus_7"]
    mlst_data = read_data_from_csv(csv_file, header=mlst_cols, field_sep="\t")
    mlst_data_clean = []
    for row in mlst_data:
        row["accession"] = os.path.splitext(os.path.basename(row["accession"]))[0]
        if row["accession"].startswith("DIGCD"):
            m = re.search('DIGCD-(.*)_S\\d+$', row["accession"])
            row["accession"] = m.group(1)

        for key in row:
                if key.startswith("locus"):
                    if row[key] is not None:
                        row[key] = row[key][row[key].find("(")+1:row[key].find(")")]
        mlst_data_clean.append(row)

    return mlst_data_clean


def update_mlst_database(config_dict, csv_file):
    mlst_data_clean = parse_mlst_csv(csv_file)
    
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()
    try:
        for row in mlst_data_clean:

            sql = """INSERT INTO mlst
                    (fk_isolate_ID, fk_ST_ID, locus_1, locus_2, locus_3, 
                    locus_4, locus_5, locus_6, locus_7) VALUES 
                    ((SELECT pk_ID from isolate WHERE accession = %(accession)s),
                    (SELECT pk_ID from mlst_sequence_types WHERE ST = %(ST)s),
                    %(locus_1)s, %(locus_2)s, 
                    %(locus_3)s, %(locus_4)s, %(locus_5)s, 
                    %(locus_6)s, %(locus_7)s);"""

            cur.execute(sql, row)

        conn.commit()
        cur.close()
        conn.close()
        
    except psycopg2.IntegrityError as e:
            print(e)
    
