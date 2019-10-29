import psycopg2, psycopg2.extras

from penGU.db.NGSDatabase import NGSDatabase


def extract_data_from_db(config_dict, sample_name):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        sql = """SELECT i.y_number,
                c.t250,
                c.t100,
                c.t50,
                c.t25,
                c.t10,
                c.t5,
                c.t2,
                c.t0,
                cs.wg_number,
                cs.clustercode,
                rm.reference_name,
                mst.ST
                FROM isolate i 
                JOIN clustercode c 
                ON i.pk_ID = c.fk_isolate_ID 
                JOIN clustercode_snpaddress cs 
                ON c.fk_clustercode_ID = cs.pk_ID
                JOIN reference_metadata rm
                ON c.fk_reference_id = rm.pk_ID
                JOIN mlst_sequence_types mst 
                ON 
                    (SELECT fk_ST_ID FROM mlst WHERE fk_isolate_ID = i.pk_ID) = mst.pk_ID 
                WHERE y_number = %s ;"""

        dict_cur.execute(sql, (sample_name,))

        sample_data = dict_cur.fetchone()

        dict_cur.close()
        conn.close()

    except psycopg2.IntegrityError as e:
        print(e)

    return sample_data

def create_xml(config_dict, sample_name, output_xml):
    sample_data = extract_data_from_db(config_dict, sample_name)

    print(sample_data)