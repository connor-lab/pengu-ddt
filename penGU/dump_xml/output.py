import psycopg2, psycopg2.extras
from dicttoxml import dicttoxml

from penGU.db.NGSDatabase import NGSDatabase


def extract_mlst_scheme_from_db(config_dict, sample_name):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        sql = """SELECT * FROM mlst_scheme_metadata"""

        dict_cur.execute(sql)
        mlst_metadata = dict_cur.fetchone()
        dict_cur.close()
        conn.close()

    except psycopg2.IntegrityError as e:
        print(e)

    return mlst_metadata


def extract_data_from_db(config_dict, sample_name):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        sql = """SELECT i.y_number, 
	             c.t250, c.t100, c.t50, c.t25, c.t10, c.t5, c.t2, c.t0, 
	             cs.clustercode, cs.wg_number, cs.clustercode_frequency, 
	             rm.reference_name,
	             mst.st,
	             st.locus_1, st.locus_2, st.locus_3, st.locus_4, st.locus_5, st.locus_6, st.locus_7 
	             FROM isolate i 
                    FULL OUTER JOIN clustercode c 
                        ON i.pk_ID = c.fk_isolate_ID 
                    FULL OUTER JOIN clustercode_snpaddress cs 
                        ON c.fk_clustercode_ID = cs.pk_ID
                    FULL OUTER JOIN reference_metadata rm
                        ON c.fk_reference_id = rm.pk_ID 
				    FULL OUTER JOIN mlst st 
				        ON st.fk_isolate_id = i.pk_id 
				    FULL OUTER JOIN mlst_sequence_types mst 
				        ON st.fk_st_id = mst.pk_id
	                WHERE i.y_number = %s """

        dict_cur.execute(sql, (sample_name,))

        sample_data = dict_cur.fetchone()

        dict_cur.close()
        conn.close()

    except psycopg2.IntegrityError as e:
        print(e)

    mlst_metadata = extract_mlst_scheme_from_db(config_dict, sample_name)

    new_sample_data = sample_data.copy()

    for key in sample_data:
        if mlst_metadata.get(key):
            new = mlst_metadata.get(key)
            new_sample_data[new] = new_sample_data.pop(key)
    

    return new_sample_data

def create_xml(config_dict, sample_name, output_xml):
    sample_data = extract_data_from_db(config_dict, sample_name)

    print(sample_data)