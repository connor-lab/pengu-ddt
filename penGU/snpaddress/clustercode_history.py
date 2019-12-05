from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config

def update_clustercode_history(config_dict, updated_records):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    for row in updated_records:
        if row.get("old_clustercode") is not None:
            sql = """INSERT INTO clustercode_history
                        (fk_isolate_ID,
                        fk_old_clustercode_ID,
                        fk_new_clustercode_ID,
                        new_t250,
                        new_t100,
                        new_t50,
                        new_t25,
                        new_t10,
                        new_t5,
                        new_t2,
                        new_t0,
                        old_t250,
                        old_t100,
                        old_t50,
                        old_t25,
                        old_t10,
                        old_t5,
                        old_t2,
                        old_t0 )
                        VALUES (
                            (SELECT pk_ID FROM isolate WHERE accession = %(accession)s), 
                            (SELECT pk_ID FROM clustercode_snpaddress WHERE clustercode = %(old_clustercode)s),
                            (SELECT pk_ID FROM clustercode_snpaddress WHERE clustercode =  %(new_clustercode)s),
                            %(new_t250)s, %(new_t100)s, %(new_t50)s, %(new_t25)s, %(new_t10)s, %(new_t5)s, %(new_t2)s, %(new_t0)s, 
                            %(old_t250)s, %(old_t100)s, %(old_t50)s, %(old_t25)s, %(old_t10)s, %(old_t5)s, %(old_t2)s, %(old_t0)s )"""
            cur.execute(sql, row)
    
    conn.commit()
    cur.close()
    conn.close()
