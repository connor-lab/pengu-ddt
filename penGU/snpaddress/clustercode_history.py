from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config

def update_clustercode_history(config_dict, updated_records):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    for row in updated_records:
        if row["old_clustercode"] is not None:
            sql = """INSERT INTO clustercode_history
                        (fk_isolate_ID,
                        fk_old_clustercode_ID, 
                        fk_new_clustercode_ID)
                        VALUES (
                            (SELECT pk_ID FROM isolate WHERE y_number = %(y_number)s), 
                            (SELECT pk_ID FROM clustercode_snpaddress WHERE clustercode = %(old_clustercode)s),
                            (SELECT pk_ID FROM clustercode_snpaddress WHERE clustercode =  %(new_clustercode)s))"""
            cur.execute(sql, row)
    
    conn.commit()
    cur.close()
    conn.close()
