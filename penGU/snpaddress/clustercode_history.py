from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config

def update_clustercode_history(config_dict, updated_records):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    for row in updated_records:
        if row["old_clustercode"] is not None:
            sql = """INSERT INTO clustercode_history
                        (y_number,
                        old_clustercode, 
                        new_clustercode)
                        VALUES (%(y_number)s, %(old_clustercode)s, %(new_clustercode)s);"""
            cur.execute(sql, row)
    
    conn.commit()
    cur.close()
    conn.close()
