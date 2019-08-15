import datetime
import psycopg2, psycopg2.extras

from penGU.db.NGSDatabase import NGSDatabase

def create_singleton_clustercode():
    singleton_clustercode = {"clustercode" : "S",
                             "clustercode_frequency" : "0",
                             "reference_name" : "NA",
                             "t250" : 0,
                             "t100" : 0,
                             "t50" : 0,
                             "t25" : 0,
                             "t10" : 0,
                             "t5" : 0,
                             "created_at" : datetime.datetime.now()}

    return singleton_clustercode

def update_clustercode_database(config_dict, insert_dict_list):
       
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    insert_dict_list_derep = []
    for row in insert_dict_list:
        if row["y_number"] not in row["reference_name"]:
            copy_row = dict(row)
            del copy_row['y_number']
            if copy_row not in insert_dict_list_derep:
                insert_dict_list_derep.append(copy_row)

    insert_dict_list_derep.append(create_singleton_clustercode())

    try:
        for row in insert_dict_list_derep:
            row["clustercode_updated"] = datetime.datetime.now()
                        
            ## Does snpaddress exist in DB? If yes UPDATE if no INSERT
            sql = """SELECT id,clustercode_frequency FROM clustercode_snpaddress WHERE 
                           clustercode = %(clustercode)s"""
            cur.execute(sql,(row))
            res = cur.fetchone()
            
            if res is not None:
                if res['clustercode_frequency'] is not None and row['clustercode_frequency'] is not res['clustercode_frequency']:
                    print("Updating clustercode freqency to {clustercode_frequency!s} for {clustercode}".format(**row))
                    sql = """UPDATE clustercode_snpaddress
                            SET clustercode_frequency = %(clustercode_frequency)s,
                            clustercode_updated = %(clustercode_updated)s
                            WHERE clustercode = %(clustercode)s"""
                    cur.execute(sql, (row))
                elif res['clustercode_frequency'] is not None and row['clustercode_frequency'] is res['clustercode_frequency']:
                    print("Not updating clustercode database, clustercode {clustercode} frequency ({clustercode_frequency!s}) is unchanged".format(**row))
            
            elif res is None:
                print("Adding clustercode {clustercode} to database".format(**row))
                sql = """INSERT INTO clustercode_snpaddress
                        (clustercode,
                        clustercode_frequency, 
                        reference_name, 
                        t250,
                        t100,
                        t50,
                        t25,
                        t10,
                        t5,
                        clustercode_updated)
                        VALUES (%(clustercode)s, %(clustercode_frequency)s, 
                        %(reference_name)s, %(t250)s, %(t100)s, %(t50)s, %(t25)s, 
                        %(t10)s, %(t5)s, %(clustercode_updated)s);
                        """
                cur.execute(sql, row)
        
        conn.commit()
        cur.close()
        conn.close()
 
    except psycopg2.IntegrityError as e:
           print(e)
