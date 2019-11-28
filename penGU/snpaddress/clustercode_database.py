import datetime
import psycopg2, psycopg2.extras

from penGU.db.NGSDatabase import NGSDatabase

def get_current_year(config_dict):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    sql = """SELECT wg_number FROM clustercode_snpaddress ORDER BY pk_ID DESC LIMIT 1"""
    dict_cur.execute(sql)
    last_record = dict_cur.fetchone()
    dict_cur.close()
    conn.close()

    if last_record: 
        wg_year = last_record.get('wg_number').split("-")[0].replace("WG", "")
        wg_id = last_record.get('wg_number').split("-")[1]
    else:
        wg_year = datetime.datetime.now().year
        wg_id = 0

    return wg_id, wg_year
 

def create_singleton_clustercode():
    singleton_clustercode = {"clustercode" : "S",
                             "clustercode_frequency" : "0",
                             "reference_name" : "NA"}

    return singleton_clustercode

def update_clustercode_database(config_dict, insert_dict_list):
    
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    insert_dict_list_derep = []
    for row in insert_dict_list:
        if row["clustercode_frequency"] > 1:
            clustercode_row = { "clustercode" : row["clustercode"], 
                                "clustercode_frequency" : row["clustercode_frequency"],
                                "reference_name" : row["reference_name"] }
            if clustercode_row not in insert_dict_list_derep:
                insert_dict_list_derep.append(clustercode_row)
        else:
            insert_dict_list_derep.append({ "clustercode" : "S", 
                                            "clustercode_frequency" : 0,
                                            "reference_name" : "NA" })
    
    try:
        for row in insert_dict_list_derep:

            row_id, current_year = get_current_year(config_dict)

            if row["clustercode"] is "S":
                row["wg_number"] = "WG19-00000"

            elif int(current_year) < int(datetime.datetime.now().year):
                year = datetime.datetime.now().year
                wg_id = 1
                row['wg_number'] = "WG" +year+ "-" +'{:05d}'.format(wg_id)

            else:
                year = str(current_year)
                wg_id = int(row_id) + 1
                row['wg_number'] = "WG" +year+ "-" +'{:05d}'.format(wg_id)

            
            row["clustercode_updated"] = datetime.datetime.now()
                        
            ## Does snpaddress exist in DB? If yes UPDATE if no INSERT
            sql = """SELECT pk_ID, clustercode_frequency FROM clustercode_snpaddress WHERE 
                           clustercode = %(clustercode)s"""
            cur.execute(sql,(row))
            res = cur.fetchone()
           
            
            if res is not None:
                if res['clustercode_frequency'] is not None and row['clustercode_frequency'] is not res['clustercode_frequency']:
                    print("Updating clustercode freqency to {clustercode_frequency!s} for {clustercode}".format(**row))
                    sql = """UPDATE clustercode_snpaddress
                            SET clustercode_frequency = %(clustercode_frequency)s, updated_at = %(clustercode_updated)s
                            WHERE clustercode = %(clustercode)s"""
                    cur.execute(sql, (row))
                elif res['clustercode_frequency'] is not None and row['clustercode_frequency'] is res['clustercode_frequency']:
                    print("Not updating clustercode database, clustercode {clustercode} frequency ({clustercode_frequency!s}) is unchanged".format(**row))
            
            elif res is None:
                print("Adding clustercode {clustercode} | {wg_number} to database".format(**row))
                sql = """INSERT INTO clustercode_snpaddress
                        (clustercode,
                        clustercode_frequency,
                        wg_number,
                        updated_at)
                        VALUES (%(clustercode)s, %(clustercode_frequency)s, %(wg_number)s, %(clustercode_updated)s);
                        """
                cur.execute(sql, row)
      
            conn.commit()
        
        cur.close()
        conn.close()
 
    except psycopg2.IntegrityError as e:
           print(e)
