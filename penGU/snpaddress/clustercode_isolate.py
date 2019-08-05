import datetime
import psycopg2, psycopg2.extras
import re

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config, read_lines_from_isolate_data

def get_snpaddress_from_snapperdb(snapperdb_config_dict, isolate_list):
    """ Use isolate name to pull snpaddress and convert into string"""
    
    NGSdb = NGSDatabase(snapperdb_config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    isolate_snpaddress_dict = {}
    for isolate in isolate_list:
        sql = """SELECT t250,t100,t50,t25,t10,t5 FROM sample_clusters WHERE 
                               fk_sample_id = (
                                   SELECT pk_id FROM samples WHERE
                                   sample_name = %s ) """
        dict_cur.execute(sql, (isolate, ))

        s = dict_cur.fetchone()
        if s is None:
            print("Isolate {} not found in snapperdb database {}".format(isolate, snapperdb_config_dict["pg_dbname"]))
      
        elif s is not None:
            address = []
            for i in s.keys():
                if re.match("^t\\d{1,3}$", i):
                    address.append(s[i])
        
            address_string = ''.join(str(a) for a in address)
            isolate_snpaddress_dict[isolate] = address_string
    
    dict_cur.close()
    conn.close()
    
    return isolate_snpaddress_dict
        

def find_snpaddress_string_in_pengudb(config_dict, isolate_snpaddress_dict):
    """Is it frequency == 1, if so assign clustercode = S"""

    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    insert_dict_list = []
    for isolate,snpaddress in isolate_snpaddress_dict.items():
        insert_dict = {}
        
        sql = """SELECT clustercode_frequency, snpaddress_string, clustercode 
                FROM clustercode_snpaddress WHERE 
                snpaddress_string = %s """
        dict_cur.execute(sql, (snpaddress, ))
        
        s = dict_cur.fetchone()

        if s is None:
            print("Couldn't find snpaddress {} in database: {}". format(snpaddress, config_dict["pg_dbname"]))

        elif s is not None:

            if s["clustercode_frequency"] == 1:
                insert_dict["clustercode"] = "S"
            elif s["clustercode_frequency"] > 1:
                insert_dict["clustercode"] = s["clustercode"]

            insert_dict["clustercode_updated"] = datetime.datetime.now()
            insert_dict["y_number"] = isolate
            
            insert_dict_list.append(insert_dict)

    dict_cur.close()
    conn.close()

    return insert_dict_list
    

def update_isolate_clustercode_db(config_dict, snapperdb_conf, isolate_file):
    snapperdb_config_dict = check_config(snapperdb_conf, config_type="snapperdb")
    isolate_list = read_lines_from_isolate_data(isolate_file)
    isolate_snpaddress_dict = get_snpaddress_from_snapperdb(snapperdb_config_dict, isolate_list)
    insert_dict_list = find_snpaddress_string_in_pengudb(config_dict, isolate_snpaddress_dict)
    
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        modified_records = []
        for row in insert_dict_list:
            updated = {}
            ## Does isolate exist in DB? If yes UPDATE if no INSERT
            dict_cur.execute("""SELECT y_number,clustercode FROM clustercode WHERE 
                           y_number = %(y_number)s""", (row))
            
            s = dict_cur.fetchone()

            if s is not None:
                if s["clustercode"] != row["clustercode"]:
                    updated["y_number"] = row["y_number"]
                    updated["old_clustercode"] = s["clustercode"]
                    updated["new_clustercode"] = row["clustercode"]

                    print("Updating clustercode of isolate {} from {} to {}".format(row["y_number"], s["clustercode"], row["clustercode"]))
                    
                    sql = """UPDATE clustercode
                            SET clustercode = %(clustercode)s,
                            clustercode_updated = %(clustercode_updated)s
                            WHERE y_number = %(y_number)s"""

                    dict_cur.execute(sql, (row))

                elif s["clustercode"] == row["clustercode"]:
                    print("Not updating clustercode of isolate {} ({} == {})".format(row["y_number"], s["clustercode"], row["clustercode"]))

            if s is None:
                updated["y_number"] = row["y_number"]
                updated["old_clustercode"] = None
                updated["new_clustercode"] = row["clustercode"]

                print("Adding isolate {} to database with clustercode {}".format(row["y_number"], row["clustercode"]))
                
                sql = """INSERT INTO clustercode
                        (y_number,
                        clustercode, 
                        clustercode_updated)
                        VALUES (%(y_number)s, %(clustercode)s, %(clustercode_updated)s);"""
                
                dict_cur.execute(sql, row)
            
            if updated:
                modified_records.append(updated)
        
        conn.commit()
        dict_cur.close()
        conn.close()
    
        return modified_records

    except psycopg2.IntegrityError as e:
            print(e)