from collections import Counter
import datetime
import psycopg2, psycopg2.extras
import re

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config, read_lines_from_isolate_data
from penGU.snpaddress.clustercode_database import update_clustercode_database

def filter_snpaddress_levels(level, level_list):
    if level in level_list:
        return True
    else:
        return False


def get_clustercode_counts(clustercode_dict_list):
    
    clustercodes = []
    for row in clustercode_dict_list:
        if row["y_number"] in row["reference_name"]:
            row["clustercode"] = "NA"
        clustercodes.append(row['clustercode'])

    clustercode_counts = Counter(clustercodes)

    sample_clustercode_counts = []
    for row in clustercode_dict_list:
        row['clustercode_frequency'] = clustercode_counts.get(row['clustercode'])
        sample_clustercode_counts.append(row)

    return sample_clustercode_counts

def assign_singleton_clustercodes(clustercode_dict_list):

    singleton_clustercode_dict_list = []

    for row in clustercode_dict_list:
        if row['clustercode_frequency'] == 1 and "NA" not in row['clustercode']:
            row['clustercode'] = "S"
        #elif "NA" in row['clustercode']:
        #    row['clustercode'] = "NA" 
        singleton_clustercode_dict_list.append(row)

    return singleton_clustercode_dict_list


def get_snpaddresses_from_snapperdb(snapperdb_config, refname):
    
    NGSdb = NGSDatabase(snapperdb_config)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = """SELECT sample_name,t250,t100,t50,t25,t10,t5,t2,t0 FROM samples, sample_clusters WHERE samples.pk_id = sample_clusters.fk_sample_id"""
    dict_cur.execute(sql)
    db_snpaddress = dict_cur.fetchall()
        
    dict_cur.close()
    conn.close()

    
    clustercode_dict_list = []
    for sample in db_snpaddress:
        sample_clustercode = {} 
        address = []
        for level,value in sample.items():
            if re.match("^t\\d{1,3}$", level):
                sample_clustercode[level] = value
                address.append(sample[level])
                
                if filter_snpaddress_levels(level, ['t250', 't100', 't50', 't25', 't10', 't5']):
                    address_string = '.'.join(str(a) for a in address)
        
        sample_clustercode["y_number"] = sample.get("sample_name")
        sample_clustercode["clustercode"] = refname + "." + address_string
        sample_clustercode["reference_name"] = refname

        clustercode_dict_list.append(sample_clustercode)
    
    isolate_snpaddress_dict_list = get_clustercode_counts(clustercode_dict_list)

    singleton_isolate_snpaddress_dict_list = assign_singleton_clustercodes(isolate_snpaddress_dict_list)

    return singleton_isolate_snpaddress_dict_list
        

def update_isolate_clustercode_db(config_dict, refname, isolate_list_file, snapperdb_addresses):
    #snapperdb_config = check_config(snapperdb_conf, config_type="snapperdb")
    #isolate_snpaddress_dict_list = get_snpaddresses_from_snapperdb(snapperdb_config, config_dict, refname)
    #print(isolate_snpaddress_dict_list)
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        modified_records = []
        for row in snapperdb_addresses:
            
            if row["y_number"] != row["reference_name"]:
                row["clustercode_updated"] = datetime.datetime.now()
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
                        updated["new_clustercode_frequency"] = row["clustercode_frequency"]

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
                    updated["new_clustercode_frequency"] = row["clustercode_frequency"]

                    print("Adding isolate {} to database with clustercode {}".format(row["y_number"], row["clustercode"]))
                
                    sql = """INSERT INTO clustercode
                        (clustercode, 
                        y_number,
                        t250,
                        t100,
                        t50,
                        t25,
                        t10,
                        t5,
                        t2,
                        t0,
                        clustercode_updated)
                        VALUES (%(clustercode)s, %(y_number)s,
                        %(t250)s, %(t100)s, %(t50)s, %(t25)s, 
                        %(t10)s, %(t5)s, %(t2)s, %(t0)s,
                        %(clustercode_updated)s);
                        """
                    dict_cur.execute(sql, row)

                if updated:
                    modified_records.append(updated)              
        
        conn.commit()
        dict_cur.close()
        conn.close()

        isolate_list = read_lines_from_isolate_data(isolate_list_file)

        for record in modified_records:
            if record["y_number"] in isolate_list:
                record["new_data"] = "TRUE"
            else:
                record["new_data"] = None
    
        return modified_records

    except psycopg2.IntegrityError as e:
            print(e)
