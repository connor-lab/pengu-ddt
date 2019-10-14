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
                sql = """SELECT isolate.y_number,
                      clustercode_snpaddress.clustercode,
                      clustercode_snpaddress.wg_number 
                      FROM isolate,clustercode_snpaddress WHERE
                      clustercode_snpaddress.pk_ID = (
                      SELECT fk_clustercode_ID FROM clustercode WHERE fk_isolate_ID = (
                      SELECT pk_ID FROM isolate WHERE y_number = %(y_number)s))"""
                dict_cur.execute(sql, row)

                s = dict_cur.fetchone()

                if s is not None:
                    
                    if s["clustercode"] != row["clustercode"]:
                        updated["y_number"] = row["y_number"]
                        updated["old_clustercode"] = s["clustercode"]
                        updated["new_clustercode"] = row["clustercode"]
                        updated["new_clustercode_frequency"] = row["clustercode_frequency"]

                        print("Updating clustercode of isolate {} from {} to {}".format(row["y_number"], s["clustercode"], row["clustercode"]))
                    
                        sql = """UPDATE clustercode
                                SET fk_clustercode_ID = (
                                    SELECT pk_ID FROM clustercode_snpaddress WHERE clustercode = %(clustercode)s),
                                clustercode_updated = %(clustercode_updated)s
                                WHERE fk_isolate_ID = (SELECT pk_ID FROM isolate WHERE y_number = %(y_number)s)"""

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
                        (fk_clustercode_ID, 
                        fk_isolate_ID,
                        t250,
                        t100,
                        t50,
                        t25,
                        t10,
                        t5,
                        t2,
                        t0,
                        clustercode_updated)
                        VALUES (
                            (SELECT pk_ID from clustercode_snpaddress WHERE clustercode = %(clustercode)s),
                            (SELECT pk_ID from isolate WHERE y_number = %(y_number)s),
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


def get_all_clustercode_data(config_dict, records=None):

    y_number_regex = re.compile("^\\d{4}-\\d{6}")

    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if records:
        try:
            all_clustercode_data = []
            for record in records:

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
                cs.clustercode FROM isolate i 
                JOIN clustercode c 
                ON i.pk_ID = c.fk_isolate_ID 
                JOIN clustercode_snpaddress cs 
                ON c.fk_clustercode_ID = cs.pk_ID 
                WHERE y_number = %(y_number)s"""

                dict_cur.execute(sql, record)

                s = dict_cur.fetchone()

                s['old_clustercode'] = record['old_clustercode']
                s['new_clustercode'] = s.pop('clustercode')
                s['new_data'] = record['new_data']

                all_clustercode_data.append(s)

        except psycopg2.IntegrityError as e:
            print(e)

    else:
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
                    cs.reference_name FROM isolate i 
                    JOIN clustercode c 
                    ON i.pk_ID = c.fk_isolate_ID 
                    JOIN clustercode_snpaddress cs 
                    ON c.fk_clustercode_ID = cs.pk_ID """
    
            dict_cur.execute(sql)
    
            all_clustercode_data = dict_cur.fetchall()
        
        except psycopg2.IntegrityError as e:
            print(e)

    
    dict_cur.close()
    conn.close()

    for record in all_clustercode_data:

        y_number_rev = record['y_number'][::-1]

        if y_number_regex.match(y_number_rev):
            y_number_datetime = y_number_rev.split("_")[0]
            record['pipeline_time'] = y_number_datetime.split("-")[0][::-1]
            record['pipeline_date'] = datetime.datetime.strptime(y_number_datetime.split("-")[1][::-1], "%y%m%d").strftime("%Y-%m-%d")
            record['y_number'] = "".join(y_number_rev.split("_")[1:])[::0-1]
    


    
    return all_clustercode_data

    