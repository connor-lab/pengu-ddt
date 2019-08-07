from collections import Counter
import json
import datetime
import psycopg2, psycopg2.extras
import re

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config


def retrieve_snp_addresses_from_snapperdb(snapperdb_config_dict):
    """ This function queries a snapperdb database and
    returns a nested dict of {{tXX : cluster} : frequency}"""

    NGSdb = NGSDatabase(snapperdb_config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sql = "SELECT t250,t100,t50,t25,t10,t5 FROM sample_clusters;"
    dict_cur.execute(sql)
    rec = dict_cur.fetchall()
    dict_cur.close()
    conn.close()

    snp_address_freq = dict(Counter(json.dumps(l) for l in rec))
    
    return snp_address_freq

def create_singleton_clustercode(insert_dict):
    """ Add a 000000 clustercode to the insert dict"""
    singleton_dict= {}
    for key in insert_dict:
        if re.match("^t\\d{1,3}$", key):
            singleton_dict[key] = 0
    
    singleton_dict["reference_name"] = "SINGLETON"
    singleton_dict["clustercode"] = "S"
    singleton_dict["clustercode_updated"] = datetime.datetime.now()
    singleton_dict["clustercode_frequency"] = None

    return singleton_dict

def generate_clustercode(insert_dict):
    address = []
    for i in insert_dict.keys():
        if re.match("^t\\d{1,3}$", i):
            address.append(insert_dict[i])
        
    address = '.'.join(str(a) for a in address)
    
    clustercode = insert_dict["reference_name"] + "." + address

    return clustercode

def clean_snapperdb_data(config_dict, snapperdb_config_dict, snapperdb_refgenome):
    snp_address_freq = retrieve_snp_addresses_from_snapperdb(snapperdb_config_dict)
    
    insert_dict_list = []
    
    for address,freq in snp_address_freq.items():
        
        insert_dict = json.loads(address)
        insert_dict["clustercode_frequency"] = freq
        insert_dict["reference_name"] = snapperdb_refgenome
        insert_dict["clustercode_updated"] = datetime.datetime.now()

        insert_dict["clustercode"] = generate_clustercode(insert_dict)
        
        insert_dict_list.append(insert_dict)

    insert_dict_list.append(create_singleton_clustercode(insert_dict))

    return insert_dict_list

def update_clustercode_database(config_dict, snapperdb_conf, snapperdb_refgenome):
    
    snapperdb_config_dict = check_config(snapperdb_conf, config_type="snapperdb")
    
    insert_dict_list = clean_snapperdb_data(config_dict, snapperdb_config_dict, snapperdb_refgenome)
     
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    try:
        for row in insert_dict_list:
            
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
