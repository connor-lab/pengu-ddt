from collections import Counter
import json
import datetime
import psycopg2, psycopg2.extras
import re

from penGU.db.NGSDatabase import NGSDatabase
from penGU.input.utils import check_config_file


def retrieve_snp_addresses_from_snapperdb(snapperdb_config_dict):
    """ This function queries a snapperdb database and
    returns a dict of {{tXX : cluster} : frequency} to 
    be filtered for singletons later"""

    NGSdb = NGSDatabase(snapperdb_config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dict_cur.execute("""SELECT t250,t100,t50,t25,t10,t5,t2,t0 FROM strain_clusters;""")
    rec = dict_cur.fetchall()
    dict_cur.close()
    conn.close()

    snp_address_freq = dict(Counter(json.dumps(l) for l in rec))
    
    return snp_address_freq

def clean_snapperdb_data(config_dict, snapperdb_config_dict):
    snp_address_freq = retrieve_snp_addresses_from_snapperdb(snapperdb_config_dict)
    insert_dict_list = []
    for address,freq in snp_address_freq.items():
        insert_dict = json.loads(address)
        insert_dict["clustercode_frequency"] = freq
        insert_dict["reference_name"] = snapperdb_config_dict["reference_genome"]
        insert_dict["clustercode_updated"] = datetime.datetime.now()

        address = []
        for i in insert_dict.keys():
            if re.match("^t\\d{1,3}$", i):
                address.append(insert_dict[i])
        
        address = '.'.join(str(a) for a in address)
        today = "{:%Y-%m-%d}".format(datetime.datetime.now())
        clustercode = today + "." + insert_dict["reference_name"] + "." + address

        insert_dict["clustercode"] = clustercode
        insert_dict["snpaddress_string"] = address.replace(".","")
        insert_dict_list.append(insert_dict)

    return insert_dict_list

def update_clustercode_database(config_dict, snapperdb_conf):
    snapperdb_config_dict = check_config_file(snapperdb_conf, config_type="snapperdb")
    insert_dict_list = clean_snapperdb_data(config_dict, snapperdb_config_dict)
 
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    try:
        for row in insert_dict_list:
            cur.execute("""INSERT INTO clustercode_snpaddress
                       (clustercode,
                       clustercode_frequency, 
                       reference_name, 
                       t250,
                       t100,
                       t50,
                       t25,
                       t10,
                       t5,
                       t2,
                       t0,
                       snpaddress_string,
                       clustercode_updated)
                       VALUES (%(clustercode)s, %(clustercode_frequency)s, 
                       %(reference_name)s, %(t250)s, %(t100)s, %(t50)s, %(t25)s, 
                       %(t10)s, %(t5)s, %(t2)s, %(t0)s, %(snpaddress_string)s, 
                       %(clustercode_updated)s);
                    """, row)
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.IntegrityError as e:
            print(e)
