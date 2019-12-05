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
        if row["accession"] in row["reference_name"]:
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
        
        sample_clustercode["accession"] = sample.get("sample_name")
        sample_clustercode["clustercode"] = refname + "." + address_string
        sample_clustercode["reference_name"] = refname

        clustercode_dict_list.append(sample_clustercode)
    
    isolate_snpaddress_dict_list = get_clustercode_counts(clustercode_dict_list)

    singleton_isolate_snpaddress_dict_list = assign_singleton_clustercodes(isolate_snpaddress_dict_list)

    return singleton_isolate_snpaddress_dict_list
        

def update_isolate_clustercode_db(config_dict, refname, isolate_list_file, snapperdb_addresses):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:

        static_columns = ['accession', 'reference_name', 'original_id']

        modified_records = []


        for row in snapperdb_addresses:
            modified_data = {}
            
            if row["accession"] != row["reference_name"]:
                row["clustercode_updated"] = datetime.datetime.now()

                ## Does isolate exist in DB? If yes UPDATE if no INSERT
                sql = """SELECT i.accession,
                    i.original_ID,
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
                    cs.clustercode_frequency
                    FROM isolate i 
                    JOIN clustercode c 
                    ON i.pk_ID = c.fk_isolate_ID 
                    JOIN clustercode_snpaddress cs 
                    ON c.fk_clustercode_ID = cs.pk_ID
                    WHERE accession = %(accession)s ; """
                
                dict_cur.execute(sql, row)

                s = dict_cur.fetchone()

                if s is not None:

                    changed_columns = []

                    for key in s.keys() & row.keys():
                        if not s.get(key) == row.get(key):
                            changed_columns.append(key)
                    
                    if changed_columns:

                        changed_data = ";".join(sorted(changed_columns))

                        sql = """UPDATE clustercode SET
                                    fk_clustercode_ID = (SELECT pk_ID from clustercode_snpaddress WHERE clustercode = %(clustercode)s),
                                    t250 = %(t250)s,
                                    t100 = %(t100)s,
                                    t50 = %(t50)s,
                                    t25 = %(t25)s,
                                    t10 = %(t10)s,
                                    t5 = %(t5)s,
                                    t2 = %(t2)s,
                                    t0 = %(t0)s,
                                    clustercode_updated = %(clustercode_updated)s
                                    WHERE fk_isolate_ID = (SELECT pk_ID from isolate WHERE accession = %(accession)s) """

                        dict_cur.execute(sql, (row))

                        for column in static_columns:
                            modified_data.update( { column : row.get(column) } )

                        for k, v in s.items():
                            if k not in static_columns:
                                modified_data.update( { "old_" + k : v } )
                            
                        for k, v in row.items():
                            if k not in static_columns:
                                modified_data.update( { "new_" + k : v } )

                        print("Isolate {} has changed attributes: {}".format(row["accession"], changed_data ))


                        modified_data.update( { "UPDATED" : changed_data } )

                    else:
                        print("Not updating clustercode of isolate {} ({} == {})".format(row["accession"], s["clustercode"], row["clustercode"]))

                else:
                    print("Adding isolate {} to database with clustercode {}".format(row["accession"], row["clustercode"]))

                    sql = """INSERT INTO clustercode
                        (fk_clustercode_ID, 
                        fk_isolate_ID,
                        fk_reference_ID,
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
                            (SELECT pk_ID from isolate WHERE accession = %(accession)s),
                            (SELECT pk_ID from reference_metadata WHERE reference_name = %(reference_name)s),
                        %(t250)s, %(t100)s, %(t50)s, %(t25)s, 
                        %(t10)s, %(t5)s, %(t2)s, %(t0)s,
                        %(clustercode_updated)s); """
                        
                    dict_cur.execute(sql, row)

                    for column in static_columns:
                            modified_data.update( { column : row.get(column) } )

                    for k, v in row.items():
                            if k not in static_columns:
                                modified_data.update( { "new_" + k : v } )
            
            if modified_data:
                modified_records.append(modified_data)              
        
        conn.commit()
        dict_cur.close()
        conn.close()

        isolate_list = read_lines_from_isolate_data(isolate_list_file)

        for record in modified_records:
            if record["accession"] in isolate_list:
                record["new_data"] = "TRUE"
            else:
                record["new_data"] = None
    
        return modified_records

    except psycopg2.IntegrityError as e:
            print(e)


def get_all_clustercode_data(config_dict, records=None):

    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if records:
        try:
            all_clustercode_data = []
            for record in records:

                sql = """SELECT i.accession,
                i.original_ID,
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
                cs.clustercode_frequency,
                rm.reference_name
                FROM isolate i 
                JOIN clustercode c 
                ON i.pk_ID = c.fk_isolate_ID 
                JOIN clustercode_snpaddress cs 
                ON c.fk_clustercode_ID = cs.pk_ID
                JOIN reference_metadata rm
                ON c.fk_reference_id = rm.pk_ID 
                WHERE accession = %(accession)s"""

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

            sql = """SELECT i.accession,
                    i.original_ID,
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
                    cs.clustercode_frequency,
                    rm.reference_name
                    FROM isolate i 
                    JOIN clustercode c 
                    ON i.pk_ID = c.fk_isolate_ID 
                    JOIN clustercode_snpaddress cs 
                    ON c.fk_clustercode_ID = cs.pk_ID
                    JOIN reference_metadata rm
                    ON c.fk_reference_id = rm.pk_ID """
    
            dict_cur.execute(sql)
    
            all_clustercode_data = dict_cur.fetchall()
        
        except psycopg2.IntegrityError as e:
            print(e)

    
    dict_cur.close()
    conn.close()

    accession_regex = re.compile("^\\d{4}-\\d{6}")

    for record in all_clustercode_data:

        accession_rev = record['accession'][::-1]

        if accession_regex.match(accession_rev):
            accession_datetime = accession_rev.split("_")[0]
            record['pipeline_time'] = accession_datetime.split("-")[0][::-1]
            record['pipeline_date'] = accession_datetime.split("-")[1][::-1]
            record['accession'] = "".join(accession_rev.split("_")[1:])[::0-1]
      
    return all_clustercode_data