import datetime
import requests

from penGU.db.NGSDatabase import NGSDatabase

def get_or_update_pubmlst_url_from_db(config_dict, mlst_scheme_name, pubmlst_url):
    print("Synchronising local MLST reference database with PubMLST\n")
    ## Check if URL is already in the database
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    sql = """SELECT pubmlst_url 
            FROM mlst_scheme_metadata 
            WHERE pubmlst_name 
            LIKE (%s);"""

    cur.execute(sql, (mlst_scheme_name,))
    
    pubmlst_url_from_db = cur.fetchone()
    
    if pubmlst_url_from_db is None and pubmlst_url is None:
        print("NO DATABASE OR PROVIDED URL FOR SEQUENCE TYPES - FAILED TO UPDATE MLST DATABASE")

    if pubmlst_url_from_db is None and pubmlst_url is not None:
        print("NO DATABASE URL - ADDING PROVIDED URL TO DATABASE")
        time_now = datetime.datetime.now()

        sql = """INSERT INTO mlst_scheme_metadata 
                       (pubmlst_url, pubmlst_updated_at, pubmlst_name) 
                       VALUES (%s, %s, %s);
                       """

        cur.execute(sql, (pubmlst_url, time_now, mlst_scheme_name))

    elif pubmlst_url is None and pubmlst_url_from_db is not None:
        print("NO URL PROVIDED - USING DATABASE URL")
        pubmlst_url = pubmlst_url_from_db

    elif pubmlst_url_from_db[0] != pubmlst_url:
        time_now = datetime.datetime.now()

        sql = """UPDATE mlst_scheme_metadata 
                SET pubmlst_url = (%s), pubmlst_updated_at = (%s) 
                WHERE pubmlst_name like (%s);"""

        cur.execute(sql, (pubmlst_url, time_now, mlst_scheme_name))
        print("PROVIDED URL DIFFERENT TO DATABASE URL - UPDATING DATABASE URL")


    elif pubmlst_url_from_db[0] == pubmlst_url:
        print("PROVIDED URL IS IDENTICAL TO DATABASE URL")

    conn.commit()
    cur.close()
    conn.close()

    return pubmlst_url

    
def download_mlst_refdb(config_dict, mlst_scheme_name, pubmlst_url=None):
    pubmlst_url = get_or_update_pubmlst_url_from_db(config_dict, mlst_scheme_name, pubmlst_url)
    mlst_db = requests.get(pubmlst_url).text

    STs = mlst_db.splitlines()
    header = STs[0].split()[0:8]
    ST_list = []
    for line in STs[1:]:
        ST_dict = dict(zip(header, line.split()[0:8]))
        ST_list.append(ST_dict)
    try:
        if "ST" in list(ST_dict.keys())[0]:
            return ST_list
    except IndexError:
        print("Is this definitely PubMLST sequence type data?")
        exit(1)
    
def update_mlst_metadata(config_dict, mlst_scheme_name, pubmlst_url=None):
    ST_list = download_mlst_refdb(config_dict, mlst_scheme_name, pubmlst_url)
    gene_list = list(ST_list[0])[1:]
    locus_gene_dict = {}
    for locus, gene in enumerate(gene_list, 1):
        locus_number = "locus_" + str(locus)
        locus_gene_dict[locus_number] = gene
    locus_gene_dict["mlst_scheme_name"] = mlst_scheme_name
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    sql = """UPDATE mlst_scheme_metadata
                   SET locus_1 = %(locus_1)s, 
                       locus_2 = %(locus_2)s,
                       locus_3 = %(locus_3)s,
                       locus_4 = %(locus_4)s,
                       locus_5 = %(locus_5)s,
                       locus_6 = %(locus_6)s,
                       locus_7 = %(locus_7)s
                    WHERE pubmlst_name LIKE %(mlst_scheme_name)s;
                    """

    cur.execute(sql, locus_gene_dict)

    conn.commit()
    cur.close()
    conn.close()
    return ST_list

def generate_mlst_refdb(config_dict, mlst_scheme_name, pubmlst_url=None):
    ST_list = update_mlst_metadata(config_dict, mlst_scheme_name, pubmlst_url)
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()

    # Make sure we have a NULL-type ST in the database
    ST_list.append(dict.fromkeys(ST_list[0].keys(), "-"))
    
    for ST_row in ST_list:
        row = list(ST_row.values())

        sql = """SELECT exists
                (SELECT 1 FROM mlst_sequence_types
                WHERE st = %s)"""
        
        cur.execute(sql, (row[0],))

        ST_exists = cur.fetchone()[0]
        
        if ST_exists is False:

            sql = """INSERT INTO mlst_sequence_types 
                       (ST, locus_1, locus_2, locus_3, locus_4, locus_5, locus_6, locus_7) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                       """
            
            cur.execute(sql, (row))
    
    conn.commit()
    cur.close()
    conn.close()
