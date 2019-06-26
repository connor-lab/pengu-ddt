import psycopg2

from penGU.db.NGSDatabase import NGSDatabase

from penGU.input.utils import read_data_from_csv

def update_distance_refdb(config_dict, dist_csv):
    dist_ref_data = read_data_from_csv(dist_csv)
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    cur = conn.cursor()
    
    try:
        for row in dist_ref_data:

            sql = """INSERT into reference_metadata
                          (reference_name,
                          snapperdb_db_name,
                          ncbi_accession,
                          genome_filename)
                          VALUES
                          (%(reference_name)s,
                          %(snapperdb_db_name)s,
                          %(ncbi_accession)s,
                          %(genome_filename)s)"""

            cur.execute(sql, (row))

        conn.commit()
        cur.close()
        conn.close()
        
    except psycopg2.IntegrityError as e:
        print(e)


        

