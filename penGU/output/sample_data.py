import psycopg2, psycopg2.extras

from penGU.db.NGSDatabase import NGSDatabase

def categorise_output_data(sample_data):
    sequencing_metadata = [ 'y_number', 'sequencing_start_date', 'sequencing_instrument', 'pipeline_start_date', 'sequencing_run' ]
    reference_picking_data = [ 'reference_name', 'reference_common_kmers', 'reference_mash_distance', 'reference_mash_p_value', 'ref_coverage_mean', 'ref_coverage_stddev' ]
    snp_typing_data = [ 't250', 't100', 't50', 't25', 't10', 't5', 't2', 't0' ]
    clustering_data = [ 'clustercode', 'clustercode_frequency', 'wg_number', 'z_score_fail' ]
    
    mlst_data = [ 'st' ]
    for key in sample_data:
        if key.startswith("locus"):
            mlst_data.append(key)

    data_categories = { 'sequencing_metadata' : sequencing_metadata,
                                  'mlst_data' : mlst_data,
                     'reference_picking_data' : reference_picking_data,
                            'snp_typing_data' : snp_typing_data,
                            'clustering_data' : clustering_data }

    categorised_data = {}

    for category, keys in data_categories.items():
        categorised_data[category] = {}
        for key in keys:
            categorised_data[category].update( { key : sample_data.get(key) })
    
    return categorised_data



def extract_mlst_scheme_from_db(config_dict):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        sql = """SELECT * FROM mlst_scheme_metadata"""

        dict_cur.execute(sql)
        mlst_metadata = dict_cur.fetchone()
        dict_cur.close()
        conn.close()

    except psycopg2.IntegrityError as e:
        print(e)

    return mlst_metadata


def assign_locus_names_to_mlst(config_dict, mlst_data):
    mlst_metadata = extract_mlst_scheme_from_db(config_dict)

    new_mlst_data = mlst_data.copy()

    for key in mlst_data:
        if mlst_metadata.get(key):
            new = mlst_metadata.get(key)
            new_mlst_data[new] = new_mlst_data.pop(key)
    
    return new_mlst_data


def get_cluster_data_from_db(config_dict, wg_number):

    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        sql = """SELECT i.y_number, c.t2, c.t0 FROM isolate i
                 JOIN clustercode c
                 ON i.pk_ID = c.fk_isolate_ID
                 JOIN clustercode_snpaddress cs
                 ON c.fk_clustercode_ID = cs.pk_ID
                 JOIN reference_metadata rm
                 ON c.fk_reference_id = rm.pk_ID 
                 WHERE cs.wg_number = %s """


        dict_cur.execute(sql, (wg_number,))

        cluster_data = dict_cur.fetchall()

        conn.close()

    except psycopg2.IntegrityError as e:
        print(e)

    return cluster_data

def get_cluster_members(config_dict, wg_number, sample_name, sample_snp_type ):

    cluster_data = get_cluster_data_from_db(config_dict, wg_number)

    clustered_samples = { '0SNP' : [], '2SNP' : [], '5SNP' : [] }

    for row in cluster_data:
        if not row.get('y_number') == sample_name:
            if row.get('t2') == sample_snp_type.get('t2'):
                if row.get('t0') == sample_snp_type.get('t0'):
                    clustered_samples['0SNP'].append(row.get('y_number'))
                else:
                    clustered_samples['2SNP'].append(row.get('y_number'))
            else:
                clustered_samples['5SNP'].append(row.get('y_number'))

    return clustered_samples
   
def extract_data_from_db(config_dict, sample_name):
    NGSdb = NGSDatabase(config_dict)
    conn = NGSdb._connect_to_db()
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        sql = """SELECT i.y_number,
                 s.sequencing_run, s.sequencing_instrument, s.sequencing_start_date, s.pipeline_start_date,
                 s.ref_coverage_mean, s.ref_coverage_stddev, s.z_score_fail,
	             c.t250, c.t100, c.t50, c.t25, c.t10, c.t5, c.t2, c.t0, 
	             cs.clustercode, cs.wg_number, cs.clustercode_frequency, 
	             rm.reference_name,
                 rd.reference_mash_distance, rd.reference_mash_p_value, rd.reference_common_kmers,
	             mst.st,
	             st.locus_1, st.locus_2, st.locus_3, st.locus_4, st.locus_5, st.locus_6, st.locus_7
	             FROM isolate i 
                    FULL OUTER JOIN sequencing s
                        ON i.pk_ID = s.fk_isolate_ID
                    FULL OUTER JOIN clustercode c 
                        ON i.pk_ID = c.fk_isolate_ID 
                    FULL OUTER JOIN clustercode_snpaddress cs 
                        ON c.fk_clustercode_ID = cs.pk_ID
                    FULL OUTER JOIN reference_metadata rm
                        ON c.fk_reference_id = rm.pk_ID 
				    FULL OUTER JOIN mlst st 
				        ON st.fk_isolate_id = i.pk_id 
				    FULL OUTER JOIN mlst_sequence_types mst 
				        ON st.fk_st_id = mst.pk_id
                    FULL OUTER JOIN reference_distance rd
                        ON i.pk_id = rd.fk_isolate_id
	                WHERE i.y_number = %s 
                    ORDER BY rd.reference_mash_distance 
                    ASC LIMIT 1"""


        dict_cur.execute(sql, (sample_name,))

        sample_data = dict_cur.fetchone()

        conn.close()

    except psycopg2.IntegrityError as e:
        print(e)
    
    return sample_data

def get_all_sample_data(config_dict, sample_name):
    sample_data = dict(extract_data_from_db(config_dict, sample_name))
    
    categorised_sample_data = categorise_output_data(sample_data)

    categorised_sample_data['mlst_data'] = assign_locus_names_to_mlst(config_dict, categorised_sample_data.pop('mlst_data'))

    if not categorised_sample_data.get("clustering_data").get("clustercode") == "S":

        snp_clusters = get_cluster_members(config_dict, categorised_sample_data.get('clustering_data').get('wg_number'), sample_name, categorised_sample_data.get('snp_typing_data'))

        categorised_sample_data['clustering_data'].update( { 'cluster_members' : snp_clusters } )

    return categorised_sample_data

    