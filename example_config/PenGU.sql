create table isolate(
	pk_ID serial primary key not null,
	y_number text unique not null,
    original_ID text unique not null,
	episode_number text
    );

create table sequencing(
	pk_ID SERIAL primary key not null,
	fk_isolate_ID int not null references isolate(pk_ID),
	sequencing_instrument text,
	sequencing_run text,
	sequencing_start_date date,
    sequencing_end_date date,
    calculated_magnitude int,
    trimmed_readlength decimal(5, 2),
    mean_insert_size int,
	qc_pass bool,
    created_at timestamp default current_timestamp
    );

create table mlst_scheme_metadata(
    pk_ID SERIAL primary key not null,
    pubmlst_name text unique not null,
    pubmlst_url text unique not null,
    pubmlst_updated_at TIMESTAMP not null,
    locus_1 text,
    locus_2 text,
    locus_3 text,
    locus_4 text,
    locus_5 text,
    locus_6 text,
    locus_7 text
    );
    
create table mlst_sequence_types(
    pk_ID SERIAL primary key not null,
    ST text unique not null,
    locus_1 text,
    locus_2 text,
    locus_3 text,
    locus_4 text,
    locus_5 text,
    locus_6 text,
    locus_7 text
    );

create table mlst(
    pk_ID SERIAL primary key not null,
    fk_isolate_ID int not null references isolate(pk_ID),
    fk_ST_ID int not null references mlst_sequence_types(pk_ID),
    locus_1 text,
    locus_2 text,
    locus_3 text,
    locus_4 text,
    locus_5 text,
    locus_6 text,
    locus_7 text,
    created_at timestamp default current_timestamp
    );

create table reference_metadata(
    pk_ID SERIAL primary key not null,
    reference_name text unique not null,
    snapperdb_db_name text unique not null,
    ncbi_accession text,
    genome_filename text unique not null,
    created_at timestamp default current_timestamp
    );

create table reference_distance(
    pk_ID SERIAL primary key not null,
    fk_isolate_ID int not null references isolate(pk_ID),
    fk_reference_ID int not null references reference_metadata(pk_ID),
    reference_mash_distance decimal(11, 10),
    reference_mash_p_value text not null,
    reference_common_kmers text not null,
    created_at timestamp default current_timestamp
    );

create table clustercode_snpaddress(
    pk_ID SERIAL primary key not null,
    clustercode text UNIQUE not null,
    wg_number text UNIQUE not null,
    clustercode_frequency int not null,
    updated_at timestamp not null,
    created_at timestamp default current_timestamp
    );

create table clustercode(
    pk_ID SERIAL primary key not null,
    fk_isolate_ID int not null references isolate(pk_ID),
    fk_reference_ID int not null references reference_metadata(pk_ID),
    t250 int not null,
    t100 int not null,
    t50 int not null,
    t25 int not null,
    t10 int not null,
    t5 int not null,
    t2 int not null,
    t0 int not null,
    fk_clustercode_ID int not null references clustercode_snpaddress(pk_ID),
    created_at timestamp default current_timestamp,
    clustercode_updated timestamp not null
    );

create table clustercode_history(
    pk_ID SERIAL primary key not null,
    fk_isolate_ID int not null references isolate(pk_ID),
    fk_old_clustercode_ID int not null references clustercode_snpaddress(pk_ID),
    fk_new_clustercode_ID int not null references clustercode_snpaddress(pk_ID),
    created_at timestamp default current_timestamp
    );



create table ribotype_metadata(
    pk_ID SERIAL primary key not null,
    ribotype_reference_name text unique not null,
    ribotype text not null
    );

create table ribotype_distance(
    pk_ID SERIAL primary key not null,
    fk_isolate_ID int not null references isolate(pk_ID),
    fk_ribotype_reference_ID int not null references ribotype_metadata(pk_ID),
    ribotype text not null,
    ribotype_mash_distance decimal(11, 10)
    );

create table toxinotype_db_tcda(
    pk_ID SERIAL primary key not null,
    sequence text unique not null
    );

create table toxinotype_db_tcdb(
    pk_ID SERIAL primary key not null,
    sequence text unique not null
    );

create table toxinotype_db_toxa(
    pk_ID SERIAL primary key not null,
    sequence text unique not null
    );

create table toxinotype_db_toxb(
    pk_ID SERIAL primary key not null,
    sequence text unique not null
    );

create table toxinotype(
    pk_ID SERIAL primary key not null,
    fk_isolate_ID int not null references isolate(pk_ID),
    fk_tox_a_ID int references toxinotype_db_toxa(pk_ID),
    fk_tox_b_ID int references toxinotype_db_toxb(pk_ID),
    fk_tcd_a_ID int references toxinotype_db_tcda(pk_ID),
    fk_tcd_b_ID int references toxinotype_db_tcdb(pk_ID)
    );
