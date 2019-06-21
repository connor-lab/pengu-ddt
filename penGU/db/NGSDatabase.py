import sys
import os

import psycopg2

class NGSDatabase:
    
    path_to_config = None
    
    #pg_dbname = None
    #pg_uname = None
    #pg_pword = None
    #pg_host = None
    #pg_port = None
    #conn_string = None
    #sql_scriptfile = None

     
    def __init__(self, config_dict, sql_scriptfile=None):
        self.parse_config_dict(config_dict)

        self.sql_scriptfile = sql_scriptfile

          
# -------------------------------------------------------------------------------------------------

    def parse_config_dict(self, config_dict):
        # # we loop through thusly in case not all these things are in the config
        for attr in config_dict:
            if attr == 'pg_dbname':
                self.pg_dbname = config_dict[attr]
            if attr == 'pg_uname':
                self.pg_uname = config_dict[attr]
            if attr == 'pg_pword':
                self.pg_pword = config_dict[attr]
            if attr == 'pg_host':
                self.pg_host = config_dict[attr]
            if attr == 'pg_port':
                self.pg_port = config_dict[attr]
# -------------------------------------------------------------------------------------------------

    def _connect_to_db(self):
        self.conn_string = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
            host=self.pg_host, 
            port=self.pg_port,
            dbname=self.pg_dbname, 
            user=self.pg_uname, 
            password=self.pg_pword)
        does_db_exist = self.check_if_db_exists()
        if does_db_exist == True:
            self.db_conn = psycopg2.connect(self.conn_string)
            return self.db_conn
        else:
            print("Cant connect to DB: {}".format(self.pg_dbname))
            
# -------------------------------------------------------------------------------------------------

    def check_if_db_exists(self):
        try:
            psycopg2.connect(self.conn_string)
            return True
        except psycopg2.OperationalError:
            return False

# -------------------------------------------------------------------------------------------------
    def create_db(self, sql_scriptfile=None):
        does_db_exist = self.check_if_db_exists()
        if does_db_exist == True:
            sys.stderr.write("Database " + self.pg_dbname + " already exists\n")
        elif does_db_exist == False and self.sql_scriptfile is not None:
            if os.path.isabs(self.sql_scriptfile) == False:
                sql_script = os.path.join(self.sql_scriptfile)
            else:
                sql_script = self.sql_scriptfile
            sys.stdout.write("The database {} does not exist - creating...\n".format(self.pg_dbname))
            make_db_conn_string = "host={host} port={port} dbname=postgres user={user} password={password}".format(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_uname, 
                password=self.pg_pword)
            conn = psycopg2.connect(make_db_conn_string)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute('CREATE DATABASE {0}'.format(self.pg_dbname))
            cur.close()
            conn_string = "host={host} port={port} dbname={dbname} user={user} password={password}".format(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_dbname, 
                user=self.pg_uname, 
                password=self.pg_pword)
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
            cur.execute(open(sql_script, 'r').read())
            conn.commit()
            conn.close()
        return does_db_exist