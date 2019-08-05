import sys
import os

from .NGSDatabase import NGSDatabase

def parse_config(config, config_type):
    config_dict = {}
    try:
        if config_type is "NGS":
            config_file = config
            if os.path.isabs(config_file) == False:
                path_to_config = os.path.join(config_file)
            else:
                path_to_config = config_file
            with open(path_to_config, 'r') as cfg:
                for line in cfg.readlines():
                    if line.startswith('pg_dbname'):
                        config_dict['pg_dbname'] = line.strip().split()[-1]
                    if line.startswith('pg_uname'):
                        config_dict['pg_uname'] = line.strip().split()[-1]
                    if line.startswith('pg_pword'):
                        config_dict['pg_pword'] = line.strip().split()[-1]
                    if line.startswith('pg_host'):
                        config_dict['pg_host'] = line.strip().split()[-1]
                    if line.startswith('pg_port'):
                        config_dict['pg_port'] = line.strip().split()[-1]
        elif config_type is "snapperdb":
            config_dict_raw = dict(item.split("=") for item in config.split(" "))
            config_dict['pg_dbname'] = config_dict_raw.get('dbname') 
            config_dict['pg_uname'] = config_dict_raw.get('user')
            config_dict['pg_pword'] = config_dict_raw.get('password')
            config_dict['pg_host'] = config_dict_raw.get('host')
            config_dict['pg_port'] = config_dict_raw.get('port', '5432')
            config_dict['reference_genome'] = config_dict_raw.get('refgenome')


    except IOError:
        print("Cannot find {0}'".format(path_to_config))
        sys.exit()
    

    return config_dict

def check_or_create_db(config_dict, sql_scriptfile=None):
    if sql_scriptfile is not None:
        db = NGSDatabase(config_dict, sql_scriptfile)
        db._connect_to_db()
        db.create_db()
    else:
        db = NGSDatabase(config_dict)
        db._connect_to_db()

