import csv
import os

from penGU.db.penguDB import parse_config

def check_config(config=None, config_type="NGS"):
    try:
        if config is not None:
            config_dict = parse_config(config, config_type)
            return config_dict
    except:
        print("Please provide a working config file")

def read_data_from_csv(csv_file, header=None, **kwargs):
    """
    Returns a list of dicts for each row of a csv file
    """
    if os.path.isabs(csv_file) == False:
        path_to_csv = os.path.join(csv_file)
    else:
        path_to_csv = csv_file
    row_list = []
    if "field_sep" not in kwargs.keys():
        field_sep = ','
    else:
        field_sep = kwargs.get("field_sep")
    with open(path_to_csv, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=field_sep, fieldnames=header)
        for record in csv_reader:
            if list(record.values())[0].startswith("#") is not True:
                # IT'S A COMMENT IF IT STARTS WITH "#" 
                # IF THIS IS YOUR HEADER ROW, SUPPLY A LIST OF COLUMN NAMES WHEN CALLING THE FUNCTION
                row_list.append(record)
    return row_list

def read_lines_from_isolate_data(isolate_list_file):
    if os.path.isabs(isolate_list_file) == False:
        path_to_file = os.path.join(isolate_list_file)
    else:
        path_to_file = isolate_list_file
    
    isolate_list = []
    with open(path_to_file, mode='r') as text_file:
        isolate_list = text_file.read().splitlines()
        #isolate_list.append(isolate)
    
    return isolate_list


def string_to_bool(s):
    trues = ("yes", "true", "1")
    return s.lower() in trues
