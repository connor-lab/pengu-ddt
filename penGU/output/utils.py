import csv
import os
import sys

from dicttoxml import dicttoxml
from xml.dom.minidom import parseString

def make_fieldnames(csv_data):
    fieldnames = [ "y_number","original_id","new_data","UPDATED","reference_name",
                   "old_clustercode","old_clustercode_frequency","old_wg_number", 
                   "old_t250","old_t100","old_t50","old_t25","old_t10","old_t5","old_t2","old_t0",
                   "new_clustercode","new_clustercode_frequency","new_wg_number","new_clustercode_updated",
                   "new_t250","new_t100","new_t50","new_t25","new_t10","new_t5","new_t2","new_t0" ]
    
    return fieldnames

def write_updated_records_to_csv(updated_records, output_csv):

    for row in updated_records:
        if not row.get("old_clustercode"):
            row["old_clustercode"] = "NA"
            

    fieldnames = make_fieldnames(updated_records)

    with open(output_csv, 'w') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        sorted_updated_records = sorted(updated_records, key=lambda x: ( x['new_clustercode'] is None, x['new_clustercode']))
        for row in sorted_updated_records:
            writer.writerow(row)

def write_all_records_to_csv(records, output_csv):
    fieldnames = make_fieldnames(records)

    with open(output_csv, 'w') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        sorted_records = sorted(records, key=lambda x: ( x['clustercode'] is None, x['clustercode']))
        for row in sorted_records:            
            writer.writerow(row)

def write_sample_xml(sample_data, output_xml):
    xml = dicttoxml({ sample_data.get('sequencing_metadata').get('y_number') : sample_data }, custom_root="ARU_WGS_TYPING", attr_type=False)

    dom = parseString(xml)

    with open(output_xml, 'w') as ox:
        ox.write(dom.toprettyxml())

def flatten_sample_dict(sample_dict):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '|' + k, v) for k, v in flatten_sample_dict(value).items() ]
        elif isinstance(value, list):
            return [ (key, ";".join(value)) ]
        else:
            return [ (key, value) ]

    items = [ item for k, v in sample_dict.items() for item in expand(k, v) ]

    return dict(items)

def write_sample_data_to_csv(sample_csv_rows, output_csv):
    data_categories = [ "sequencing_metadata", "mlst_data", "reference_picking_data", "snp_typing_data", "clustering_data" ]

    fieldnames = make_fieldnames(sample_csv_rows)

    ordered_fields = []

    for category in data_categories:
        for field in fieldnames:
            if field.startswith(category):
                ordered_fields.append(field)

    with open(output_csv, 'w') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=ordered_fields)
        writer.writeheader()
        sorted_records = sorted(sample_csv_rows, key=lambda x: ( x['sequencing_metadata|pipeline_start_date'] is None, x['sequencing_metadata|pipeline_start_date']))
        for row in sample_csv_rows:            
            writer.writerow(row)

def add_qc_pass_fail_to_sample_data(sample_data):
    distance_cutoff = 0.015
    coverage_cutoff = 30

    sample_distance = sample_data.get("reference_picking_data").get("reference_mash_distance")
    sample_coverage = sample_data.get("reference_picking_data").get("ref_coverage_mean")

    if sample_distance is not None and float(sample_distance) > distance_cutoff:
        sample_data["reference_picking_data"].update( {"reference_distance_qc_fail" : True } ) 

    if sample_coverage is not None and float(sample_coverage) < coverage_cutoff:
        sample_data["reference_picking_data"].update( {"ref_cov_qc_fail" : True } )

    return sample_data

