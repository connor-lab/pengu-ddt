import csv
import os
import sys

def write_updated_records_to_csv(updated_records, output_csv):
    with open(output_csv, 'w') as csvfile:
        fieldnames = updated_records[0].keys()

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        sorted_updated_records = sorted(updated_records, key=lambda x: ( x['new_clustercode'] is None, x['new_clustercode']))
        for row in sorted_updated_records:
            if not row["old_clustercode"]:
                row["old_clustercode"] = "NA"
            
            writer.writerow(row)

def write_all_records_to_csv(records, output_csv):
    with open(output_csv, 'w') as csvfile:
        fieldnames = records[0].keys()

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        sorted_records = sorted(records, key=lambda x: ( x['clustercode'] is None, x['clustercode']))
        for row in sorted_records:            
            writer.writerow(row)