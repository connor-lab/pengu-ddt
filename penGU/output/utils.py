import csv
import os
import sys

def write_updated_records_to_csv(updated_records, output_csv):
    with open(output_csv, 'w') as csvfile:
        fieldnames = updated_records[0].keys()

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in updated_records:
            writer.writerow(row)
            