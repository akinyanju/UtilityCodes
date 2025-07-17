#!/usr/bin/env python3

import argparse
import os
import duckdb
import sys

# ─────────────────────────────────────────────────────────────
# Error handling
def exit_with_error(message):
    print(f"[ERROR] {message}")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────
# Helper to read sample names from a file
def read_samples_from_file(file_path):
    with open(file_path, 'r') as f:
        samples = [line.strip() for line in f if line.strip()]
    return samples

# ─────────────────────────────────────────────────────────────
# Main logic to update records inside DuckDB
def process_duckdb(db_path, output_path, run_type, samples=None):
    if not os.path.exists(db_path):
        exit_with_error(f"DuckDB file not found: {db_path}")

    con = duckdb.connect(database=db_path, read_only=False)

    # Build sample filtering clause if needed
    sample_clause = ""
    if samples:
        sample_list = ','.join([f"'{s}'" for s in samples])
        sample_clause = f"AND Sample_Name IN ({sample_list})"

    # Preview how many rows will be affected
    preview_query = f"""
    SELECT COUNT(*) AS to_update
    FROM all_metrics
    WHERE Project_run_type = '{run_type}'
      AND ProjStatus = 'Delivered'
      {sample_clause}
    """
    to_update = con.execute(preview_query).fetchone()[0]

    if to_update == 0:
        print(f"[INFO] No rows to update for run type '{run_type}' and provided samples.")
        return

    print(f"[INFO] {to_update} rows will be updated to 'Undelivered'.")

    # Update the database
    update_query = f"""
    UPDATE all_metrics
    SET ProjStatus = 'Undelivered'
    WHERE Project_run_type = '{run_type}'
      AND ProjStatus = 'Delivered'
      {sample_clause}
    """
    con.execute(update_query)

    # Export updated rows to a TSV file for user inspection
    export_query = f"""
    COPY (
      SELECT *
      FROM all_metrics
      WHERE Project_run_type = '{run_type}'
        {sample_clause}
    ) TO '{output_path}' (DELIMITER '\t', HEADER TRUE)
    """
    con.execute(export_query)

    print("\n✅ Update complete.")
    print(f"📁 Updated rows exported to: {output_path}")
    print(f"🧪 Inspect: less -S {output_path}")
    print(f"🚀 Push if correct: rsync -vahP {db_path} ctgenometech03:/srv/shiny-server/.InputDatabase\n")

# ─────────────────────────────────────────────────────────────
# Argument parsing and execution
def main():
    parser = argparse.ArgumentParser(description='Update project delivery status to "Undelivered" in metrics.duckdb')
    parser.add_argument('--inputFile', required=True, help='Input DuckDB file (e.g., metrics.duckdb)')
    parser.add_argument('--outputFile', required=True, help='Output TSV file for inspection')
    parser.add_argument('--runType', required=True, help='Project run type (e.g., GT25-CourtoisE-84-run2)')
    parser.add_argument('--sample', help='Comma-separated sample names to update')
    parser.add_argument('--sampleFile', help='File with sample names (one per line)')

    args = parser.parse_args()

    input_directory = '/gt/data/seqdma/GTwebMetricsTables'
    output_directory = '/gt/data/seqdma/GTwebMetricsTables'

    db_path = os.path.join(input_directory, args.inputFile)
    output_file = os.path.join(output_directory, args.outputFile)

    # Collect sample list
    sample_list = []
    if args.sample:
        sample_list.extend([s.strip() for s in args.sample.split(',') if s.strip()])
    if args.sampleFile:
        if not os.path.exists(args.sampleFile):
            exit_with_error(f"Sample file not found: {args.sampleFile}")
        sample_list.extend(read_samples_from_file(args.sampleFile))

    sample_list = list(set(sample_list))  # deduplicate

    process_duckdb(db_path, output_file, args.runType, sample_list)

if __name__ == '__main__':
    main()

# import argparse
# import os

# # Helper function to check if the directory exists
# def check_directory_exists(directory):
#     if not os.path.exists(directory):
#         print(f"Error: Directory {directory} does not exist.")
#         exit(1)

# # Helper function to check if the file exists
# def check_file_exists(file_path):
#     if not os.path.exists(file_path):
#         print(f"Error: The file {file_path} does not exist.")
#         exit(1)

# # Helper to read sample names from a file
# def read_samples_from_file(file_path):
#     with open(file_path, 'r') as f:
#         samples = [line.strip() for line in f if line.strip()]
#     return samples

# # Helper function to process the file
# def process_file(input_file, output_file, run_type, samples=None):
#     with open(input_file, mode='r', newline='') as file:
#         rows = file.readlines()

#     for i, row in enumerate(rows):
#         columns = row.split('\t')
#         column_values = [col.strip() for col in columns]

#         #match_run_type = run_type in column_values
#         match_run_type = len(column_values) > 2 and column_values[2] == run_type
#         match_sample = any(sample in column_values for sample in samples) if samples else True

#         if match_run_type and match_sample and columns[-1].strip() == 'Delivered':
#             columns[-1] = 'Undelivered'
#             rows[i] = '\t'.join(columns).strip() + '\n'

#     with open(output_file, mode='w', newline='') as file:
#         file.writelines(rows)

#     print("\n")
#     print(f"INFO: Inspect the modified file {output_file}. Run below code to check it\n")
#     print(f"ACTION: <grep {run_type} {output_file} | less -S>. If all is good, then take the next actions below\n")
#     print(f"ACTION: mv {output_file} {input_file}")
#     print(f"ACTION: rsync -vahP {input_file} ctgenometech03:/srv/shiny-server/.InputDatabase")
#     print("\n")

# # Main function to set up argument parsing and control flow
# def main():
#     parser = argparse.ArgumentParser(description='Update a run project to undelivered')
#     parser.add_argument('--inputFile', required=True, help='Input file to modify e.g. hic.metrics.txt')
#     parser.add_argument('--outputFile', required=True, help='Output file e.g. hic.metrics.modified.txt')
#     parser.add_argument('--runType', required=True, help='Project run type e.g. GT25-SkarnesW-30-run2')
#     parser.add_argument('--sample', help='Comma-separated sample names whose status changes to Undelivered')
#     parser.add_argument('--sampleFile', help='File with one sample name per line whose status changes to Undelivered')

#     args = parser.parse_args()

#     input_directory = '/gt/data/seqdma/GTwebMetricsTables'
#     output_directory = '/gt/data/seqdma/GTwebMetricsTables'

#     check_directory_exists(input_directory)

#     input_file = os.path.join(input_directory, args.inputFile)
#     output_file = os.path.join(output_directory, args.outputFile)

#     check_file_exists(input_file)

#     # Collect all sample names
#     sample_list = []

#     if args.sample:
#         sample_list.extend([s.strip() for s in args.sample.split(',') if s.strip()])

#     if args.sampleFile:
#         check_file_exists(args.sampleFile)
#         sample_list.extend(read_samples_from_file(args.sampleFile))

#     # Remove duplicates
#     sample_list = list(set(sample_list))

#     process_file(input_file, output_file, args.runType, sample_list)

# if __name__ == '__main__':
#     main()
