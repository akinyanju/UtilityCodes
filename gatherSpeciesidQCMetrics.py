#!/usr/bin/env python3

import os
import re
import pandas as pd
from glob import glob
import subprocess
from datetime import datetime, timedelta

# --- Configuration ---
Application = "speciesid"
OUT = f"/gt/data/seqdma/GTwebMetricsTables"
QCDirFileSuccess = os.path.join(OUT, f".{Application}.QCDir.update.txt") # File to log successfully processed directories
QCDirFileFail = os.path.join(OUT, f".{Application}.QCDir.ToCollectQC.txt") # File to log successfully processed directories
report_pattern = f"*QCreport.{Application}.csv"
search_dirs_first = ["/gt/data/seqdma/qifa", "/gt/data/seqdma/.qifa.qc-archive"] # Directories to search on the first run
search_dirs_next = ["/gt/data/seqdma/qifa"] # Directories to search on subsequent runs
# Ensure the output directory exists
os.makedirs(OUT, exist_ok=True)

# --- Utility Functions ---
# Read previously processed directory paths from a text file
def read_previous_paths(file):
    if os.path.exists(file):
        with open(file) as f:
            return set(line.strip() for line in f)
    return set()

# Append new paths to the log file if they haven’t been written before
def write_paths(file, paths, already_written):
    new_paths = [p for p in paths if p not in already_written]
    if new_paths:
        with open(file, 'a') as f:
            for path in new_paths:
                f.write(path + "\n")
    return already_written.union(new_paths)

# Extract metadata from a .settings.json file in each folder
def extract_metadata_from_setting(setting_file):
    try:
        with open(setting_file) as f:
            content = f.read()
            delivery_folder = re.search(r'"deliveryFolder":\s*"([^"]+)"', content)
            project_final = re.search(r'"projectFinal":\s*"([^"]+)"', content)
            release_date = re.search(r'"releaseDate":\s*"([^"]+)"', content)

            delivery_folder_val = delivery_folder.group(1).split("/")[-2] if delivery_folder else None
            project_final_val = project_final.group(1) if project_final else None
            release_date_val = release_date.group(1) if release_date else None

            if delivery_folder_val and project_final_val and release_date_val:
                return delivery_folder_val, project_final_val, release_date_val
    except Exception as e:
        print(f"Error reading setting file {setting_file}: {e}")
    return None, None, None

# Extract the flowcell ID from RunInfo.xml or fallback to Run_Metric_Summary.draft.csv
def extract_flowcell_id(folder):
    runinfo_path = os.path.join(folder, "RunInfo.xml")
    flowcell_csv = os.path.join(folder, "Run_Metric_Summary.draft.csv")

    if os.path.exists(runinfo_path):
        try:
            with open(runinfo_path) as f:
                content = f.read()
                match = re.search(r"<Flowcell>([^<]+)</Flowcell>", content)
                if match:
                    return match.group(1)
        except Exception as e:
            print(f"Error reading RunInfo.xml in {folder}: {e}")

    if os.path.exists(flowcell_csv):
        try:
            # Read lines and find header row dynamically
            with open(flowcell_csv, 'r') as f:
                lines = f.readlines()
                
            header_idx = None
            for i, line in enumerate(lines):
                if "MachineID" in line and "FlowCellID" in line:
                    header_idx = i
                    break

            if header_idx is None:
                print(f"Could not find FlowCellID header in {flowcell_csv}")
                return None

            # Read using csv.DictReader from header row
            import csv
            from io import StringIO

            csv_data = "".join(lines[header_idx:])
            reader = csv.DictReader(StringIO(csv_data))

            for row in reader:
                flowcell_id = row.get("FlowCellID", "").strip()
                if flowcell_id and flowcell_id != "!!!TBD!!!":
                    return flowcell_id
                break  # Only use the first row of data

        except Exception as e:
            print(f"Error reading FlowCellID from CSV in {folder}: {e}")

    return None

# Clean and parse the QC metrics CSV starting from the correct row
def extract_clean_metrics(report_file):
    with open(report_file) as f:
        lines = f.readlines()
        start_idx = next(i for i, line in enumerate(lines) if "GT_QC_Sample_ID" in line)
    return pd.read_csv(report_file, skiprows=start_idx)

# Push the final metrics CSV to the remote shiny server
def push_to_server(file_path):
    dest = f"ctgenometech03:/srv/shiny-server/.InputDatabase"
    try:
        print(f"\n Syncing {file_path} to {dest}")
        subprocess.run(["rsync", "-vahP", file_path, dest], check=True)
        print("Sync complete.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Rsync failed: {e}")
        return False

# Manage backup copies of the metrics file and cleanup old logs
def manage_backups(metrics_file, updated):
    backup_dir = os.path.join(OUT, ".GTmetricsbackup")
    log_dir = os.path.join(OUT, ".slurmlog")
    os.makedirs(backup_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    day_stamp = datetime.today().strftime("%Y%m%d")
    backup_file = os.path.join(backup_dir, f".{Application}.metrics.{day_stamp}.txt")

    if updated:
        subprocess.run(["cp", metrics_file, backup_file])
        print(f"🗃️  Backup created: {backup_file}")
        subprocess.run(["find", backup_dir, "-type", "f", "-mtime", "+10", "-delete"])
        subprocess.run(["find", log_dir, "-type", "f", "-mtime", "+1", "-delete"])
    elif not os.path.exists(backup_file):
        open(backup_file, 'a').close()
        print(f"Empty backup placeholder touched: {backup_file}")

# --- Main Script ---

# Read previously processed paths
processed_success = read_previous_paths(QCDirFileSuccess)
processed_fail = read_previous_paths(QCDirFileFail)

# Use full archive for the first run; otherwise, only look in active dir
search_dirs = search_dirs_first if not processed_success else search_dirs_next

# Scan directories for candidate report files
all_dirs_to_process = set()
print("🔍 Scanning for QC report files...")
for base_dir in search_dirs:
    for path in glob(f"{base_dir}/**/{report_pattern}", recursive=True):
        folder_path = os.path.dirname(path)
        if folder_path not in processed_success:
            all_dirs_to_process.add(folder_path)

print(f"Found {len(all_dirs_to_process)} candidate folders to process.")

# Initialize output containers
final_dfs = []
success_paths = []
fail_paths = []

# Process each candidate folder
for folder in sorted(all_dirs_to_process):
    print(f"\nProcessing folder: {folder}")
    setting_file = os.path.join(folder, ".settings.json")

    # Validate metadata presence
    if not os.path.exists(setting_file):
        print(f"Missing .settings.json file: {setting_file}")
        fail_paths.append(folder)
        continue

    delivery, project_final, release = extract_metadata_from_setting(setting_file)
    if not (delivery and project_final and release):
        print(f"Incomplete metadata: delivery={delivery}, project={project_final}, release={release}")
        fail_paths.append(folder)
        continue

    # Validate report file
    report_file = os.path.join(folder, f"{project_final}_QCreport.{Application}.csv")
    if not os.path.exists(report_file):
        print(f" Missing report file: {report_file}")
        fail_paths.append(folder)
        continue

    # Validate Flowcell ID
    flowcell = extract_flowcell_id(folder)
    if not flowcell:
        print(" Could not determine FlowcellID.")
        fail_paths.append(folder)
        continue

    # Extract and clean the metrics
    try:
        df = extract_clean_metrics(report_file)
        df.insert(0, "Investigator_Folder", delivery)
        df.insert(1, "Project_run_type", project_final)
        df.insert(2, "FlowcellID", flowcell)
        final_dfs.append(df)
        success_paths.append(folder)
        print("✅ Successfully processed.")
    except Exception as e:
        print(f"Failed to parse metrics: {e}")
        fail_paths.append(folder)

# Merge and deduplicate new metrics with existing ones
updated = False
if final_dfs:
    full_df = pd.concat(final_dfs, ignore_index=True)
    metrics_output_file = os.path.join(OUT, f"{Application}.metrics.csv")

    if os.path.exists(metrics_output_file):
        existing_df = pd.read_csv(metrics_output_file)
        combined_df = pd.concat([existing_df, full_df], ignore_index=True)
        combined_df.drop_duplicates(inplace=True)

        if len(combined_df) > len(existing_df):
            combined_df.to_csv(metrics_output_file, index=False)
            updated = True
            print(f"\n Metrics updated: {len(combined_df) - len(existing_df)} new rows added.")
        else:
            print(f"\n No new unique metrics to append.")
    else:
        full_df.drop_duplicates(inplace=True)
        full_df.to_csv(metrics_output_file, index=False)
        updated = True
        print(f"\n Created new metrics file with {len(full_df)} rows.")

    # Handle backups and push if data was updated
    if updated:
        manage_backups(metrics_output_file, updated=True)
        if push_to_server(metrics_output_file):
            print(" File successfully pushed to server.")
        else:
            print(" File not pushed.")
    else:
        manage_backups(metrics_output_file, updated=False)

# Log processed paths
processed_success = write_paths(QCDirFileSuccess, success_paths, processed_success)
processed_fail = write_paths(QCDirFileFail, fail_paths, processed_fail)

print(f"\n Updated: {QCDirFileSuccess}")
print(f" Skipped/Failed: {QCDirFileFail}")
