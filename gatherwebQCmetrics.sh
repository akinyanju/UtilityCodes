#!/usr/bin/env bash

#SBATCH -p gt_compute
#SBATCH --cpus-per-task=2
#SBATCH -t 72:00:00
#SBATCH --mem=16G
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=raman.lawal@jax.org
#SBATCH --job-name=duckDBgatherwebQCmetrics
#SBATCH --output=/gt/data/seqdma/GTwebMetricsTables/.slurmlog/%x.%N.o%j.log

# ─────────────────────────────────────────────────────────────
#Author: Raman Akinyanju Lawal <akinyanju.lawal@jax.org>
# ─────────────────────────────────────────────────────────────
# Load required modules
module use --append /gt/research_development/qifa/elion/modulefiles
module load node/8.6.0
module load duckdb/1.2.2
# ─────────────────────────────────────────────────────────────
# Constants and Configuration
Email="GTdrylab@jax.org"
OUT="/gt/data/seqdma/GTwebMetricsTables"
QCdir_illumina_nonarchive="/gt/data/seqdma/qifa"
QCdir_pacbio_nonarchive="/gt/data/seqdma/qifa-pb"
QCdir_ont_nonarchive="/gt/data/seqdma/qifa-ont"
QCdir_archive="/gt/data/seqdma/.qifa.qc-archive"
qifaPipelineDir="/gt/research_development/qifa/elion/software/qifa-ops/0.1.0"
export SETJSONFILE=".settings.json"
export RunInfo="RunInfo.xml"
# ─────────────────────────────────────────────────────────────
# Color-coded log functions
log_info()    { echo -e "\033[1;34m[INFO]\033[0m $1"; }
log_warn()    { echo -e "\033[1;33m[WARN]\033[0m $1"; }
log_error()   { echo -e "\033[1;31m[ERROR]\033[0m $1"; }
# ─────────────────────────────────────────────────────────────
# Error trap with email reporting
set -E
declare -i last_lineno
trap 'last_lineno=$LINENO' DEBUG
trap 'catch_error $last_lineno "$BASH_COMMAND"' ERR

catch_error() {
  local line="$1"
  local cmd="$2"
  log_error "Error on line $line: $cmd"
  echo "[$(date)] Error on line $line: $cmd" > "$OUT/.logfile"
  mail -s "[FAILURE] GT Metrics Update Error" "$Email" < "$OUT/.logfile"
  exit 1
}
# ─────────────────────────────────────────────────────────────
# ─── Ensure DuckDB is installed ──────────────────
check_duckdb_installed() {
  if ! command -v duckdb >/dev/null 2>&1; then
    log_error "DuckDB CLI not found on system."
    echo -e "DuckDB is missing on the system where this job ran.\n\nJob failed at: $(date)\nHostname: $(hostname)" \
      | mail -s "DuckDB installation missing for $Application Pipeline" "$Email"
    exit 1
  fi
}
# ─────────────────────────────────────────────────────────────
# Initialization wrapper
init_command_per_app() {
  check_list_temp_script
  set_global_paths

  # Skip application if whitelist fails
  if ! whitelist_QCdir; then
    return 1
  fi
  if ! update_ProjDir_list; then
    return 1
  fi
  database
}
 # Run this only once after all applications are processed
init_command_once() {
    set_global_paths
    duckDB_call_seq
    qc_app_index_table
    create_qc_metrics_indexes "qc_illumina_metrics"
    create_qc_metrics_indexes "qc_pacbio_metrics"
    create_qc_metrics_indexes "qc_ont_metrics"
    create_unified_qc_view
    destination_server
}
#---------------------------------------------------
# Performs pre-execution validation for a QC metrics processing pipeline
# - A required list of pipeline names exists.
# - All critical files (main metrics script and app-specific template) exist before proceeding.
# - Sends an email notification and aborts if anything is missing.
#---------------------------------------------------
check_list_temp_script() {
  if [[ ! -f "$qifaPipelineDir/gatherApplicationMetrics.js" ]]; then
    log_error "Missing gatherApplicationMetrics.js"
    echo -e "ERROR: Missing $qifaPipelineDir/gatherApplicationMetrics.js\nProgram aborted!" \
      | mail -s "[DASHBOARD] GT interface missing gatherApplicationMetrics.js" "$Email"
    return 1
  fi

  clean_app="$(echo "$Application" | tr -d '[:space:]' | tr '[:lower:]' '[:upper:]')"

  # Skip template check for PacBio and ONT
  if [[ "$clean_app" != "PACBIO" && "$clean_app" != "ONT" ]]; then
    TEMPLATE_FILE="$qifaPipelineDir/qifa-qc-scripts/$Application/$Application.pe.report.database.template"
    if [[ ! -f "$TEMPLATE_FILE" ]]; then
      log_error "Missing template: $TEMPLATE_FILE"
      echo -e "ERROR: Missing $TEMPLATE_FILE\n\nMetric collection now aborted for $Application!" \
        | mail -s "[DASHBOARD] GT interface missing $Application.pe.report.database.template" "$Email"
      return 1
    fi
  fi

  return 0
}
#---------------------------------------------------
#set global variables
#---------------------------------------------------
set_global_paths() {
  #if any of the directories not avaiable, create it
  for dir in "$OUT/.last_import_push" "$OUT/.slurmlog" "$OUT/.logs" \
    "$OUT/.whitelist_QCdir/archive_scan_tracker" "$OUT/.whitelist_QCdir/non_archive_scan_tracker"; do
      [[ ! -d "$dir" ]] && mkdir -p "$dir"
  done

  archive_check_file="$OUT/.whitelist_QCdir/archive_scan_tracker/$Application.QCDir_archive.update_day_start.txt"
  reset_check_file="$OUT/.whitelist_QCdir/non_archive_scan_tracker/$Application.QCDir_nonarchive.update.reset_day_start.txt"
  qcdir_file_list="$OUT/.whitelist_QCdir/$Application.qcdir_file_list.txt"
  qcdir_file_update_list="$OUT/.whitelist_QCdir/$Application.qcdir_file_update_list.txt"
  nonarchive_tmp_file="$OUT/.whitelist_QCdir/.tmp.QCDir_nonarchive.$Application"
  nonarchive_dirlist_file="$OUT/.whitelist_QCdir/.DirList.$Application"
  pulling_undelivered_qc_flag="$OUT/.whitelist_QCdir/$Application.activate_collecting_undelivered_metrics_from_archive.txt"


  unmatched_species_log="$OUT/.logs/unmatched_species_samples.log"

  slurm_log_dir="$OUT/.slurmlog"
  # Clean old SLURM logs (>1 day)
  find "$slurm_log_dir" -type f -mtime +1 -delete
  last_import_push_dir="$OUT/.last_import_push"

  duckDB_PATH="$OUT/GTdashboardMetrics.duckdb"
  duckDB_lockfile="$OUT/.duckdb.lock"
  duckDB_lastpush="$last_import_push_dir/GTdashboardMetrics.lastpush.duckdb"
  duckDB_logfile="$OUT/.logs/duckdb.import.log"
  duckDB_duplicatefile="$OUT/.logs/duckdb.deduplicate.metrics.log"
  duckDB_errorlog="$OUT/.logs/duckdb.error.log"
  duckDB_missing_metrics="$OUT/.logs/duckdb.missing_metrics.log"
  push_server="ctgenometech03:/srv/shiny-server/.InputDatabase/duckDB"
}
#---------------------------------------------------
# Determine eligible QC directories for metrics update
# - Skips archival directories for 30 days after last scan
# - Collects only new folders not already recorded
# - Resets tracking every 10 days
#---------------------------------------------------

#------------------------------------------------------------------
# Function: app_match
# Purpose : Check if Application exists in any valid DuckDB table
#           (qc_illumina_metrics, qc_pacbio_metrics, qc_ont_metrics).
#           Avoids querying non-existent tables.
#------------------------------------------------------------------
app_match() {
  local queries=()
  local app_lower
  app_lower=$(echo "$Application" | tr '[:upper:]' '[:lower:]')

  # Check which tables exist
  existing_tables=$(duckdb "$duckDB_PATH" -csv -noheader <<SQL
SELECT table_name FROM information_schema.tables
WHERE lower(table_name) IN ('qc_illumina_metrics', 'qc_pacbio_metrics', 'qc_ont_metrics');
SQL
)

  # Conditionally add each SELECT if the table exists
  while read -r table; do
    queries+=("SELECT DISTINCT Application FROM $table")
  done <<< "$existing_tables"

  # Exit early if no valid tables found
  if [[ ${#queries[@]} -eq 0 ]]; then
    return 1
  fi

  # Build and run the combined UNION query
  duckdb "$duckDB_PATH" -csv -noheader <<SQL
SELECT Application FROM (
  ${queries[0]}$(for ((i=1; i<${#queries[@]}; i++)); do echo -e "\nUNION\n${queries[i]}"; done)
)
WHERE LOWER(Application) = LOWER('$Application');
SQL
}

#------------------------------------------------------------------
# Function: find_longread_directories_nonarchive
# Purpose : Find PacBio or ONT project directories in non-archival
#           directories using .settings.json. Logs structured messages
#           and handles missing files gracefully.
#------------------------------------------------------------------
find_longread_directories_nonarchive() {
  local result_dirs=()
  local search_dir
  local app_lower=$(echo "$Application" | tr '[:upper:]' '[:lower:]')

  if [[ "$app_lower" == "pacbio" ]]; then
    search_dir="$QCdir_pacbio_nonarchive"
  elif [[ "$app_lower" == "ont" ]]; then
    search_dir="$QCdir_ont_nonarchive"
  else
    log_error "[$Application] Unsupported Application: $Application"
    return 1
  fi

  if ! find "$search_dir" -type f -name "$SETJSONFILE" | grep -q .; then
    log_warn "[$Application] No $SETJSONFILE found in $search_dir"
    return 0
  fi

  while IFS= read -r json_path; do
    if grep -i '"application"[[:space:]]*:[[:space:]]*"'$Application'"' "$json_path" >/dev/null; then
      local proj_dir
      proj_dir=$(dirname "$json_path")
      result_dirs+=("${proj_dir}/${Application}")
    fi
  done < <(find "$search_dir" -type f -name "$SETJSONFILE")

  log_info "[$Application] Found ${#result_dirs[@]} matching $Application projects in $search_dir"
  printf "%s\n" "${result_dirs[@]}"
}
#------------------------------------------------------------------
# Function: find_longread_directories_archive
# Purpose : Find PacBio or ONT project directories in archival
#           directories using .settings.json. Logs actions and handles
#           missing files safely.
#------------------------------------------------------------------
find_longread_directories_archive() {
  local result_dirs=()
  local search_dir="$QCdir_archive"
  local app_lower=$(echo "$Application" | tr '[:upper:]' '[:lower:]')

  if [[ "$app_lower" != "pacbio" && "$app_lower" != "ont" ]]; then
    log_error "[$Application] Unsupported Application: $Application"
    return 1
  fi

  if ! find "$search_dir" -type f -name "$SETJSONFILE" | grep -q .; then
    log_warn "[$Application] No $SETJSONFILE found in $search_dir"
    return 0
  fi

  while IFS= read -r json_path; do
    if grep -i '"application"[[:space:]]*:[[:space:]]*"'$Application'"' "$json_path" >/dev/null; then
      local proj_dir
      proj_dir=$(dirname "$json_path")
      result_dirs+=("${proj_dir}/${Application}")
    fi
  done < <(find "$search_dir" -type f -name "$SETJSONFILE")

  log_info "[$Application] Found ${#result_dirs[@]} matching $Application projects in $search_dir"
  printf "%s\n" "${result_dirs[@]}"
}

#------------------------------------------------------------------
# Function: whitelist_QCdir
# Purpose : Identify new or missing QC project directories by checking
#           archival and non-archival locations based on a 30-day refresh
#           cycle or database state. Handles long-read (PacBio, ONT) and
#           Illumina data intelligently, deduplicates directory lists,
#           resets update trackers periodically, and ensures DirList is
#           populated for downstream processing.
#------------------------------------------------------------------

whitelist_QCdir() {
  # Step 1: Establish reference date for archive scan
  if [[ -s "$archive_check_file" ]]; then
    day_5_start=$(cat "$archive_check_file")
  else
    day_5_start=$(date +"%Y-%m-%d")
    echo "$day_5_start" > "$archive_check_file"
  fi

  # Step 2: Compute days since last archive scan
  day_present=$(date +"%Y-%m-%d")
  day_5_countdown=$(( ( $(date -d "$day_present" +%s) - $(date -d "$day_5_start" +%s) ) / 86400 ))

  # Step 3: If more than 5 days, scan archival directory. 
  # undelivered data is collected only from this directory to avoid collecting unprocessed QC. This is triggered by "$pulling_undelivered_qc_flag"
  if [[ "$day_5_countdown" -ge 5 ]]; then
    log_info "[$Application] Scanning archived directories for application (30-day refresh)."
    if [[ "$Application" == "PacBio" || "$Application" == "ONT" ]]; then
      find_longread_directories_archive > "$qcdir_file_list"
    else
      find "$QCdir_archive" -type d -name "$Application" > "$qcdir_file_list"
    fi        
    if [[ -f "$qcdir_file_update_list" ]]; then
        grep -vf "$qcdir_file_update_list" "$qcdir_file_list" | awk '!seen[$0]++' > "$nonarchive_dirlist_file"
        mv "$nonarchive_dirlist_file" "$qcdir_file_list"
        DirList=$(cat "$qcdir_file_list")
    fi
    echo "$day_present" > "$archive_check_file"
    touch "$pulling_undelivered_qc_flag"
  # Step 4: If database exists, look for newly added non-archived directories
  elif [[ -s "$duckDB_PATH" ]]; then
    if app_match | grep -Fxq "$Application" && [[ -f "$qcdir_file_list" ]]; then
      log_info "[$Application] found in database. Will now search for new QC directory...."
      cat "$qcdir_file_list" >> "$qcdir_file_update_list"
      awk '!seen[$0]++' "$qcdir_file_update_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_update_list"
      if [[ "$Application" == "PacBio" || "$Application" == "ONT" ]]; then
        find_longread_directories_nonarchive > "$qcdir_file_list"
      else
        find "$QCdir_illumina_nonarchive" -type d -name "$Application" > "$qcdir_file_list"
      fi
      #if project was previously collected, remove the directory and collect only new project
      grep -vf "$qcdir_file_update_list" "$qcdir_file_list" | awk '!seen[$0]++' > "$nonarchive_dirlist_file"
      mv "$nonarchive_dirlist_file" "$qcdir_file_list"
      DirList=$(cat "$qcdir_file_list")
    else
      log_warn "[$Application] missing in database or [$qcdir_file_list]. Starting directory collection from archival and non-archival sources..."
      if [[ "$Application" == "PacBio" || "$Application" == "ONT" ]]; then
        find_longread_directories_nonarchive > "$qcdir_file_list"
        find_longread_directories_archive >> "$qcdir_file_list"
      else
        find "$QCdir_illumina_nonarchive" "$QCdir_archive" -type d -name "$Application" > "$qcdir_file_list"
      fi
      awk '!seen[$0]++' "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      if [[ -f "$qcdir_file_update_list" ]]; then
        grep -vf "$qcdir_file_update_list" "$qcdir_file_list" | awk '!seen[$0]++' > "$nonarchive_dirlist_file"
        mv "$nonarchive_dirlist_file" "$qcdir_file_list"
      fi
      DirList=$(cat "$qcdir_file_list")
      #touch "$pulling_undelivered_qc_flag"
    fi
  # Step 5: If no existing metrics file, scan both non-archived and archived
  else
    log_info "[$Application] Collecting QC directories from both the archival and non-archival..."
    log_info "[$Application] 30 days countdown to look-back into archival now set..."
    if [[ "$Application" == "PacBio" || "$Application" == "ONT" ]]; then
      find_longread_directories_nonarchive > "$qcdir_file_list"
      find_longread_directories_archive >> "$qcdir_file_list"
    else
      find "$QCdir_illumina_nonarchive" "$QCdir_archive" -type d -name "$Application" > "$qcdir_file_list"
    fi
    awk '!seen[$0]++' "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    DirList=$(cat "$qcdir_file_list")
    echo "$day_present" > "$archive_check_file"
    #touch "$pulling_undelivered_qc_flag"
  fi
  # Step 6: Reset non-archive update tracker every 10 days
  if [[ -s "$reset_check_file" ]]; then
    day_10_start=$(cat "$reset_check_file")
    day_10_end=$(date +"%Y-%m-%d")
    day_10_countdown=$(( ( $(date -d "$day_10_end" +%s) - $(date -d "$day_10_start" +%s) ) / 86400 ))

    if [[ "$day_10_countdown" -ge 10 ]]; then
      log_info "[$Application] Resetting non-archive update list after 10 days."
      truncate -s 0 "$qcdir_file_update_list"
      echo "$day_present" > "$reset_check_file"
    fi
  else
    echo "$day_present" > "$reset_check_file"
  fi
  #Got to next if no new update
  ProjTotal=$(printf "%s\n" "$DirList" | sed '/^\s*$/d' | wc -l)
  if [[ "$ProjTotal" -eq 0 ]]; then
    log_warn "[$Application] No eligible project directories found. Exiting whitelist_QCdir function."
    return 1;
  fi
}
#------------------------------------------------------------
# Filters project directories for valid QC metrics collection
# - Ensures only primary application (e.g., WGS, ATAC) is selected
# - Based on presence of .settings.json and application match
#------------------------------------------------------------
update_ProjDir_list() {
  ProjDirs=""
  ProjCount=$(echo -en "$DirList\n" | wc -l)

  for n in $(seq 1 "$ProjCount"); do
    ProjList=$(echo -en "$DirList\n" | sed -n "${n}p")
    ProjDir_list="${ProjList%/*}"

    if [[ -f "$ProjDir_list/$SETJSONFILE" ]]; then
      projectApplication=$(grep '"application":' "$ProjDir_list/$SETJSONFILE" | awk -F'"' '{print $4}')

      if [[ -n "$projectApplication" ]]; then
        # Application routing logic for primary QC type. 
        if [[ "$Application" == "atacseq"     && ",ATACSeq,ATAC,"            == *",$projectApplication,"* ]] ||
           [[ "$Application" == "chic"        && ",Other,"                   == *",$projectApplication,"* ]] ||
           [[ "$Application" == "hic"         && ",HiC,"                     == *",$projectApplication,"* ]] ||
           [[ "$Application" == "ctp"         && ",PDX-WGS,"                 == *",$projectApplication,"* ]] ||
           [[ "$Application" == "pdxrnaseqR2" && ",cellranger,"              == *",$projectApplication,"* ]] ||
           [[ "$Application" == "rnaseq"      && ",mRNA,Total-RNA,"          == *",$projectApplication,"* ]] ||
           [[ "$Application" == "rrbs"        && ",RRBS,"                    == *",$projectApplication,"* ]] ||
           [[ "$Application" == "wgbs"        && ",WGBS,"                    == *",$projectApplication,"* ]] ||
           [[ "$Application" == "chipseq"     && ",ChiP,ChIPSeq,"            == *",$projectApplication,"* ]] ||
           [[ "$Application" == "pdxrnaseq"   && ",PDX-mRNA,pdxrnaseq,"      == *",$projectApplication,"* ]] ||
           [[ "$Application" == "pdxwgs"      && ",PDX-WGS,"                 == *",$projectApplication,"* ]] ||
           [[ "$Application" == "rnaseqR2"    && ",cellranger,"              == *",$projectApplication,"* ]] ||
           [[ "$Application" == "wes"         && ",WES,"                     == *",$projectApplication,"* ]] ||
           [[ "$Application" == "wgs"         && ",WGS,ChIA-PET,"            == *",$projectApplication,"* ]] ||
           [[ "$Application" == "basic"       && ",Amplicon,Other,"          == *",$projectApplication,"* ]] ||
           [[ "$Application" == "ONT"         && ",ONT,"                     == *",$projectApplication,"* ]] ||
           [[ "$Application" == "PacBio"      && ",PacBio,"                  == *",$projectApplication,"* ]]; then

            ProjDirs+="${ProjDir_list}"$'\n'
        fi
      fi
    fi
  done

  ProjTotal=$(printf "%s\n" "$ProjDirs" | sed '/^\s*$/d' | wc -l)
  echo -en "$ProjDirs" > "$qcdir_file_list"

  if [[ "$ProjTotal" -eq 0 ]]; then
    log_warn "[$Application] No eligible project directories found. Exiting update_ProjDir_list function."
    return 1;
  fi
}

#------------------------------------------------------------
# Function: QCdelivery_check_status
# Purpose : Extract key metadata from .settings.json for each project
# Fallbacks to "NULL" if any expected field is missing
#------------------------------------------------------------
QCdelivery_check_status() {
  local settings_path="$ProjDir/$SETJSONFILE"

  if [[ -f "$settings_path" ]]; then

    # ───── Helper to extract JSON key with fallback ─────
    get_json_value() {
      local key=$1
      local val

      val=$(jq -r ".$key" "$settings_path" 2>/dev/null)
      if [[ -z "$val" || "$val" == "null" ]]; then
        val=$(awk -F'"' -v k="$key" '$2 == k {print $4}' "$settings_path")
      fi
      val=$(echo "$val" | xargs)
      [[ -z "$val" ]] && val="NULL"
      echo "$val"
    }

    # Extract fields using helper
    projectId=$(get_json_value "projectId")
    projectFinal=$(get_json_value "projectFinal")

    # ───── Extract deliveryFolder ─────
    if grep -q "\"deliveryFolder\"" "$settings_path"; then
      folder_path=$(awk -F'"' '/"deliveryFolder"/ {print $4}' "$settings_path")
      if [[ -z "$folder_path" || "$folder_path" == "NULL" ]]; then
        deliveryfolder="NULL"
      else
        clean_path=$(echo "$folder_path" | sed 's#^/*##; s#/*$##')
        # If path contains "/", extract second-to-last directory (PacBio); else use as-is
        if [[ "$clean_path" == */* ]]; then
          if [[ "$Application" == "PacBio" ]]; then
            deliveryfolder=$(echo "$clean_path" | awk -F'/' '{print $(NF-2)}')
          else
            deliveryfolder=$(echo "$clean_path" | awk -F'/' '{print $(NF-1)}')
          fi
        else
          deliveryfolder="$clean_path"
        fi
        [[ -z "$deliveryfolder" ]] && deliveryfolder="NULL"
      fi
    else
      deliveryfolder="NULL"
      log_warn "[$Application] $ProjDir → Missing deliveryFolder in $SETJSONFILE"
    fi

    # ───── Extract releaseDate ─────
    if grep -q "\"releaseDate\"" "$settings_path"; then
      raw_date=$(awk -F'"' '/"releaseDate"/ {print $4}' "$settings_path")
      releaseDate=$(date -d "$raw_date" +"%Y-%m-%d" 2>/dev/null || echo "NULL")
    else
      releaseDate="NULL"
      log_warn "[$Application] $ProjDir → Missing releaseDate in $SETJSONFILE"
    fi
  else
    projectId="NULL"
    projectFinal="NULL"
    deliveryfolder="NULL"
    releaseDate="NULL"
    log_warn "[$Application] Missing $SETJSONFILE in $ProjDir → All metadata set to NULL"
  fi
}
#------------------------------------------------------------
# Function: QCdir_updatelist
# Purpose : If no delivery info is available (deliveryfolder/releaseDate is NULL),
#           remove the project from whitelist to allow re-collection later
#------------------------------------------------------------
QCdir_updatelist() {
  local skip=0

  if [[ "$deliveryfolder" == "NULL" ]]; then
    log_warn "[$Application] $ProjDir → deliveryFolder is NULL"
    skip=1
  fi

  if [[ "$releaseDate" == "NULL" ]]; then
    log_warn "[$Application] $ProjDir → releaseDate is NULL"
    skip=1
  fi

  if [[ "$skip" -eq 1 ]]; then
    if grep -q "$ProjDir" "$qcdir_file_list"; then
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
    fi
    return 1  # Signal to database function that this project should be skipped. continue to next is called in databae function
  fi
  return 0
}

#------------------------------------------------------------
# Function: pivot_long_illumina
# Purpose : convert the gather_illumina_metrics_js output to pivot_long_illumina
#------------------------------------------------------------
pivot_long_illumina() {
  # Get all headers (comma-separated)
  IFS=',' read -r -a all_headers < <(head -n1 "$metrics_csv")

  # Get starting column number of 'Reads_Total'
  start_col=$(head -n1 "$metrics_csv" | tr ',' '\n' | nl | grep -w "Reads_Total" | awk '{print $1}')

  if [[ -z "$start_col" ]]; then
    log_error "[$Application] Pivoting long....: Column 'Reads_Total' not found! at $metrics_csv"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi

  # Headers before pivot point
  id_headers=("${all_headers[@]:0:$((start_col - 1))}")

  # Output long format
  {
    (IFS=','; echo "${id_headers[*]},name,value")

    tail -n +2 "$metrics_csv" | while IFS=',' read -r -a row; do
      id_fields=("${row[@]:0:$((start_col - 1))}")
      for ((j=start_col-1; j<${#row[@]}; j++)); do
        name="${all_headers[$j]}"
        value="${row[$j]}"
        (IFS=','; echo "${id_fields[*]},$name,$value")
      done
    done
  }
}
#------------------------------------------------------------
# Function: gather_illumina_metrics_js
# Purpose : Run application-specific JS parser and normalize the output
#------------------------------------------------------------
function gather_illumina_metrics_js {
  metrics_csv="$OUT/${projectId}_QCreport.$Application.csv"
  metrics_csv_tmp="$OUT/${projectId}_QCreport.$Application.tmp.csv"
  metrics_log="$OUT/${projectId}_QCreport.$Application.log"
  metrics_csv_species="$OUT/${projectId}_QCreport.speciesid.csv"

  if [[ $Application == "basic" ]]; then
    $qifaPipelineDir/gatherApplicationMetrics.js getmetrics \
      -p "$ProjDir" \
      -q basic \
      -r "$qifaPipelineDir/qifa-qc-scripts/$Application/$Application.pe.report.database.template" \
      -o "$metrics_csv" 2>&1 > "$metrics_log"
  else
    $qifaPipelineDir/gatherApplicationMetrics.js getmetrics \
      -p "$ProjDir" \
      -q basic,"$Application" \
      -r "$qifaPipelineDir/qifa-qc-scripts/$Application/$Application.pe.report.database.template" \
      -o "$metrics_csv" 2>&1 > "$metrics_log"
  fi
   
  if [[ -s "$metrics_csv" ]]; then
    $qifaPipelineDir/gatherApplicationMetrics.js getmetrics \
      -p "$ProjDir" \
      -q basic,speciesid \
      -r "$qifaPipelineDir/qifa-qc-scripts/speciesid/speciesid.pe.report.body.template" \
      -o  "$metrics_csv_species" 2>&1 > "$metrics_log"

      cat "$metrics_csv_species"  | gawk 'BEGIN {FPAT = "([^,]+)|(\"([^\"]|\"\")*\")"; OFS="," }
        NR==1 {
          for (i=1; i<=NF; i++) if ($i=="GT_QC_Sample_ID" || $i=="Sample_Name" || $i ~ /^Domain[1-4]$/ || $i ~ /^Species[1-5]$/) idx[n++]=i;
          for (i=0; i<n; i++) printf "%s%s", $idx[i], (i<n-1 ? OFS : ORS); next }
          { for (i=0; i<n; i++) printf "%s%s", $idx[i], (i<n-1 ? OFS : ORS) }' > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$metrics_csv_species"
      #extract the species and domains column and merge those with the main csv file. If species information is not available, 
        #n.a. gets introduced by the java gather script which will be handled later in the code 
      gawk -v logfile="$unmatched_species_log" -v Application="$Application" -v projectId="$projectId" \
        'BEGIN {FPAT = "([^,]+)|(\"([^\"]|\"\")*\")"; OFS = ",";}
        NR==FNR {
          for (i=1; i<=NF; i++) {
            if ($i == "Sample_Name") sname_col = i;
            if ($i == "GT_QC_Sample_ID") gid_col = i;
            if ($i ~ /^Domain[1-4]$/ || $i ~ /^Species[1-5]$/) col[i] = $i; }
          if (FNR == 1) next; 
          key_name = $sname_col;
          key_id = $gid_col;
          val = "";
          for (i in col) val = val OFS $i;

          map_name[key_name] = val;
          map_id[key_id] = val; next;
        }
        FNR == 1 {
          for (i=1; i<=NF; i++) {
            if ($i == "Sample_Name") sname_col = i;
            if ($i == "GT_QC_Sample_ID") gid_col = i;
          }
          hdr = $0;
          for (i in col) hdr = hdr OFS col[i]; print hdr; next;}
        {
          key_name = $sname_col;
          key_id = $gid_col;

          if ((key_name in map_name) && map_name[key_name] != "") {
            print $0 map_name[key_name]; 
          } else if ((key_id in map_id) && map_id[key_id] != "") { 
            print $0 map_id[key_id];
          } else {
            print $0 OFS "NaN" OFS "NaN" OFS "NaN" OFS "NaN" OFS "NaN" OFS "NaN" OFS "NaN" OFS "NaN" OFS "NaN";
            lname = tolower(key_name);
            lid = tolower(key_id);
              if (lname != "" && lname != "null" && lname != "nan" && lname != "n.a." &&
                  lid != "" && lid != "null" && lid != "nan" && lid != "n.a.") {
                print "[" strftime("%F %T") "] " Application "] → " projectId " → UNMATCHED for species id Sample_Name → \"" key_name "\" or GT_QC_Sample_ID → \"" key_id "\"" >> logfile;
              }
            }
          }' "$metrics_csv_species" "$metrics_csv" > "$nonarchive_tmp_file"
        mv "$nonarchive_tmp_file" "$metrics_csv"
        rm "$metrics_csv_species"
  fi

  # Check if metrics CSV exists before processing
  if [[ ! -s "$metrics_csv" ]]; then
    log_error "[$Application] Missing or empty metrics CSV: $metrics_csv"
    log_warn "[$Application] $projectId may not be properly processed by $qifaPipelineDir/gatherApplicationMetrics.js getmetrics"
    log_info "[$Application] path in $duckDB_missing_metrics"
    echo -e "Failed gather $qifaPipelineDir/gatherApplicationMetrics.js getmetrics → ProjectID $projectId → QCdir $ProjDir" >> "$duckDB_missing_metrics"
    awk '!seen[$0]++' "$duckDB_missing_metrics" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$duckDB_missing_metrics"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi

  # Count number of columns in the original CSV
 
  col_total=$(head -n1 "$metrics_csv" | awk -F',' '{print NF}')
  {
    # Header
    echo "Investigator_Folder,Project_ID,Project_run_type,Release_Date,RunID,InstrumentID,FlowcellID,Lane,ProjStatus,$(head -n1 "$metrics_csv"),Cluster_PF_PCT,Reads_Cluster_number_Mb,Reads_Cluster_number_PF_Mb,Q30_or_higher_PCT,Yield_Gb,Aligned_PhiX_PCT,Error_rate_PhiX_alignment"
    # Body
    tail -n +2 "$metrics_csv" | \
    awk '{n=split($0,a,"\""); for(i=2;i<=n;i+=2) gsub(",", "", a[i]); s=a[1]; for(i=2;i<=n;i++) s=s a[i]; print s}' | \
    awk -v OFS=',' -v N="$col_total" \
        -v A="$deliveryfolder" -v B="$projectId" -v C="$projectFinal" \
        -v D="$releaseDate" -v E="$RunID" -v F="$InstrumentID" -v G="$FlowcellID" -v H="$Lane" \
        -v K="$Cluster_PF" -v L="$Reads" -v M="$Reads_PF" -v N2="$PCT_Q30_and_More" \
        -v O="$Yield" -v P="$Aligned" -v Q="$Error" '
    BEGIN { FS=OFS="," }
    {
      while (NF < N) $(++NF) = "";
      for (i = 1; i <= NF; i++) if ($i == "") $i = "NULL";
      J = (A == "NULL" || A == "" || D == "NULL" || D == "") ? "Undelivered" : "Delivered";
      print A, B, C, D, E, F, G, H, J, $0, K, L, M, N2, O, P, Q;
    }'
  } > "$metrics_csv_tmp"
  sed 's/"//g' "$metrics_csv_tmp" > ${metrics_csv_tmp}_1
  mv ${metrics_csv_tmp}_1 "$metrics_csv_tmp"
  # Sometime, empty rows is carried over from "$metrics_csv" whereas metadata like Investigator folders get added thereafter. 
    #The  awk will remove rows where either the sample or the GT_QC_Sample column is missing data.
  #The second awk identifies the column number for Species and then Homo sapiens naming inconsistency
  cat "$metrics_csv_tmp" | \
    awk -F',' -v OFS=',' 'NR==1 {for (i=1; i<=NF; i++) if (tolower($i)=="species") s=i; for (i=1; i<=NF; i++) col[$i]=i; print; next}
        tolower($col["GT_QC_Sample_ID"]) !~ /^(null|nan|n\.a\.)$/ && tolower($col["Sample_Name"]) !~ /^(null|nan|n\.a\.)$/ {
        if (s) { gsub(/homo sapiens|Human|human/, "Homo sapiens", $s); gsub(/marmoset/, "Callithrix jacchus", $s) } print }' \
  > "$nonarchive_tmp_file" && mv "$nonarchive_tmp_file" "$metrics_csv"
  rm "$metrics_csv_tmp"
  #just a final sanitization so they gets properly imported into database
  cat "$metrics_csv" | \
    awk 'NR==1 {for (i=1; i<=NF; i++) {gsub(/>=/, "_GE_", $i); gsub(/<=/, "_LE_", $i); gsub(/>/, "_GT_", $i); 
      gsub(/</, "_LT_", $i); gsub(/%/, "PCT", $i); gsub(/:/, "", $i); gsub(/[[:space:]]+/, "_", $i); 
      gsub(/__+/, "_", $i); sub(/_$/, "", $i)}} 1' FS=',' OFS=',' > "$metrics_csv_tmp"  
  mv "$metrics_csv_tmp" "$metrics_csv"
  # The second awk will remove rows where the last column is missing value. For instance, if Read Total is missing value, remove that row
  pivot_long_illumina | \
  awk -F',' '{gsub(/^[ \t]+|[ \t]+$/, "", $NF); lc=tolower($NF); if (lc != "nan" && lc != "null" && lc != "n.a.") print}' | sed '1d' || \
  return 1 
}
#
# ─────────────────────────────────────────────────────────────
# Function: pivot_long_longread
# Purpose : Pivot the PacBio metrics CSV to long format, converting
#           NaN/n.a. to NULL and skipping Instrument_SN
# ─────────────────────────────────────────────────────────────
pivot_long_longread() {
  # 1) Read header into array
  IFS=',' read -r -a all_headers < <(head -n1 "$metrics_csv")

  # 2) Read first data row (strip quotes safely)
  local first_data
  first_data=$(tail -n +2 "$metrics_csv" | head -n1 | sed 's/"//g')
  IFS=',' read -r -a first_values <<< "$first_data"

  # 3) Determine first numeric column index (supports %, sci notation)
  local start_idx=-1
  for i in "${!first_values[@]}"; do
    val="${first_values[i]//\"/}"     # remove quotes
    val="${val%%%}"                   # remove trailing percent sign if present
    if [[ "$val" =~ ^[-+]?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$ ]]; then
      start_idx=$i
      break
    fi
  done

  if (( start_idx < 0 )); then
    log_error "[PIVOT_LONG_longread] pivot_long_longread: No numeric column found in $metrics_csv"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi

  # 4) Exclusion list — treat these numeric columns as metadata (not pivoted)
  EXCLUDE_METRIC_HEADERS=(Plate_Number SMRT_Cell_Lot_Number Movie_Time_hours Instrument_SN)
  declare -A EXCLUDE_MAP
  for name in "${EXCLUDE_METRIC_HEADERS[@]}"; do EXCLUDE_MAP["$name"]=1; done

  # 5) ID headers = columns before start_idx + excluded numeric non-metrics
  id_headers=( "${all_headers[@]:0:start_idx}" )
  for (( i = start_idx; i < ${#all_headers[@]}; i++ )); do
    [[ -n "${EXCLUDE_MAP[${all_headers[i]}]}" ]] && id_headers+=("${all_headers[i]}")
  done

  # 6) Print new header
  ( IFS=','; echo "${id_headers[*]},name,value" )

  # 7) Process each data row
  tail -n +2 "$metrics_csv" | while IFS=',' read -r -a row; do
    # extract ID values (original + excluded metrics)
    id_fields=( "${row[@]:0:start_idx}" )
    for (( i = start_idx; i < ${#all_headers[@]}; i++ )); do
      [[ -n "${EXCLUDE_MAP[${all_headers[i]}]}" ]] && id_fields+=( "${row[i]}" )
    done

    # pivot actual metrics only
    for (( j = start_idx; j < ${#row[@]}; j++ )); do
      name="${all_headers[j]}"
      value="${row[j]}"

      [[ -n "${EXCLUDE_MAP[$name]}" ]] && continue  # skip pivoting

      # clean value
      value="${value%\"}"; value="${value#\"}"

      # normalize missing/nulls
      shopt -s nocasematch
      [[ "$value" =~ ^(nan|n\.a\.?|n\.a)$ ]] && value="NULL"
      shopt -u nocasematch

      ( IFS=','; echo "${id_fields[*]},$name,$value" )
    done
  done
}
# ─────────────────────────────────────────────────────────────
# Function: gather_metrics_pacbio
# Purpose : Extract and normalize PacBio QC or Run report into
#           a flattened CSV, optionally including PB and CC metrics
#           and removing unwanted columns.
# ─────────────────────────────────────────────────────────────
gather_metrics_pacbio() {
  shopt -s nullglob

  # Locate QC or fallback
  USE_FALLBACK=0
  local report_files=( "$ProjDir/package/${projectFinal}"*_QC_Report.csv )
  if (( ${#report_files[@]} == 0 )); then
    local fb="$ProjDir/package/Run_Report_${projectFinal}.csv"
    if [[ -f "$fb" ]]; then
      report_files=( "$fb" )
      USE_FALLBACK=1
      log_warn "[$Application] Using fallback report: $(basename "$fb")"
    else
      log_error "[$Application] No QC or Run_Report for ${projectFinal}"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
      return 1
    fi
  fi

  local QC_Report="${report_files[0]}"
  local base; base=$(basename "$QC_Report" .csv)
  metrics_csv="$OUT/${base}.csv"
  metrics_csv_tmp="$OUT/${base}.tmp.csv"

  # Extract CSV from header onward
  local hdr
  hdr=$(grep -n -m1 -E 'Sample_ID|Sample Name|Sample_Name' "$QC_Report" | cut -d: -f1)
  [[ $hdr ]] || { log_error "[PACBIO] No header in $QC_Report"; return 2; }
  tail -n +"$hdr" "$QC_Report" > "$metrics_csv"
  local col_total; col_total=$(head -n1 "$metrics_csv" | awk -F',' '{print NF}')
  log_info "[PACBIO] Extracted $col_total columns from $QC_Report"

  # JSON path
  settings_path="$ProjDir/$SETJSONFILE"
  rawReportPacBio=$(jq -r '..|.path?|select(type=="string")' "$settings_path" 2>/dev/null | head -n1)
  [[ -z "$rawReportPacBio" ]] && log_warn "[PACBIO] No 'path'; metrics→NULL"

  # Species
  Species=$(jq -r '.organism[0] // empty' "$settings_path" 2>/dev/null)
  [[ -z "$Species" ]] && { Species=NULL; log_warn "[PACBIO] Species=NULL"; }

  # PB and CC Metrics (skip if fallback)
  if (( USE_FALLBACK == 0 )); then
    PB_KEYS=(Polymerase_Reads_raw Polymerase_Read_Bases_raw Polymerase_RL_N50_raw \
             Polymerase_RL_longestSubread_mean_raw Polymerase_RL_longestSubread_N50_raw \
             Unique_Molecular_Yield Local_Base_Rate)
    declare -A PB_METRICS=(
      [Polymerase_Reads_raw]=raw_data_report.nreads
      [Polymerase_Read_Bases_raw]=raw_data_report.nbases
      [Polymerase_RL_N50_raw]=raw_data_report.read_n50
      [Polymerase_RL_longestSubread_mean_raw]=raw_data_report.insert_length
      [Polymerase_RL_longestSubread_N50_raw]=raw_data_report.insert_n50
      [Unique_Molecular_Yield]=raw_data_report.unique_molecular_yield
      [Local_Base_Rate]=raw_data_report.local_base_rate_median
    )
    for key in "${PB_KEYS[@]}"; do
      local val=""
      [[ -n "$rawReportPacBio" ]] && val=$(jq -r --arg id "${PB_METRICS[$key]}" \
        '.attributes[]|select(.id==$id)|.value//empty' \
        "$rawReportPacBio/outputs/raw_data.report.json" 2>/dev/null)
      declare "$key"="${val:-NULL}"
    done

    CC_KEYS=(HiFi_reads_ccs_raw HiFi_reads_yield_ccs_raw HiFi_readlength_ccs_raw_mean \
             HiFi_readlength_median_bp_ccs_raw HiFi_readlength_N50_bp_ccs_raw \
             Q30_base_PCT_ccs_raw HiFi_Number_of_Passes_mean_ccs_raw Missing_adapters_PCT_ccs_raw)
    declare -A CC_METRICS=(
      [HiFi_reads_ccs_raw]=ccs2.number_of_ccs_reads
      [HiFi_reads_yield_ccs_raw]=ccs2.total_number_of_ccs_bases
      [HiFi_readlength_ccs_raw_mean]=ccs2.mean_ccs_readlength
      [HiFi_readlength_median_bp_ccs_raw]=ccs2.median_ccs_readlength
      [HiFi_readlength_N50_bp_ccs_raw]=ccs2.ccs_readlength_n50
      [Q30_base_PCT_ccs_raw]=ccs2.percent_ccs_bases_q30
      [HiFi_Number_of_Passes_mean_ccs_raw]=ccs2.mean_npasses
      [Missing_adapters_PCT_ccs_raw]=ccs2.percent_missing_adapters
    )
    ccs_json="$rawReportPacBio/outputs/ccs.report.json"
    for key in "${CC_KEYS[@]}"; do
      local val=""
      [[ -f "$ccs_json" ]] && val=$(jq -r --arg id "${CC_METRICS[$key]}" \
        '.attributes[]|select(.id==$id)|.value//empty' \
        "$ccs_json" 2>/dev/null)
      declare "$key"="${val:-NULL}"
    done

    # Convert percentages
    if [[ "$Q30_base_PCT_ccs_raw" != "NULL" ]]; then
      Q30_base_PCT_ccs_raw=$(awk "BEGIN{printf \"%.2f\", $Q30_base_PCT_ccs_raw*100}")
    fi
    if [[ "$Missing_adapters_PCT_ccs_raw" != "NULL" ]]; then
      Missing_adapters_PCT_ccs_raw=$(awk "BEGIN{printf \"%.2f\", $Missing_adapters_PCT_ccs_raw*100}")
    fi
  fi

  # Instrument detection
  InstrumentName=$(basename "$(dirname "$ProjDir")")
  if [[ -z "$InstrumentName" ]] && compgen -G "$ProjDir/CCS_Report_${projectFinal}*.html" >/dev/null; then
    InstrumentName=$(grep -m1 "Instrument Name" "$ProjDir"/CCS_Report_"${projectFinal}"*.html \
      | sed -E 's+<tr><th>Instrument Name</th><td>++;s+</td></tr>++')
  fi
  case "$InstrumentName" in
    *84148*) InstrumentID="CT_Revio_84148" ;;
    *SQ65119*|*64119*) InstrumentID="CT_Sequel_SQ65119" ;;
    *SQ65039*|*r64039*) InstrumentID="CT_Sequel_SQ65039" ;;
    *) InstrumentID="${InstrumentName:-NULL}" ;;
  esac

  # Normalize and remove excluded columns
  local orig_header first_row
  orig_header=$(head -n1 "$metrics_csv")
  first_row=$(tail -n +2 "$metrics_csv" | awk -F'"' '{n=split($0,a,"\"");for(i=2;i<=n;i+=2)gsub(",", "", a[i]);s=a[1];for(i=2;i<=n;i++)s=s a[i];print s;exit}')
  IFS=, read -r -a cols <<<"$orig_header"
  IFS=, read -r -a vals <<<"$first_row"

  for i in "${!cols[@]}"; do
    cols[i]="${cols[i]// /_}"
    cols[i]="${cols[i]//\(/_}"
    cols[i]="${cols[i]//\)/_}"
  done

  EXCLUDE_COLS=(Sample_Comment Sample_Summary Sample_Well DNA_Control_Complex Template_Prep_Kit \
Binding_Kit Sequencing_Kit Immobilization_time__hours_ Pre-Extension_Time__hours_ \
Status Adapter_Dimer Short_Insert Run_Comments Experiment_Name Experiment_ID \
Run_Start Run_Complete Transfer_Complete Run_Description Instrument \
Instrument_Control_SW_Version Primary_Analysis_Version Instrument_Chemistry_Bundle_Version \
Instrument_SN)
  declare -A SKIP_COL
  for col in "${EXCLUDE_COLS[@]}"; do SKIP_COL["$col"]=1; done

  meta_idx=(); value_idx=()
  for i in "${!cols[@]}"; do
    colname="${cols[i]}"
    if [[ -z "${SKIP_COL[$colname]}" ]]; then
      if [[ "${vals[i]}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        value_idx+=( "$i" )
      else
        meta_idx+=( "$i" )
      fi
    fi
  done

  {
    pb_hdr=""
    cc_hdr=""
    if (( USE_FALLBACK == 0 )); then
      pb_hdr=$(IFS=,; echo "${PB_KEYS[*]}")
      cc_hdr=$(IFS=,; echo "${CC_KEYS[*]}")
    fi
    meta_hdr=""; for idx in "${meta_idx[@]}"; do meta_hdr+="${cols[idx]},"; done
    val_hdr="";  for idx in "${value_idx[@]}"; do val_hdr+="${cols[idx]},";  done
    meta_hdr=${meta_hdr%,}; val_hdr=${val_hdr%,}

    echo "Investigator_Folder,Project_ID,Project_run_type,Release_Date,InstrumentID,Species,ProjStatus,${meta_hdr}${pb_hdr:+,}${pb_hdr}${cc_hdr:+,}${cc_hdr}${val_hdr:+,}${val_hdr}"

    tail -n +2 "$metrics_csv" | awk -F'"' '{
      n=split($0,a,"\""); for(i=2;i<=n;i+=2)gsub(",", "", a[i]); s=a[1];for(i=2;i<=n;i++)s=s a[i];print s
    }' | gawk -v OFS=',' -v N="$col_total" -v A="$deliveryfolder" -v B="$projectId" -v C="$projectFinal" \
         -v D="$releaseDate" -v ID="$InstrumentID" -v SP="$Species" -v USE_FALLBACK="$USE_FALLBACK" \
         -v PB1="$Polymerase_Reads_raw" -v PB2="$Polymerase_Read_Bases_raw" -v PB3="$Polymerase_RL_N50_raw" \
         -v PB4="$Polymerase_RL_longestSubread_mean_raw" -v PB5="$Polymerase_RL_longestSubread_N50_raw" \
         -v PB6="$Unique_Molecular_Yield" -v PB7="$Local_Base_Rate" \
         -v CC1="$HiFi_reads_ccs_raw" -v CC2="$HiFi_reads_yield_ccs_raw" \
         -v CC3="$HiFi_readlength_ccs_raw_mean" -v CC4="$HiFi_readlength_median_bp_ccs_raw" \
         -v CC5="$HiFi_readlength_N50_bp_ccs_raw" -v CC6="$Q30_base_PCT_ccs_raw" \
         -v CC7="$HiFi_Number_of_Passes_mean_ccs_raw" -v CC8="$Missing_adapters_PCT_ccs_raw" \
         -v MI="${meta_idx[*]}" -v VI="${value_idx[*]}" '
    BEGIN {FS=OFS=","; split(MI,m," "); split(VI,v," ")}
    {
      split($0, orig, ",")
      for(i=1;i<=N;i++) if(orig[i]=="") orig[i]="NULL"
      J=(A==""||D=="")?"Undelivered":"Delivered"
      printf "%s,%s,%s,%s,%s,%s,%s", A,B,C,D,ID,SP,J
      for(i in m) printf ",%s", orig[m[i]+1]
      if (USE_FALLBACK == 0) {
        printf ",%s", PB1; printf ",%s", PB2; printf ",%s", PB3; printf ",%s", PB4
        printf ",%s", PB5; printf ",%s", PB6; printf ",%s", PB7
        printf ",%s", CC1; printf ",%s", CC2; printf ",%s", CC3; printf ",%s", CC4
        printf ",%s", CC5; printf ",%s", CC6; printf ",%s", CC7; printf ",%s", CC8
      }
      for(i in v) printf ",%s", orig[v[i]+1]
      print ""
    }'
  } > "$metrics_csv_tmp" 
  sed 's/"//g' "$metrics_csv_tmp" > "$metrics_csv"
  mv "$metrics_csv" "$metrics_csv_tmp"
  cat "$metrics_csv_tmp" | \
    awk 'NR==1 {for (i=1; i<=NF; i++) {gsub(/>=/, "_GE_", $i); gsub(/<=/, "_LE_", $i); gsub(/>/, "_GT_", $i); 
      gsub(/</, "_LT_", $i); gsub(/%/, "PCT", $i); gsub(/:/, "", $i); gsub(/[[:space:]]+/, "_", $i); 
      gsub(/__+/, "_", $i); sub(/_$/, "", $i)}} 1' FS=',' OFS=',' > "$metrics_csv"  
  pivot_long_longread > "$metrics_csv_tmp"
  mv "$metrics_csv_tmp" "$metrics_csv"
}

# ─────────────────────────────────────────────────────────────
# Function: gather_metrics_ont
# Purpose : Locate and parse Oxford Nanopore QC or QCreport CSV,
#           strip thousand‐separator commas and quotes,
#           prepend metadata (folder, project, dates, instrument, species),
#           normalize empty fields to SQL NULL, and set
#           Delivered/Undelivered status.
# ─────────────────────────────────────────────────────────────
gather_metrics_ont() {
  shopt -s nullglob

  # 1) Find ONT QC report or QCreport, in package/ or top‐level
  local candidates=(
    "$ProjDir/package/${projectFinal}"*_QC_Report.csv
    "$ProjDir/${projectFinal}"*_QC_Report.csv
  )
  local variant_candidates=(
    "$ProjDir/package/${projectFinal}"*QCreport.csv
    "$ProjDir/${projectFinal}"*QCreport.csv
  )

  local QC_Report=""
  for f in "${candidates[@]}"; do
    [[ -f $f ]] && { QC_Report="$f"; log_info "[ONT] Using QC_Report: $(basename "$f")"; break; }
  done
  if [[ -z $QC_Report ]]; then
    for f in "${variant_candidates[@]}"; do
      [[ -f $f ]] && { QC_Report="$f"; log_warn "[ONT] Using QCreport variant: $(basename "$f")"; break; }
    done
  fi
  if [[ -z $QC_Report ]]; then
    log_error "[$Application] No QC_Report or QCreport for ${projectFinal}"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi

  # 2) Prepare paths
  local base=$(basename "$QC_Report" .csv)
  metrics_csv="$OUT/${base}.csv"
  metrics_csv_tmp="$OUT/${base}.tmp.csv"

  # 3) Extract CSV from header onward
  local hdr_line
  hdr_line=$(grep -n -m1 -E 'Sample_ID|Sample Name|Sample_Name' "$QC_Report" | cut -d: -f1)
  [[ $hdr_line ]] || { log_error "[ONT] No header in $QC_Report"; return 2; }
  tail -n +"$hdr_line" "$QC_Report" > "$metrics_csv"

  # 3a) Remove any existing "Species" column
  awk 'BEGIN{FPAT="([^,]*)|(\"[^\"]+\")"} NR==1 {for(i=1;i<=NF;i++) if(tolower($i)!="\"species\"") 
    keep[++k]=i} {for(i=1;i<=k;i++) printf "%s%s", $keep[i], (i<k?",":"\n")}' \
    "$metrics_csv" > "$metrics_csv_tmp" && mv "$metrics_csv_tmp" "$metrics_csv"

  # 3b) Strip inner‐commas from quoted fields, then remove all quotes
  awk '{n=split($0,a,"\""); for(i=2;i<=n;i+=2) gsub(",", "", a[i]); s=a[1]; for(i=2;i<=n;i++) s=s a[i]; print s}' \
  "$metrics_csv" > "$metrics_csv_tmp" && mv "$metrics_csv_tmp" "$metrics_csv"

  # 4) Load JSON metadata and species
  local settings_path="$ProjDir/$SETJSONFILE"
  Species=$(jq -r '.organism[0] // empty' "$settings_path" 2>/dev/null) || Species=NULL
  [[ -z $Species ]] && { Species=NULL; log_warn "[ONT] Species=NULL"; }

  # 5) Instrument detection (replaced logic)
  InstrumentName=$(basename "${ProjDir%/*}")
  if [[ -f "$ProjDir/.qifa.ont.json" ]]; then
    hostname=$(grep '"hostname":' "$ProjDir/.qifa.ont.json" | awk '{print $2}' | grep -o '".*"' | sed 's/"//g')
    serial=$(grep '"host_product_serial_number":' "$ProjDir/.qifa.ont.json" | awk '{print $2}' | grep -o '".*"' | sed 's/"//g')
    if [[ -n "$hostname" ]]; then
      InstrumentName="$hostname"
    elif [[ -n "$serial" ]]; then
      InstrumentName="$serial"
    else
      InstrumentName="NULL"
    fi
  fi

  case "$InstrumentName" in
    *GXB02036*)    InstrumentID="CT_GridIONX5_GXB02036"    ;;
    *PCA100115*)   InstrumentID="CT_PromethION_PCA100115" ;;
    *GXB03074*)    InstrumentID="BH_GridIONX5_GXB03074"    ;;
    *P2S-01535*)   InstrumentID="BH_P2_Solo_P2S-01535"     ;;
    *GXB01025*)    InstrumentID="CT_GridIONX5_GXB01025"    ;;
    *GXB01102*)    InstrumentID="CT_GridIONX5_GXB01102"    ;;
    *GXB01186*)    InstrumentID="CT_GridIONX5_GXB01186"    ;;
    *PC24B149*)    InstrumentID="CT_PromethION_PC24B149"   ;;
    *PCT0053*)     InstrumentID="CT_PromethION_PCT0053"    ;;
    *)             InstrumentID="${InstrumentName:-NULL}"  ;;
  esac

  # 6) Build final CSV with NULL and ProjStatus logic
  {
    local original_header
    original_header=$(head -n1 "$metrics_csv")
    echo "Investigator_Folder,Project_ID,Project_run_type,Release_Date,InstrumentID,Species,ProjStatus,${original_header}"

    tail -n +2 "$metrics_csv" | \
    awk -v A="$deliveryfolder" -v B="$projectId" -v C="$projectFinal" \
        -v D="$releaseDate" -v ID="$InstrumentID" -v SP="$Species" \
        'BEGIN{FS=OFS=","}
         {
           split($0, orig, FS)
           for(i=1;i<=length(orig);i++) if(orig[i]=="") orig[i]="NULL"
           status = (A==""||A=="NULL"||D==""||D=="NULL") ? "Undelivered" : "Delivered"
           printf "%s,%s,%s,%s,%s,%s,%s", A,B,C,D,ID,SP,status
           for(i=1;i<=length(orig);i++) printf "%s%s", OFS, orig[i]
           print ""
         }'
  } > "$metrics_csv_tmp"

  cat "$metrics_csv_tmp" | \
  awk 'NR==1 {for (i=1; i<=NF; i++) {gsub(/>=/, "_GE_", $i); gsub(/<=/, "_LE_", $i); gsub(/>/, "_GT_", $i); 
    gsub(/</, "_LT_", $i); gsub(/%/, "PCT", $i); gsub(/:/, "", $i); gsub(/[[:space:]]+/, "_", $i); 
    gsub(/__+/, "_", $i); sub(/_$/, "", $i)}} 1' FS=',' OFS=',' > "$metrics_csv"

  # Pivot to long with sanitization
  pivot_long_longread > "$metrics_csv_tmp"
  mv "$metrics_csv_tmp" "$metrics_csv"
}

#
#------------------------------------------------------------------
# Function: get_database_row_count_if_table_exists
# Purpose : Check if a given table exists in the DuckDB database,
#           and if so, return the number of rows in that table.
#           Returns 0 if the table does not exist or if an error occurs.
#------------------------------------------------------------------

get_database_row_count_if_table_exists() {
  local db="$1"     # Path to DuckDB database file
  local table="$2"  # Table name to check
  if duckdb "$db" -c "SELECT * FROM sqlite_master WHERE name = '$table';" 2>/dev/null | grep -q "$table"; then
    duckdb "$db" -csv -noheader -c "SELECT COUNT(*) FROM $table;" 2>/dev/null
  else
    echo 0
  fi
}
# ─────────────────────────────────────────────────────────────
# Function: duckDB_call
# Purpose:
#   Handles conditional import of QC metrics into `qc_illumina_metrics` in DuckDB.
#   It performs the following steps:
#     - Skips insert if exact row already exists (based on primary metadata)
#     - Loads tab-delimited file into a temporary table using `read_csv_auto`
#     - Renames columns and appends `Application` field
#     - Inserts into `qc_illumina_metrics` using `INSERT OR IGNORE` to avoid duplication
#     - Logs before/after row count to measure new insertions
#     - Uses `flock` to prevent race conditions during concurrent DB access
#   This function ensures all rows are conditionally inserted with strong safeguards
#   and avoids unnecessary rewriting or duplication.
# ─────────────────────────────────────────────────────────────
duckDB_call() {
  tmp_table="${Application}_tmp"
  tmp_table_raw="${Application}_raw"
  metrics_file="$OUT/${projectId}_QCreport.$Application.$n.csv"

  # Step 1: Reset log file if older than 31 days
  if [[ -f "$duckDB_logfile" ]] && [[ $(find "$duckDB_logfile" -mtime +31 -print) ]]; then
    rm -f "$duckDB_logfile"
  fi

  # Ensure the log file always exists
  touch "$duckDB_logfile"

  # Step 2: Pre-check and deduplication of DuckDB.  
    # If metrics was previously collected and imported in database, skip. 
  if [[ -s "$metrics_file" ]]; then
    #First deduplicate, if any
    awk '!seen[$0]++' "$metrics_file" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$metrics_file"
    #extract the sample size used for logs
    SAMPLE_SIZE=$(cat "$metrics_file" | awk -F',' 'NR > 1 {seen[$11]++} END {print length(seen)}')
    # Extract key values from first real data row (line 2)
    mapfile -t first_row < <(sed -n 2p "$metrics_file")

    IFS=',' read -r \
      deliveryfolder projectId projectFinal releaseDate \
      RunID InstrumentID FlowcellID Lane \
      ProjStatus GT_QC_Sample_ID Sample_Name Species \
      metric_name metric_value <<< "${first_row[0]}"

    timestamp=$(date)

    # Check if such a row already exists. If yes, exit the import for that project
    existing_row_count=$(duckdb "$duckDB_PATH" -csv -noheader -c "
      SELECT COUNT(*) FROM qc_illumina_metrics
      WHERE Investigator_Folder = '$deliveryfolder'
        AND Project_ID = '$projectId'
        AND Project_run_type = '$projectFinal'
        AND Release_Date = '$releaseDate'
        AND RunID = '$RunID'
        AND InstrumentID = '$InstrumentID'
        AND FlowcellID = '$FlowcellID'
        AND Lane = '$Lane'
        AND GT_QC_Sample_ID = '$GT_QC_Sample_ID'
        AND Sample_Name = '$Sample_Name'
        AND Species = '$Species'
        AND name = '$metric_name'
        AND value = '$metric_value'
        AND Application = '$Application';
      " 2>/dev/null)

    if [[ "$existing_row_count" -gt 0 ]]; then
      echo -e "\n--- [START] Import for $Application → $deliveryfolder → $projectId @ $timestamp ---" >> "$duckDB_logfile"
      echo "    ⚠️  Skipping import: Exact metrics already exist for $Application → $deliveryfolder → $projectId" >> "$duckDB_logfile"
      echo "--- [END] Import for $Application → $deliveryfolder → $projectId @ $timestamp ---" >> "$duckDB_logfile"
      return 0
    fi

    # ─────────────────────────────────────────────────────────────
    # Step 3: Perform import
    (
      flock -x 200
      echo "[$(date)] LOCK ACQUIRED by job $$ for $Application → $projectId" >> "$duckDB_logfile"
      echo -e "--- [START] Import for $Application → $projectId @ $timestamp ---" >> "$duckDB_logfile"

      before_count=$(get_database_row_count_if_table_exists "$duckDB_PATH" "qc_illumina_metrics")
      [[ ! "$before_count" =~ ^[0-9]+$ ]] && before_count=0

    # Try the most memory-hungry configuration first since loss of memory by duckdb can occur, then fallback to lower if it fails
    memory_limits=("12GB" "8GB" "4GB")
    success=0

    for mem_limit in "${memory_limits[@]}"; do
      echo "[$(date)] Trying DuckDB insert with memory_limit=$mem_limit" >> "$duckDB_logfile"
      #Validate column count BEFORE attempting DuckDB import
      #If the import file csv has fewer or more than 14 columns, or a wrong header, the SELECT will fail
      if [[ $(head -n1 "$metrics_file" | awk -F',' '{print NF}') -lt 14 ]]; then
        log_error "Header mismatch in $metrics_file"
        grep -v "$ProjDir" $qcdir_file_list > $nonarchive_tmp_file
        mv $nonarchive_tmp_file $qcdir_file_list 
        log_info "[$Application] will recollect $ProjDir in future gather"
        rm -f "$lane_tmp"  
        return 1
      fi

      #duckdb "$duckDB_PATH" <<EOF 2> >(tee "$duckDB_errorlog" >&2)
      duckdb "$duckDB_PATH" <<EOF 2> "$duckDB_errorlog"
PRAGMA memory_limit='$mem_limit';

DROP TABLE IF EXISTS $tmp_table_raw;
DROP TABLE IF EXISTS $tmp_table;

CREATE TABLE $tmp_table_raw AS
SELECT * FROM read_csv_auto('$metrics_file', delim=',', header=False);

CREATE TABLE $tmp_table AS
SELECT
  column00 AS Investigator_Folder,
  column01 AS Project_ID,
  column02 AS Project_run_type,
  column03 AS Release_Date,
  column04 AS RunID,
  column05 AS InstrumentID,
  column06 AS FlowcellID,
  column07 AS Lane,
  column08 AS ProjStatus,
  column09 AS GT_QC_Sample_ID,
  column10 AS Sample_Name,
  column11 AS Species,
  column12 AS name,
  column13 AS value,
  '$Application' AS Application
FROM $tmp_table_raw;

DROP TABLE $tmp_table_raw;

CREATE TABLE IF NOT EXISTS qc_illumina_metrics (
  Investigator_Folder TEXT,
  Project_ID TEXT,
  Project_run_type TEXT,
  Release_Date TEXT,
  RunID TEXT,
  InstrumentID TEXT,
  FlowcellID TEXT,
  Lane TEXT,
  ProjStatus TEXT,
  GT_QC_Sample_ID TEXT,
  Sample_Name TEXT,
  Species TEXT,
  name TEXT,
  value TEXT,
  Application TEXT,
  UNIQUE (
    Investigator_Folder, Project_ID, Project_run_type, Release_Date,
    RunID, InstrumentID, FlowcellID, Lane,
    GT_QC_Sample_ID, Sample_Name, Species, name, value, Application
  )
);

INSERT OR IGNORE INTO qc_illumina_metrics SELECT * FROM $tmp_table;
DROP TABLE $tmp_table;
EOF
    # Check if OOM occurred
      if grep -q "Out of Memory Error" "$duckDB_errorlog"; then
        log_warn "[$Application] ⚠️ DuckDB failed with memory_limit=$mem_limit — retrying with lower limit..."
        continue  # retry with next memory limit
      else
        success=1
        break  # success — exit loop
      fi
    done

    # If all retries failed, report and exit
    if [[ "$success" -eq 0 ]]; then
      log_error "[$Application] ❌ DuckDB insert failed at all memory limits: ${memory_limits[*]}"
      echo "DuckDB insert failure: Out of memory at all fallback levels" >> "$duckDB_logfile"
      log_warn "Resetting all directory inside [$qcdir_file_list] due to memory limits: ${memory_limits[*]}"
      truncate -s 0 "$qcdir_file_list"
      exit 1
    fi

    # If duplicate constraint error, log it
    if grep -q "PRIMARY KEY or UNIQUE constraint violation" "$duckDB_errorlog"; then
      log_warn "[$Application] DUPLICATE DETECTED: $deliveryfolder → $projectId → $GT_QC_Sample_ID → $metric_name"
      echo -e "[$(date)]\t$Application\t$deliveryfolder\t$projectId\t$GT_QC_Sample_ID\t$metric_name" >> "$duckDB_duplicatefile"
    fi
      after_count=$(get_database_row_count_if_table_exists "$duckDB_PATH" "qc_illumina_metrics")
      [[ ! "$after_count" =~ ^[0-9]+$ ]] && after_count=0
      new_rows=$((after_count - before_count))
      [[ "$new_rows" -lt 0 ]] && new_rows=0

      echo "    a. Total in qc_illumina_metrics before insert to database is $before_count" >> "$duckDB_logfile"
      echo "    b. Total in qc_illumina_metrics after insert to database is $after_count" >> "$duckDB_logfile"
      echo "    c. Imported $new_rows new rows (from $SAMPLE_SIZE sample) for $Application → $projectId" >> "$duckDB_logfile"
      echo "--- [END] Import for $Application → $projectId @ $timestamp ---" >> "$duckDB_logfile"
      echo "[$(date)] LOCK RELEASED by job $$ for $Application → $projectId" >> "$duckDB_logfile"
      echo >> "$duckDB_logfile" #insert new line
    ) 200>"$duckDB_lockfile"
    # Remove the stale lock if not already auto removed to allow database get written into by other job
    if [ -f "$duckDB_lockfile" ]; then
      rm -f "$duckDB_lockfile"
    fi    
  else
    log_warn "[$Application] No metrics data to import → $projectId"
  fi
}

#------------------------------------------------------------------
# Function: insert_pacbio_metrics_to_duckdb
# Purpose : Inserts PacBio QC metrics into the DuckDB database.
#           Ensures required columns exist, dynamically adds missing ones,
#           injects Application column, and deduplicates rows.
#------------------------------------------------------------------
insert_pacbio_metrics_to_duckdb() {
  if [[ ! -f "$metrics_csv" ]]; then
    log_error "[$Application] Metrics CSV not found: $metrics_csv"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi
  if [[ ! -f "$duckDB_PATH" ]]; then
    log_warn "[$Application] DuckDB file not found. Creating new: $duckDB_PATH"
    touch "$duckDB_PATH"
  fi

  (
    flock -x 200
    trap 'rm -f "$duckDB_lockfile"' EXIT

    IFS=',' read -r -a headers < <(head -n1 "$metrics_csv")

    # Step 1: Ensure base table exists, including Application
    duckdb "$duckDB_PATH" <<SQL
CREATE TABLE IF NOT EXISTS qc_pacbio_metrics (
  Investigator_Folder TEXT,
  Project_ID TEXT,
  Project_run_type TEXT,
  Release_Date TEXT,
  InstrumentID TEXT,
  Species TEXT,
  ProjStatus TEXT,
  Application TEXT
);
SQL

    # Step 2: Dynamically add new columns if needed
    tmp_alter=".tmp_duckdb_alter.sql"
    rm -f "$tmp_alter" && touch "$tmp_alter"
    for col in "${headers[@]}"; do
      case "$col" in
        Investigator_Folder|Project_ID|Project_run_type|Release_Date|InstrumentID|Species|ProjStatus|Application)
          continue ;;
      esac
      clean_col=$(echo "$col" | sed 's/[^a-zA-Z0-9_]/_/g' | sed 's/__/_/g' | sed 's/_$//')
      echo "ALTER TABLE qc_pacbio_metrics ADD COLUMN IF NOT EXISTS \"$clean_col\" TEXT;" >> "$tmp_alter"
    done
    duckdb "$duckDB_PATH" < "$tmp_alter"
    rm -f "$tmp_alter"

    rm -f "$duckDB_errorlog"

    count_before=$(get_database_row_count_if_table_exists "$duckDB_PATH" qc_pacbio_metrics)
    count_csv=$(($(wc -l < "$metrics_csv") - 1))

    # Step 3: Load CSV into temp table and inject Application column
    duckdb "$duckDB_PATH" <<SQL
DROP TABLE IF EXISTS __tmp_csv_cols__;
CREATE TABLE __tmp_csv_cols__ AS SELECT * FROM read_csv_auto('$metrics_csv', HEADER=TRUE);
ALTER TABLE __tmp_csv_cols__ ADD COLUMN IF NOT EXISTS Application TEXT;
UPDATE __tmp_csv_cols__ SET Application = '$Application';
SQL

    if [[ $? -ne 0 ]]; then
      log_error "[$Application] Failed to load CSV into temp table from: $metrics_csv"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
      cat "$duckDB_errorlog" >&2
      mail -s "[DASHBOARD] Failure importing PacBio metrics $Application" "$Email" <<EOF
[$Application] Failed to load CSV into temp table from: $metrics_csv
Error details from $duckDB_errorlog:
$(cat "$duckDB_errorlog")
EOF
      return 1
    fi

    # Step 4: Get list of common columns
    common_cols=$(duckdb "$duckDB_PATH" -csv -noheader -c "
SELECT name
FROM pragma_table_info('qc_pacbio_metrics')
INTERSECT
SELECT name
FROM pragma_table_info('__tmp_csv_cols__');
")

    # Ensure name/value always last (optional)
    common_cols=$(echo "$common_cols" | grep -v -e '^name$' -e '^value$'; echo name; echo value)
    col_list=$(echo "$common_cols" | paste -sd "," -)

    # Step 5: Insert only new rows (no duplicates)
    duckdb "$duckDB_PATH" <<SQL 2>> "$duckDB_errorlog"
INSERT INTO qc_pacbio_metrics (${col_list})
SELECT ${col_list} FROM __tmp_csv_cols__
EXCEPT
SELECT ${col_list} FROM qc_pacbio_metrics;
DROP TABLE IF EXISTS __tmp_csv_cols__;
SQL

    count_after=$(get_database_row_count_if_table_exists "$duckDB_PATH" qc_pacbio_metrics)
    inserted_count=$((count_after - count_before))

    if [[ $inserted_count -gt 0 ]]; then
      log_info "[$Application] Imported $inserted_count new rows from $(basename "$metrics_csv")"
    elif [[ $inserted_count -eq 0 ]]; then
      log_warn "[$Application] No new rows imported — ALL rows already exist: $(basename "$metrics_csv")"
      echo -e "[$(date)]\tPACBIO\t$(basename "$metrics_csv")\tDUPLICATE" >> "$duckDB_duplicatefile"
    fi

    if grep -q "PRIMARY KEY or UNIQUE constraint violation" "$duckDB_errorlog"; then
      log_warn "[$Application] CONSTRAINT error on $(basename "$metrics_csv")"
    elif [[ -s "$duckDB_errorlog" ]]; then
      log_error "[$Application] Unexpected error importing $(basename "$metrics_csv"). See $duckDB_errorlog"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
    fi
  ) 200>"$duckDB_lockfile"

  if [[ -f "$duckDB_lockfile" ]]; then
    rm -f "$duckDB_lockfile"
  fi
  if [[ -f "$metrics_csv" ]]; then
    rm "$metrics_csv"
  fi
}
#------------------------------------------------------------------
# Function: insert_ont_metrics_to_duckdb
# Purpose : Inserts ONT QC metrics into DuckDB.
#           1. Ensures `qc_ont_metrics` table exists with base columns.
#           2. Dynamically adds any missing CSV-derived columns.
#           3. Loads the CSV into a temp table and injects Application.
#           
#           5. Inserts only new rows (deduplicates via EXCEPT).
#------------------------------------------------------------------
insert_ont_metrics_to_duckdb() {
  if [[ ! -f "$metrics_csv" ]]; then
    log_error "[$Application] Metrics CSV not found: $metrics_csv"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi
  if [[ ! -f "$duckDB_PATH" ]]; then
    log_warn "[$Application] DuckDB file not found. Creating new: $duckDB_PATH"
    touch "$duckDB_PATH"
  fi

  (
    flock -x 200
    trap 'rm -f "$duckDB_lockfile"' EXIT

    IFS=',' read -r -a headers < <(head -n1 "$metrics_csv")

    # Step 1: Ensure base table exists, including Application
    duckdb "$duckDB_PATH" <<SQL
CREATE TABLE IF NOT EXISTS qc_ont_metrics (
  Investigator_Folder TEXT,
  Project_ID TEXT,
  Project_run_type TEXT,
  Release_Date TEXT,
  InstrumentID TEXT,
  Species TEXT,
  ProjStatus TEXT,
  Application TEXT
);
SQL

    # Step 2: Dynamically add new columns if needed
    tmp_alter=".tmp_duckdb_alter.sql"
    rm -f "$tmp_alter" && touch "$tmp_alter"
    for col in "${headers[@]}"; do
      case "$col" in
        Investigator_Folder|Project_ID|Project_run_type|Release_Date|InstrumentID|Species|ProjStatus|Application)
          continue ;;
      esac
      clean_col=$(echo "$col" | sed 's/[^a-zA-Z0-9_]/_/g' | sed 's/__/_/g' | sed 's/_$//')
      echo "ALTER TABLE qc_ont_metrics ADD COLUMN IF NOT EXISTS \"$clean_col\" TEXT;" >> "$tmp_alter"
    done
    duckdb "$duckDB_PATH" < "$tmp_alter"
    rm -f "$tmp_alter"
    
    rm -f "$duckDB_errorlog"

    count_before=$(get_database_row_count_if_table_exists "$duckDB_PATH" qc_ont_metrics)
    count_csv=$(($(wc -l < "$metrics_csv") - 1))

    # Step 3: Load CSV into temp table and inject Application column
    duckdb "$duckDB_PATH" <<SQL
DROP TABLE IF EXISTS __tmp_csv_cols__;
CREATE TABLE __tmp_csv_cols__ AS SELECT * FROM read_csv_auto('$metrics_csv', HEADER=TRUE);
ALTER TABLE __tmp_csv_cols__ ADD COLUMN IF NOT EXISTS Application TEXT;
UPDATE __tmp_csv_cols__ SET Application = '$Application';
SQL

    if [[ $? -ne 0 ]]; then
      log_error "[$Application] Failed to load CSV into temp table from: $metrics_csv"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
      cat "$duckDB_errorlog" >&2
      mail -s "[DASHBOARD] Failure importing ONT metrics $Application" "$Email" <<EOF
[$Application] Failed to load CSV into temp table from: $metrics_csv
Error details from $duckDB_errorlog:
$(cat "$duckDB_errorlog")
EOF
      return 1
    fi

    # Step 4: Get list of common columns
    common_cols=$(duckdb "$duckDB_PATH" -csv -noheader -c "
SELECT name
FROM pragma_table_info('qc_ont_metrics')
INTERSECT
SELECT name
FROM pragma_table_info('__tmp_csv_cols__');
")

    # Ensure name/value always last (optional)
    common_cols=$(echo "$common_cols" | grep -v -e '^name$' -e '^value$'; echo name; echo value)
    col_list=$(echo "$common_cols" | paste -sd "," -)

    # Step 5: Insert only new rows (no duplicates)
    duckdb "$duckDB_PATH" <<SQL 2>> "$duckDB_errorlog"
INSERT INTO qc_ont_metrics (${col_list})
SELECT ${col_list} FROM __tmp_csv_cols__
EXCEPT
SELECT ${col_list} FROM qc_ont_metrics;
DROP TABLE IF EXISTS __tmp_csv_cols__;
SQL

    count_after=$(get_database_row_count_if_table_exists "$duckDB_PATH" qc_ont_metrics)
    inserted_count=$((count_after - count_before))

    if [[ $inserted_count -gt 0 ]]; then
      log_info "[ONT] Imported $inserted_count new rows from $(basename "$metrics_csv")"
    elif [[ $inserted_count -eq 0 ]]; then
      log_warn "[ONT] No new rows imported — ALL rows already exist: $(basename "$metrics_csv")"
      echo -e "[$(date)]\tONT\t$(basename "$metrics_csv")\tDUPLICATE" >> "$duckDB_duplicatefile"
    fi

    if grep -q "PRIMARY KEY or UNIQUE constraint violation" "$duckDB_errorlog"; then
      log_warn "[ONT] CONSTRAINT error on $(basename "$metrics_csv")"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
    elif [[ -s "$duckDB_errorlog" ]]; then
      log_error "[ONT] Unexpected error importing $(basename "$metrics_csv"). See $duckDB_errorlog"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
    fi
  ) 200>"$duckDB_lockfile"

  if [[ -f "$duckDB_lockfile" ]]; then
    rm -f "$duckDB_lockfile"
  fi
  if [[ -f "$metrics_csv" ]]; then
    rm "$metrics_csv"
  fi
}
#------------------------------------------------------------------
# Function: extract_and_process_run_metrics_illumina
# Purpose : Locate the appropriate Run_Metric_Summary file,
#           extract lane-level and instrument metadata, run
#           the gatherer to generate per-lane QC CSV files,
#           assemble the output into a project-level metrics
#           file, and import the result into the DuckDB database.
#           Falls back gracefully if no summary file is found.
#------------------------------------------------------------------

extract_and_process_run_metrics_illumina() {
  # Locate Run_Metric_Summary file (prefer "draft" if available)
  if [[ -f "$ProjDir/Run_Metric_Summary.draft.csv" ]]; then
    RunMetricsSummary="$ProjDir/Run_Metric_Summary.draft.csv"
  elif [[ -f "$ProjDir/Run_Metric_Summary.csv" ]]; then
    RunMetricsSummary="$ProjDir/Run_Metric_Summary.csv"
  elif [[ -f "$ProjDir/package/Run_Metric_Summary.draft.csv" ]]; then
    RunMetricsSummary="$ProjDir/package/Run_Metric_Summary.draft.csv"
  elif [[ -f "$ProjDir/package/Run_Metric_Summary.csv" ]]; then
    RunMetricsSummary="$ProjDir/package/Run_Metric_Summary.csv"
  else
    RunMetricsSummary=""
  fi

  # If summary file exists, extract instrument/run metadata
  if [[ -n "$RunMetricsSummary" ]]; then
    InstrumentID=$(grep -A1 "FlowCellID" "$RunMetricsSummary" | tr ',' '\t' | sed 's+"++g' |
                   awk 'NR==1{for(i=1;i<=NF;i++) if($i=="MachineID"){pos=i}} NR==2{print $pos}')
    FlowcellID=$(grep -A1 "FlowCellID" "$RunMetricsSummary" | tr ',' '\t' | sed 's+"++g' |
                 awk 'NR==1{for(i=1;i<=NF;i++) if($i=="FlowCellID"){pos=i}} NR==2{print $pos}')
    RunID=$(grep "Run Id" "$ProjDir/RunInfo.xml" | sed 's/=/\t/g' | awk '{print $3}' | sed 's/"//g')

    ColumnNamesSubset='Lane|ClusterPF|Reads|ReadsPF|%>=Q30|Yield|Error|Aligned'
    LaneMetColNum=$(grep -A"$(sed -n '/Cluster PF/,$p' "$RunMetricsSummary" | wc -l)" "Cluster PF" "$RunMetricsSummary" |
                    sed -n '/Read 2 (I)/q;p' | tr ',' '\t' |
                    awk 'NR==1; ($2~/^-$/)' | head -n1 | tr '\t' '\n' | sed 's+ ++g' |
                    grep -xnE "$ColumnNamesSubset" | cut -d: -f1 | paste -sd,)

    LaneMetrics=$(grep -A"$(sed -n '/Cluster PF/,$p' "$RunMetricsSummary" | wc -l)" "Cluster PF" "$RunMetricsSummary" |
                  sed -n '/Read 2 (I)/q;p' | tr ',' '\t' |
                  awk 'NR==1; ($2~/^-$/)' | cut -f "$LaneMetColNum" | sed '1d')

    LaneTotal=$(echo "$LaneMetrics" | wc -l)

    for Lanes in $(seq 1 "$LaneTotal"); do
      Lane=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f1 | xargs)
      Cluster_PF=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f2 | xargs)
      Reads=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f3 | xargs)
      Reads_PF=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f4 | xargs)
      PCT_Q30_and_More=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f5 | xargs)
      Yield=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f6 | xargs)
      Aligned=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f7 | xargs)
      Error=$(echo "$LaneMetrics" | sed -n "${Lanes}p" | cut -f8 | xargs)

      lane_tmp="$OUT/${projectId}_QCreport.$Application.$n.lane${Lane}.csv"
      if ! gather_illumina_metrics_js > "$lane_tmp"; then
        log_error "[$Application] Skipping → $projectId (lane $Lane): pivot/gather script failed or missing Reads_Total"
        grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
        mv "$nonarchive_tmp_file" "$qcdir_file_list"
        log_info "[$Application] will recollect $ProjDir in future gather"
        rm -f "$lane_tmp"
        continue
      fi
      cat "$lane_tmp" >> "$OUT/${projectId}_QCreport.$Application.$n.csv"
      rm -f "$lane_tmp"
    done
  else
    InstrumentID=NULL; FlowcellID=NULL; RunID=NULL; Lane=NULL
    Cluster_PF=NULL; Reads=NULL; Reads_PF=NULL
    PCT_Q30_and_More=NULL; Yield=NULL; Aligned=NULL; Error=NULL

    lane_tmp="$OUT/${projectId}_QCreport.$Application.$n.lane${Lane}.csv"
    if ! gather_illumina_metrics_js > "$lane_tmp"; then
      log_warn "[$Application] Skipping → $projectId (no RunMetricsSummary): pivot failed or Reads_Total missing"
      grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
      mv "$nonarchive_tmp_file" "$qcdir_file_list"
      log_info "[$Application] will recollect $ProjDir in future gather"
      rm -f "$lane_tmp"
      return 1
    fi
    cat "$lane_tmp" > "$OUT/${projectId}_QCreport.$Application.$n.csv"
    rm -f "$lane_tmp"
  fi

  final_metrics_file="$OUT/${projectId}_QCreport.$Application.$n.csv"
  if [[ -s "$final_metrics_file" ]]; then
    duckDB_call
  else
    log_warn "[$Application] No metrics file created → $projectId; skipping DB import"
    grep -v "$ProjDir" "$qcdir_file_list" > "$nonarchive_tmp_file"
    mv "$nonarchive_tmp_file" "$qcdir_file_list"
    log_info "[$Application] will recollect $ProjDir in future gather"
    return 1
  fi

  rm -f "$metrics_csv" "$metrics_log" "$final_metrics_file" $OUT/*.log 
}

#------------------------------------------------------------------
# Function: database
# Purpose : Core logic to collect and update QC metrics from each
#           project folder into the main metrics database
#------------------------------------------------------------------
database() {
  for n in $(seq 1 "$ProjTotal"); do
    ProjDir=$(echo -en "$ProjDirs\n" | sed -n "${n}p")
    
    # ###Skip qc folder with non "package" or "release" signature as they may not have been processed. This way, the folder can be re-checked later
    ###However, collect metrics from such folder if the directory is in .qifa.qc-archive, determinable by day_5_countdown
    if [[ -f "$pulling_undelivered_qc_flag" || "$day_5_countdown" -ge 5 ]]; then
      QCdelivery_check_status # Pulling undelivered projects: skip metadata check
    else
      QCdelivery_check_status
      if ! QCdir_updatelist; then
        log_warn "[$Application] Skipping → $$ProjDir due to NULL delivery metadata. Will retry in future runs."
        continue;
      fi
    fi
    #Handle return 1 error graciously 
    if [[ "$Application" == "PacBio" ]]; then
      if ! gather_metrics_pacbio; then
        log_warn "[$Application] Skipping → $projectId due to PacBio metrics failure"
        continue
      fi
      if ! insert_pacbio_metrics_to_duckdb; then
        log_warn "[$Application] Skipping → $projectId: DB insert failed"
        continue
      fi
    elif [[ "$Application" == "ONT" ]]; then
      if ! gather_metrics_ont; then
        log_warn "[$Application] Skipping → $projectId due to ONT metrics failure"
        continue
      fi
      if ! insert_ont_metrics_to_duckdb; then
        log_warn "[$Application] Skipping → $projectId: DB insert failed"
        continue
      fi
    else
      if ! extract_and_process_run_metrics_illumina; then
        log_warn "[$Application] Skipping → $projectId due to Illumina metrics failure"
        continue
      fi
    fi
  done
  [[ -f "$pulling_undelivered_qc_flag" ]] && rm "$pulling_undelivered_qc_flag"
}
# ─────────────────────────────────────────────────────────────
# Function: duckDB_call_seq
# Purpose:
#   Handles conditional import of SequencingMetrics.csv into `sequencing_metrics` table in DuckDB.
#   It performs the following:
#     - Skips if input file hasn't changed since last import
#     - Loads CSV using read_csv() with null-padding and error-tolerant options
#     - Ensures `sequencing_metrics` table exists and avoids duplicates with `INSERT OR IGNORE`
#     - Uses flock to guard concurrent DB access
#     - Logs row counts and status
#     - Updates backup copy on successful import

duckDB_call_seq() {
  seq_metrics_file="$OUT/SeqMetrics/SequencingMetrics.csv"
  seq_tmp_table_raw="sequencing_metrics_raw"
  seq_tmp_table="sequencing_metrics_tmp"
  timestamp=$(date)
  seqMet_lastimport_file="$last_import_push_dir/SequencingMetrics.lastimport.csv"

  # Reset log if older than 31 days
  [[ -f "$duckDB_logfile" ]] && find "$duckDB_logfile" -mtime +31 -type f -delete

  # Ensure log file exists
  touch "$duckDB_logfile"

  # If backup file doesn't exist, create an empty one
  [[ ! -f "$seqMet_lastimport_file" ]] && touch "$seqMet_lastimport_file"

  # Early exit if file hasn't changed since last import
  if cmp -s "$seq_metrics_file" "$seqMet_lastimport_file"; then
    echo "[$(date)] Skipping import — SequencingMetrics.csv is unchanged from last import." >> "$duckDB_logfile"
    return 0
  fi

  if [[ -s "$seq_metrics_file" ]]; then
    (
      flock -x 200

      echo "[$(date)] LOCK ACQUIRED by job $$ for Sequencing Metrics" >> "$duckDB_logfile"
      echo "--- [START] Import for Sequencing Metrics @ $timestamp ---" >> "$duckDB_logfile"

      before_count=$(get_database_row_count_if_table_exists "$duckDB_PATH" "sequencing_metrics")
      [[ ! "$before_count" =~ ^[0-9]+$ ]] && before_count=0

      duckdb "$duckDB_PATH" <<EOF 2>> "$duckDB_errorlog"
DROP TABLE IF EXISTS $seq_tmp_table_raw;
DROP TABLE IF EXISTS $seq_tmp_table;

CREATE TABLE $seq_tmp_table_raw AS
SELECT * FROM read_csv('$seq_metrics_file',
  delim = ',',
  header = true,
  ignore_errors = true,
  null_padding = true,
  quote = '"',
  escape = '"'
);

CREATE TABLE $seq_tmp_table AS
SELECT * FROM $seq_tmp_table_raw;

DROP TABLE $seq_tmp_table_raw;

CREATE TABLE IF NOT EXISTS sequencing_metrics (
  Month INTEGER,
  Year INTEGER,
  Project TEXT,
  Site TEXT,
  InstrumentID TEXT,
  RunFolder TEXT,
  groupFolder TEXT,
  Application TEXT,
  FullPath TEXT,
  Platform TEXT,
  Reads BIGINT,
  Bases BIGINT,
  Bytes BIGINT,
  DeliveryDirectory TEXT,
  SampleSize INTEGER,
  PolymeraseReadLength_bp_Mean DOUBLE,
  UNIQUE (
    Month, Year, Project, Site, InstrumentID, RunFolder,
    groupFolder, Application, FullPath, Platform, Reads, Bases,
    Bytes, DeliveryDirectory, SampleSize, PolymeraseReadLength_bp_Mean
  )
);

INSERT OR IGNORE INTO sequencing_metrics SELECT * FROM $seq_tmp_table;
DROP TABLE $seq_tmp_table;
EOF

      after_count=$(get_database_row_count_if_table_exists "$duckDB_PATH" "sequencing_metrics")
      [[ ! "$after_count" =~ ^[0-9]+$ ]] && after_count=0

      new_rows=$((after_count - before_count))
      [[ "$new_rows" -lt 0 ]] && new_rows=0

      echo "    a. Total in sequencing_metrics before insert to database is $before_count" >> "$duckDB_logfile"
      echo "    b. Total in sequencing_metrics after insert to database is $after_count" >> "$duckDB_logfile"

      if [[ "$new_rows" -eq 0 ]]; then
        echo "    ⚠️  Skipping import: All rows already exist — no new rows added." >> "$duckDB_logfile"
      else
        echo "    c. Imported $new_rows new rows to sequencing_metrics" >> "$duckDB_logfile"
      fi

      echo "--- [END] Import for Sequencing Metrics @ $timestamp ---" >> "$duckDB_logfile"
      echo "[$(date)] LOCK RELEASED by job $$ for Sequencing Metrics" >> "$duckDB_logfile"
      echo >> "$duckDB_logfile"

      # Save latest as last import
      cp "$seq_metrics_file" "$seqMet_lastimport_file"

    ) 200>"$duckDB_lockfile"

    [[ -f "$duckDB_lockfile" ]] && rm -f "$duckDB_lockfile"
  else
    log_warn "[Sequencing Metrics] No metrics data to import — file is missing or empty"
  fi
}
# ─────────────────────────────────────────────────────────────
# Create or Refresh Summary Index Table: qc_app_index
# ─────────────────────────────────────────────────────────────
qc_app_index_table() {
  (
    flock -x 200
    timestamp=$(date)
    echo "[$(date)] LOCK ACQUIRED by job $$ for qc_app_index_table" >> "$duckDB_logfile"
    echo "--- [START] Index table update @ $timestamp ---" >> "$duckDB_logfile"

    duckdb "$duckDB_PATH" <<'EOF'
CREATE OR REPLACE TABLE qc_app_index AS
SELECT Application, Investigator_Folder, COUNT(DISTINCT Sample_Name) AS Sample_Count
FROM qc_illumina_metrics
WHERE Application IS NOT NULL AND Investigator_Folder IS NOT NULL
GROUP BY Application, Investigator_Folder
UNION ALL
SELECT Application, Investigator_Folder, COUNT(DISTINCT Sample_Name) AS Sample_Count
FROM qc_pacbio_metrics
WHERE Application IS NOT NULL AND Investigator_Folder IS NOT NULL
GROUP BY Application, Investigator_Folder
UNION ALL
SELECT Application, Investigator_Folder, COUNT(DISTINCT Sample_ID) AS Sample_Count
FROM qc_ont_metrics
WHERE Application IS NOT NULL AND Investigator_Folder IS NOT NULL
GROUP BY Application, Investigator_Folder;
EOF

    echo "--- [END] Index table update @ $timestamp ---" >> "$duckDB_logfile"
    echo "[$(date)] LOCK RELEASED by job $$ for qc_app_index_table" >> "$duckDB_logfile"
    echo >> "$duckDB_logfile"
  ) 200>"$duckDB_lockfile"
  [[ -f "$duckDB_lockfile" ]] && rm -f "$duckDB_lockfile"
}
# ─────────────────────────────────────────────────────────────
# A reusable code for creating index for different tables used for 
#performance optimization tool in dashboard
# ─────────────────────────────────────────────────────────────
create_qc_metrics_indexes() {
  local table="$1"
  (
    flock -x 200
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] LOCK ACQUIRED by $$ for create_qc_metrics_indexes on $table" >> "$duckDB_logfile"
    echo "--- [START] create_qc_metrics_indexes for $table @ $timestamp ---" >> "$duckDB_logfile"

    duckdb "$duckDB_PATH" <<EOF
CREATE INDEX IF NOT EXISTS idx_${table}_app_release
  ON $table(Application, Release_Date);

CREATE INDEX IF NOT EXISTS idx_${table}_lab_proj_run
  ON $table(Investigator_Folder, Project_ID, Project_run_type);
EOF

    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "--- [END] create_qc_metrics_indexes for $table @ $timestamp ---" >> "$duckDB_logfile"
    echo "[$timestamp] LOCK RELEASED by $$ for create_qc_metrics_indexes on $table" >> "$duckDB_logfile"
    echo >> "$duckDB_logfile"
  ) 200>"$duckDB_lockfile"
  [[ -f "$duckDB_lockfile" ]] && rm -f "$duckDB_lockfile"
}
# ─────────────────────────────────────────────────────────────
# Create a Unified App Lookup Table for all the three indexes: illumina, ont, and pacbio. 
# This helps with dashboard optimization
# ─────────────────────────────────────────────────────────────
create_unified_qc_view() {
  (
    flock -x 200
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] LOCK ACQUIRED by $$ for create_unified_qc_view" >> "$duckDB_logfile"
    echo "--- [START] create_unified_qc_view @ $timestamp ---" >> "$duckDB_logfile"

    duckdb "$duckDB_PATH" <<EOF
DROP VIEW IF EXISTS unified_qc_view;

CREATE VIEW unified_qc_view AS
SELECT DISTINCT Application, Investigator_Folder, 'qc_illumina_metrics' AS Source FROM qc_illumina_metrics
UNION
SELECT DISTINCT Application, Investigator_Folder, 'qc_pacbio_metrics' AS Source FROM qc_pacbio_metrics
UNION
SELECT DISTINCT Application, Investigator_Folder, 'qc_ont_metrics' AS Source FROM qc_ont_metrics;
EOF

    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "--- [END] create_unified_qc_view @ $timestamp ---" >> "$duckDB_logfile"
    echo "[$timestamp] LOCK RELEASED by $$ for create_unified_qc_view" >> "$duckDB_logfile"
    echo >> "$duckDB_logfile"
  ) 200>"$duckDB_lockfile"
  [[ -f "$duckDB_lockfile" ]] && rm -f "$duckDB_lockfile"
  #clean up any hanging csv file
  rm $OUT/*.csv $OUT/*.log
}

# ─────────────────────────────────────────────────────────────
# Push database to ctgenometech03 server
# ─────────────────────────────────────────────────────────────
destination_server() {
  #── Declare all counters as integers, init to 0 ─────────────
  declare -i qc_rows_before=0 qc_rows_after=0 qc_delta_rows=0
  declare -i qc_samples_before=0 qc_samples_after=0 qc_delta_samples=0
  declare -i seq_rows_before=0 seq_rows_after=0 seq_delta_rows=0

  #─ Bail out if DB missing ───────────────────────────────────
  if [[ ! -f "$duckDB_PATH" ]]; then
    log_error "[DuckDB Missing] $duckDB_PATH not found"
    {
      echo "From: GTdrylab@jax.org"
      echo "To:   $Email"
      echo "Subject: ALERT: [DASHBOARD] GT Metrics Database Missing"
      echo ""
      echo "Error: DuckDB file $duckDB_PATH not found on $(hostname) at $(date)."
    } | sendmail -t -f GTdrylab@jax.org
    exit 1
  fi

  log_info "[INFO] Starting final push to $push_server at $(date)"

  #── Reliable run_count helper via CSV/noheader ─────────────
  run_count() {
    local raw
    raw=$(duckdb "$1" -csv -noheader -c "$2" 2>/dev/null)
    raw=${raw//[!0-9]/}
    echo "${raw:-0}"
  }

  #── 1) Fetch “after” counts ────────────────────────────────
  qc_rows_after=$(run_count "$duckDB_PATH" "SELECT COUNT(*) FROM qc_illumina_metrics;")
  qc_samples_after=$(run_count "$duckDB_PATH" "SELECT COUNT(DISTINCT Sample_Name) FROM qc_illumina_metrics;")
  seq_rows_after=$(run_count "$duckDB_PATH" "SELECT COUNT(*) FROM sequencing_metrics;")

  #── 2) If snapshot exists, fetch “before” counts ───────────
  if [[ -s "$duckDB_lastpush" ]]; then
    qc_rows_before=$(run_count "$duckDB_lastpush" "SELECT COUNT(*) FROM qc_illumina_metrics;")
    qc_samples_before=$(run_count "$duckDB_lastpush" "SELECT COUNT(DISTINCT Sample_Name) FROM qc_illumina_metrics;")
    seq_rows_before=$(run_count "$duckDB_lastpush" "SELECT COUNT(*) FROM sequencing_metrics;")

    # compute deltas, swallow zero‐result exit codes
    (( qc_delta_rows    = qc_rows_after    - qc_rows_before    ))    || true
    (( qc_delta_samples = qc_samples_after - qc_samples_before )) || true
    (( seq_delta_rows   = seq_rows_after   - seq_rows_before   ))   || true

    email_header="DuckDB pushed to production server at $(date)."
  else
    # initial load: before=0, everything is new
    qc_rows_before=0
    qc_delta_rows=$qc_rows_after

    qc_samples_before=0
    qc_delta_samples=$qc_samples_after

    seq_rows_before=0
    seq_delta_rows=$seq_rows_after

    email_header="Initial load completed at $(date)."
  fi
  #── 3) If no new rows on an update, skip ──────────────────
  if [[ -s "$duckDB_lastpush" ]] && (( qc_delta_rows == 0 && seq_delta_rows == 0 )); then
    log_info "[DuckDB] No new rows. Skipping push."
    return 0
  fi
  #── 4) Push the DB under lock ──────────────────────────────
  (
    flock -s 200
    rsync -vahP "$duckDB_PATH" "$push_server"
  ) 200>"$duckDB_lockfile"
  [[ -f "$duckDB_lockfile" ]] && rm -f "$duckDB_lockfile"

  #── 5) Snapshot the new DB ─────────────────────────────────
  cp "$duckDB_PATH" "$duckDB_lastpush"

  #── 6) Compute & humanize file size ────────────────────────
  file_bytes=$(stat -c "%s" "$duckDB_PATH")
  file_size=$(numfmt --to=iec --format="%.1f" "$file_bytes")

  #── 7) Build & send update email ───────────────────────────
  {
    echo "From: GTdrylab@jax.org"
    echo "To:   $Email"
    echo "Subject: [DASHBOARD] GT Metrics Database Update"
    echo ""
    echo "$email_header"
    echo ""
    echo "QC_METRICS table updated:"
    echo "  • Rows before:          $qc_rows_before"
    echo "  • Rows after:           $qc_rows_after"
    echo "  • New rows injected:    $qc_delta_rows"
    echo "  • Total samples:        $qc_samples_after"
    echo "  • New samples injected: $qc_delta_samples"
    echo ""
    echo "SEQUENCING_METRICS table updated:"
    echo "  • Rows before:          $seq_rows_before"
    echo "  • Rows after:           $seq_rows_after"
    echo "  • New rows injected:    $seq_delta_rows"
    echo ""
    echo "Destination: $push_server"
    echo "File size:   $file_size"
  } | sendmail -t -f GTdrylab@jax.org

  log_info "[DuckDB Push] Completed and email sent."
}
# ─────────────────────────────────────────────────────────────
# Function: catch_email_failure_db
# Purpose:
#   Catch any non-zero EXIT from the pipeline and send an
#   alert email with the last error message from the log.
#
#   Also logs which $Application (if set) failed and suggests
#   manual inspection or rollback of GTdashboardMetrics.duckdb.
#
# ─────────────────────────────────────────────────────────────
function catch_email_failure_db {
  local exit_code=$?
  local logfile="$OUT/.logfile"
  local app_msg="Application: ${Application:-N/A}"
  local email_subject="FAILURE: GT metrics update"
  local body

  if [[ "$exit_code" -ne 0 ]]; then
    ErrMsg1="Unknown error"

    if [[ -s "$logfile" ]]; then
      ErrMsg1=$(<"$logfile")
    fi

    body=$(cat <<EOF
Failure during GT metrics update.

${app_msg}
Exit Code: $exit_code

No new update was pushed to dashboard server.

Error Log:
$ErrMsg1

⚠️ Potential Database Integrity Notice
This is unlikely, but in the event that GTdashboardMetrics.duckdb was partially written before this failure, you may want to inspect the file manually or re-run the application to ensure database integrity.

Recommended Actions:
ACTION 1: Check the timestamp of GTdashboardMetrics.duckdb and compare it to the time this email was received.
ACTION 2: If the timestamp has changed unexpectedly, inspect the database by comparing it with its most recent snapshot in .last_import_push/.
ACTION 3: Identify and fix the cause of the failure. Then remove any of the following temporary files (if present) inside .whitelist_QCdir/: 
          a. *.QCDir_nonarchive.txt
          b. *.QCDir_nonarchive.update.txt
ACTION 4: If necessary, restore the previous known-good snapshot: Copy the latest valid snapshot from .last_import_push/GTdashboardMetrics.duckdb.
          Ensure the snapshot is not empty. Re-run the script after restoring.
Note: These steps may usually not required. The script is designed with safeguards to prevent corruption of the database under most circumstances
EOF
)
    echo -e "$body" | mail -s "$email_subject" "$Email"
    echo "[ERROR] Email notification sent for failure."

    : > "$logfile"
  fi
}

# ─────────────────────────────────────────────────────────────
# ERROR & EXIT TRAPS — Ensures graceful failure with email alerts
#
# 1. ERR trap: calls catch_error() when any command fails.
#    - Logs the line number and command
#    - Sends failure email
#
# 2. EXIT trap: calls catch_email_failure_db() when script exits
#    - If error code ≠ 0, emails error message and application status
#
# NOTE: These must be placed after the functions are defined
# ─────────────────────────────────────────────────────────────
trap 'catch_error $LINENO "$BASH_COMMAND"' ERR
trap 'catch_email_failure_db' EXIT
#
# ─────────────────────────────────────────────────────────────
# Function: pipeline
# Purpose : Validate and expose the pipeline list file
# ─────────────────────────────────────────────────────────────
pipelinelist() {
  if [[ -z "$Email" || -z "$qifaPipelineDir" ]]; then
    log_error "Required variables Email or qifaPipelineDir are not set."
    exit 1
  fi

  PIPELINE_LIST="$qifaPipelineDir/qifa-qc-scripts/pipelinelist/pipelinelist.txt"
  if [[ ! -f "$PIPELINE_LIST" ]]; then
    log_error "Missing $PIPELINE_LIST"
    echo -e "ERROR: Missing pipeline list at:\n$PIPELINE_LIST\n\nExpected format:\n  atacseq\n  rnaseq\n  wgs\n\nScript aborted!" \
      | mail -s "GT metrics dashboard script missing pipelinelist.txt" "$Email"
    exit 1
  fi
}
# ─────────────────────────────────────────────────────────────
# Call pipeline() to validate and expose PIPELINE_LIST
# ─────────────────────────────────────────────────────────────
pipelinelist
# ─────────────────────────────────────────────────────────────
# Main loop through pipeline list
# ─────────────────────────────────────────────────────────────
while IFS= read -r line || [[ -n "$line" ]]; do
  Application=$(echo "$line" | xargs)  # trim whitespace
  [[ -z "$Application" || "$Application" =~ ^# ]] && continue
  export Application  # ensure downstream functions see this

  if ! init_command_per_app; then
    log_warn "SKIPPING→→→[$Application] failed or has no valid QC directories. Proceeding to next application..."
    continue
  fi
done < "$PIPELINE_LIST"


# One-time operations after all apps are done
init_command_once