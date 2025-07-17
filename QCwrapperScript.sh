#!/bin/bash
#Author: Raman Akinyanju Lawal
#Date: Oct. 2023

##To do
#hwo does program handle when job is submitted but remain in slurm queue

function helpFunction()
{
   echo "##########################################################################################################"
   echo "Wrapper script to perform qifa-qc pipeline. "
   echo "Script must be initiated within the qc directory, else, program will quit"
   echo
   echo "Recommended to run in tmux session. e.g. "
   echo "<tmux> to initiate a new session; <tmux a> to recover existing session"
   echo -e '<"Ctrl b" and then "c"> to create new terminal within existing tmux session'
   echo -e '<"Ctrl b" and then "p"> to navigate to previous tmux session or <"Ctrl b" and then "n"> for next session'
   echo "###########################################################################################################"
   echo
   echo "Usage: qifa-script [-e email address]"
   echo 
   echo "required:"
   #echo -e "-b\tbasic pipeline"
   echo -e "-e\tUser email address <a.b@jax.org>. Needed for later usage during fastq delivery and email notification"
   echo "options:"
   echo -e "-q\tSpecify name of single pipeline e.g. -q chipseq"
   echo -e "-f\tSpecify a filename containing multiple pipelines to run, one per line. Can handle as many as possible"
   echo -e "-p\tName of project e.g. 23-gtct-001"
   echo -e "\tNote: -p don't need to be specified. qifa-script will prompt you if it needs to be specified"
   echo -e "-d\tQC directory. No need to specify. Program will automatically determine this for later usage"  
   echo -e "-u\tUsername. No need to specify. Program will automatically determine this for later usage"  
   echo -e "-h\tShow help."
   echo
}
#b) basic=$OPTARG;;
usage="Usage: qifa-script [-e email address]. see qifa-script -h"
options=':hq:e:f:p:u:d:'
while getopts $options option; do
  case "$option" in
    h) helpFunction; exit;;
    q) Pipeline=$OPTARG;;
    f) Multipipelines=$OPTARG;;
    e) email=$OPTARG;;
    p) projectID=$OPTARG;;
    d) qcDir=$OPTARG;;
    u) username=$OPTARG;;
    :) printf "missing argument for -%s\n" "$OPTARG"; echo "$usage" >&2; exit 1;;
   \?) printf "Invalid option: -%s\n" "$OPTARG" >&2; echo "$usage" >&2; exit 1;;
  esac
done
########################################################
# mandatory arguments
if [ ! "$email" ]; then
	echo
  	echo "Missing: -e is required. see qifa-script -h"
  	echo "";
  	exit 1
fi

########################################################
# Conflicting argument
if [[ "$Pipeline" && "$Multipipelines" ]]; then
	echo ''
	echo "ERROR: Conflict detected."
	echo -e '\tSpecify only -q or -f but not both. see qifa-script -h'
	echo ''
	exit 1;
fi

########################################################
#Variables
Name="${email%%.*}"
gather_wait_time_pre1="30"
gather_wait_time_pre2="10"
gather_wait_time="180"
basic="basic"
########################################################
#Set directory so qifa-script have no issue when entering sudo
if [[ "$qcDir" ]]; then
	qcFolder=$qcDir
	cd $qcFolder;
else
	export DESTDIR=.
	qcFolder=$(pwd $DESTDIR)
fi
########################################################
Project_type="${qcFolder##*/}"
projectFinal=$(grep projectFinal $qcFolder/.settings.json | awk '{print $2}' | grep -o '".*"' | sed 's/"//g');
########################################################

if [[ -z "$projectID" ]]; then
	if grep -q "projectId" $qcFolder/.settings.json; then
		Project_ID=$(grep "projectId" $qcFolder/.settings.json | awk '{print $2}' | grep -o '".*"' | sed 's/"//g');
	else
		echo ''
		echo -e 'ERROR: "projectId" cannot be found in the .settings.json'
		echo -e 'ACTION: you can still specify the project ID with "-p" on the command line'
		echo ''
		exit 1;
	fi
else
	Project_ID=$projectID
fi

########################################################
#Check pipeline list

function pipeline_file_check {
pipelinelist=/gt/research_development/qifa/elion/software/qifa-ops/0.1.0/qifa-qc-scripts/pipelinelist/pipelinelist.txt
if [ ! -f "$pipelinelist" ]; then
	echo "ERROR: Missing list of pipelinelist.txt"
	echo -e '\tPipeline location: /gt/research_development/qifa/elion/software/qifa-ops/0.1.0/qifa-qc-scripts/pipelinelist/'
	exit 1;
fi
}

########################################################

module use --append /gt/research_development/qifa/elion/modulefiles/
module load qifa-ops/

#Determine QC directory with ReportFile
function qc_folder {
if (echo $Project_type | grep -q "_mm1") || (echo $Project_type | grep -q "_mm0"); then
	echo -e 'INFO: You are in '$qcFolder' directory'
	echo -e 'INFO: If this is not the qc directory, then kill your job and navigate to the qc folder';
else
	echo -e 'ERROR: It does not appear you are in QC directory'
	echo -e 'Program Aborted!'
	exit 1;
fi
}
# 
#####Stop job if group "seqdata" is not invoked
#grp=$(stat -c "%G" .)
function grp_id {
	grp=$(id -gn)
	if [[ "$grp" != "seqdata" ]]; then
		echo -e 'Your current active group is "'$grp'"'
		echo -e 'For interoperability amongst QC team members, it should be "seqdata".'
		echo -e 'ACTION: Please run the following command before re-running this program.'
      	echo -e '\tnewgrp seqdata'
      	exit 1;
    fi
}
#

function check_settings_folders {
    	if [[ ! -f ".settings.json" ]] ; then
    		echo -e '".settings.json" is missing in QC folder'
    	elif [[ ! -d "fastqs" ]]; then
    		echo ''
    		echo -e 'ERROR: "fastqs" folder does not exist!'
    	elif [[ ! -f "Stats.json.unprocessed" ]]; then
    		echo ''
    		echo -e 'ERROR: "Stats.json.unprocessed"  is missing in QC folder'
    	fi
}

#
function qifa_catch_error {
    if [[ -f "${Project_ID}_QCreport.${basic}.log" ]]; then
    	if grep -q "Unrecognized platform by QiFa" ${Project_ID}_QCreport.${basic}.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${basic}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${basic}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
    		echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${basic}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	elif grep -q "Program aborted" ${Project_ID}_QCreport.${basic}.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${basic}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${basic}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${basic}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	elif grep -q "0 .gz file(s) found" ${Project_ID}_QCreport.${basic}.log ; then
    		echo ''
    		echo 'ERROR: 0 .gz file(s) found'
    		echo -e 'INFO: Check '${Project_ID}_QCreport.${basic}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${basic}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
     	fi
   	elif  [[ -f "${Project_ID}_QCreport.${basic}.package.log" ]]; then
   		if grep -q "Program aborted" ${Project_ID}_QCreport.${basic}.package.log ; then
    		echo ''
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${basic}.package.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${basic}.package.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
     	fi
   	elif  [[ -f "${Project_ID}_QCreport.${basic}.release.log" ]]; then
   		if grep -q "Program aborted" ${Project_ID}_QCreport.${basic}.release.log ; then
    		echo ''
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${basic}.release.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${basic}.release.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
     	fi
    elif [[ -f "${Project_ID}_QCreport.${basic}.gather.log" ]]; then
    	if grep -q '\!!!WARNING!!!' ${Project_ID}_QCreport.${basic}.gather.log ; then
    		report=$(awk '/##/{p=0}p;/!!!WARNING!!!/{p=1}' ${Project_ID}_QCreport.${basic}.gather.log)
    		echo ''
    		echo -e '!!!WARNING!!!'
    		echo -en "$report\n"
    		echo ''
    		echo -e 'INFO: Check '${Project_ID}_QCreport.${basic}.gather.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${basic}.gather.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
 	elif [[ -f "${Project_ID}_QCreport.${Pipeline}.log" ]]; then
    	if grep -q "Unrecognized platform by QiFa" ${Project_ID}_QCreport.$Pipeline.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${Pipeline}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
			echo ''
    		exit 1;
		elif grep -q "Program aborted" ${Project_ID}_QCreport.${Pipeline}.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${Pipeline}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
		elif grep -q "ERROR: Please complete basic QC before proceeding with" ${Project_ID}_QCreport.${Pipeline}.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${Pipeline}.log
      		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	elif grep -q "0 .gz file(s) found" ${Project_ID}_QCreport.${Pipeline}.log ; then
    		echo ''
    		echo 'ERROR: 0 .gz file(s) found'
    		echo ''
    		echo -e 'INFO: Check '${Project_ID}_QCreport.${Pipeline}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	elif grep -q "ERROR: loadQCParametersSetting organism" ${Project_ID}_QCreport.${Pipeline}.log ; then
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi			
    elif [[ -f "${Project_ID}_QCreport.${Pipeline}.gather.log" ]]; then
    	if grep -q '\!!!WARNING!!!' ${Project_ID}_QCreport.${Pipeline}.gather.log ; then
    		report=$(awk '/##/{p=0}p;/!!!WARNING!!!/{p=1}' ${Project_ID}_QCreport.${Pipeline}.gather.log)
    		echo ''
    		echo -e '!!!WARNING!!!'
    		echo -en "$report\n"
    		echo ''
    		echo -e 'INFO: Check '${Project_ID}_QCreport.${Pipeline}.gather.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.gather.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	elif grep -q 'ERROR: Could not locate output from QC pipeline' ${Project_ID}_QCreport.${Pipeline}.gather.log ; then
     		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.gather.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.gather.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
    elif  [[ -f "${Project_ID}_QCreport.${Pipeline}.package.log" ]]; then
   		if grep -q "Program aborted" ${Project_ID}_QCreport.${Pipeline}.package.log ; then
    		echo ''
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.package.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.package.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
   	elif  [[ -f "${Project_ID}_QCreport.${Pipeline}.release.log" ]]; then
   		if grep -q "Program aborted" ${Project_ID}_QCreport.${Pipeline}.release.log ; then
    		echo ''
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Pipeline}.release.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.${Pipeline}.release.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
	elif [[ -f "${Project_ID}_QCreport.${Multi_pipelines}.log" ]]; then
    	if grep -q "Unrecognized platform by QiFa" ${Project_ID}_QCreport.$Multi_pipelines.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${Multi_pipelines}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.$Multi_pipelines.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.$Multi_pipelines.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
		elif grep -q "Program aborted" ${Project_ID}_QCreport.${Multi_pipelines}.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${Multi_pipelines}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.$Multi_pipelines.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.$Multi_pipelines.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
		elif grep -q "ERROR: Please complete basic QC before proceeding with rnaseq QC" ${Project_ID}_QCreport.${Multi_pipelines}.log ; then
    		echo ''
    		cat ${Project_ID}_QCreport.${Multi_pipelines}.log
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.$Multi_pipelines.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.$Multi_pipelines.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
    elif [[ -f "${Project_ID}_QCreport.${Multi_pipelines}.gather.log" ]]; then
    	if grep -q '\!!!WARNING!!!' ${Project_ID}_QCreport.${Multi_pipelines}.gather.log ; then
    		report=$(awk '/##/{p=0}p;/!!!WARNING!!!/{p=1}' ${Project_ID}_QCreport.${Multi_pipelines}.gather.log)
    		echo ''
    		echo -e '!!!WARNING!!!'
    		echo -en "$report\n"
    		echo ''
    		echo -e 'Check '${Project_ID}_QCreport.${Multi_pipelines}.gather.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.$Multi_pipelines.gather.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
    elif  [[ -f "${Project_ID}_QCreport.${Multi_pipelines}.package.log" ]]; then
   		if grep -q "Program aborted" ${Project_ID}_QCreport.${Multi_pipelines}.package.log ; then
    		echo ''
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Multi_pipelines}.package.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.$Multi_pipelines.package.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
   	elif  [[ -f "${Project_ID}_QCreport.${Multi_pipelines}.release.log" ]]; then
   		if grep -q "Program aborted" ${Project_ID}_QCreport.${Multi_pipelines}.release.log ; then
    		echo ''
    		check_settings_folders
    		echo ''
    		echo -e 'ERROR: Check '${Project_ID}_QCreport.${Multi_pipelines}.release.log''
    		echo -e 'Program aborted.\nReview that slurm job is also cancelled'
   			echo -e 'Dear '"${Name^}"',\n\nError found in your qifa-script job submission for '$Project_ID'.\n'\
				'Review log file:  '${Project_ID}_QCreport.$Multi_pipelines.release.log'.\n\nHave a nice day!' | mail -s "ERROR_${Project_ID}_qifa-script" $email
    		echo ''
    		exit 1;
    	fi
     fi
}


####################################################################################################################################################################################

######Define function for basic qc
function run_basic_qc {
qc_folder
if	qifa-qc run -q $basic 2>&1> ${Project_ID}_QCreport.${basic}.log; then
	if [ -f "${Project_ID}_QCreport.${basic}.log" ]; then
		echo "INFO: Basic QC job submitted!"
		qifa_catch_error
		sleep $gather_wait_time_pre1
		qifa-qc gather -q $basic 2>&1> ${Project_ID}_QCreport.${basic}.gather.log
		sleep $gather_wait_time_pre2
		qifa-qc gather -q $basic 2>&1> ${Project_ID}_QCreport.${basic}.gather.log
		qifa_catch_error
		if grep -q "basic_QC job(s) failed" ${Project_ID}_QCreport.${basic}.gather.log ; then
			echo
			echo "ERROR: basic_QC job(s) failed. Remove the basic folder and re-submit job"
			echo
			exit 1;
		elif grep -q "Please complete the basic qc job submission for all sample" ${Project_ID}_QCreport.${basic}.gather.log ; then
			echo 
			echo "ERROR: Some jobs have no submission."
			echo "ACTION: Delete QC folder. Relaunch init command and resubmit qifa-script"
			echo 
			echo "Program terminated!"
			exit 1;
		else
			until qifa-qc gather -q $basic | grep "Please proceed to report result" 2>&1> ${Project_ID}_QCreport.${basic}.gather.log ; 
			do
				qifa-qc gather -q $basic 2>&1> ${Project_ID}_QCreport.${basic}.gather.log 
  				sleep $gather_wait_time
			done
			qifa-qc gather -q $basic 2>&1> ${Project_ID}_QCreport.${basic}.gather.log
			qifa_catch_error
  			echo "INFO: Basic QC metrics gathered!"
			qifa-qc report -q $basic 2>&1> ${Project_ID}_QCreport.${basic}.report.log
			echo "INFO: Basic QC metrics reported!"
			qifa_catch_error
			if [ ! -d "log" ] ; then
				mkdir log
				mv *.log log
			else
				mv *.log log
			fi
		fi
	else
		echo "INFO: Basic job is either waiting in queue or failed submission"
	fi
fi
}

function basic_package_release {
if qifa-qc package -q $basic 2>&1> ${Project_ID}_QCreport.$basic.package.log ; then
	qifa-qc release -q $basic 2>&1> ${Project_ID}_QCreport.$basic.release.log
	echo -e 'INFO: Updated '$basic' package'
	echo -e 'INFO: Relesase Path for '$basic' generated'
	echo -e 'INFO: All log files are in log folder'
	release=$(cat ${Project_ID}_QCreport.$basic.release.log)
	#echo
	#echo -ne "$release\n"
	if [ ! -d "log" ] ; then
		mkdir log
		mv *.log log;
	else
		mv *.log log;
	fi
fi
}

####Function for sinple Pipeline qc
function run_Pipeline_qc {
if [[ ! -z "$Pipeline" ]] ; then
	echo "INFO: Will now determine QC Pipeline status..."
	echo -e 'INFO: Pipeline determined as '$Pipeline''
	echo -e 'INFO: Initiating qifa-qc run -q '$Pipeline''
	qifa-qc run -q $Pipeline 2>&1> ${Project_ID}_QCreport.$Pipeline.log
	qifa_catch_error
	echo -e 'INFO: "qifa-qc run -q '$Pipeline'" initiated and '${Project_ID}_QCreport.$Pipeline.log' will be created'
	if grep -q "Please complete the $Pipeline qc job submission for all sample" ${Project_ID}_QCreport.$Pipeline.log ; then
		echo
		echo "ERROR: Some jobs have no submission."
		echo "ACTION: Delete QC folder. Relaunch init command and resubmit qifa-script"
		echo
		echo "Program terminated!"
		exit 1;
	else
		if [ -f "${Project_ID}_QCreport.$Pipeline.log" ]; then
			qifa_catch_error
			until qifa-qc gather -q $Pipeline | grep "Please proceed to report results" 2>&1> ${Project_ID}_QCreport.$Pipeline.gather.log ; 
			do
				qifa-qc gather -q $Pipeline 2>&1> ${Project_ID}_QCreport.$Pipeline.gather.log
				qifa_catch_error 
  				sleep $gather_wait_time
			done
			qifa-qc gather -q $Pipeline 2>&1> ${Project_ID}_QCreport.$Pipeline.gather.log
			qifa_catch_error
  			echo -e 'INFO: '$Pipeline' QC metrics gathered!'
			qifa-qc report -q $Pipeline 2>&1> ${Project_ID}_QCreport.$Pipeline.report.log
			echo -e 'INFO: '$Pipeline' QC metrics reported!'
			qifa-qc package -q $Pipeline 2>&1> ${Project_ID}_QCreport.$Pipeline.package.log
			qifa_catch_error
			qifa-qc release -q $Pipeline 2>&1> ${Project_ID}_QCreport.$Pipeline.release.log
			qifa_catch_error
			echo -e 'INFO: Updated '$Pipeline' package'
			echo -e 'INFO: Relesase Path for '$Pipeline' generated'
			echo -e 'INFO: All log files are in log folder'
			release=$(cat ${Project_ID}_QCreport.$Pipeline.release.log)
			#echo
			#echo -ne "$release\n"
			echo
			ReportFile=$(awk '/##/{p=0}p;/Report @/{p=1}' ${Project_ID}_QCreport.$Pipeline.report.log)
			if echo -ne "$ReportFile\n" | grep -q "Internal Report" ; then
				echo -ne "$ReportFile\n" | grep -v "Internal" | sed '/^$/d'
			else
				echo -ne "$ReportFile\n"
			fi
			if [ ! -d "log" ] ; then
				mkdir log
				mv *.log log;
			else
				mv *.log log;
			fi
		else 
			echo 'INFO: '$Pipeline' job is either waiting in queue or failed submission'
		fi
	fi
else
	echo -e 'WARNING! No Pipeline determined. You might be deliverying Basic QC'
	qifa-qc package -q $basic 2>&1> ${Project_ID}_QCreport.$basic.package.log
	qifa_catch_error
	qifa-qc release -q $basic 2>&1> ${Project_ID}_QCreport.$basic.release.log
	qifa_catch_error
	echo -e 'INFO: Updated '$basic' package'
	echo -e 'INFO: Relesase Path for '$basic' generated'
	echo -e 'INFO: All log files are in log folder'
	#release=$(cat ${Project_ID}_QCreport.$basic.release.log)
	#echo
	#echo -ne "$release\n"
	if [ ! -d "log" ] ; then
		mkdir log
		mv *.log log;
	else
		mv *.log log;
	fi
fi
}

####Function for multiple qc Pipelines 
function run_Multi_pipelines_qc {
pipeline_file_check
count=$(grep -wcf $Multipipelines $pipelinelist)
echo -e 'INFO: '$count' pipelines found'
echo "INFO: If pipeline requested is > 1, QC will be performed, one at a time on each pipeline"
paste $Multipipelines | while read Multi_pipelines; do
if  grep -qw "$Multi_pipelines" $pipelinelist ; then
	echo
	echo "INFO: Will now determine QC pipeline status..."
	echo -e 'INFO: pipeline determined as '$Multi_pipelines''
	echo -e 'INFO: Initiating qifa-qc run -q '$Multi_pipelines''
	qifa-qc run -q $Multi_pipelines 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.log
	qifa_catch_error
	echo -e 'INFO: "qifa-qc run -q '$Multi_pipelines'" initiated and '${Project_ID}_QCreport.$Multi_pipelines.log' will be created'
	if grep -q "Please complete the $Multi_pipelines qc job submission for all sample" ${Project_ID}_QCreport.$Multi_pipelines.log ; then
		echo
		echo "ERROR: Some jobs have no submission."
		echo "ACTION: Delete QC folder. Relaunch init command and resubmit qifa-script"
		echo
		echo "Program terminated!"
		exit 1;
	else
		if [ -f "${Project_ID}_QCreport.$Multi_pipelines.log" ]; then
			qifa_catch_error
			until qifa-qc gather -q $Multi_pipelines | grep "Please proceed to report results" 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.gather.log ; 
			do
				qifa-qc gather -q $Multi_pipelines 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.gather.log 
				qifa_catch_error
  				sleep $gather_wait_time
			done
			qifa-qc gather -q $Multi_pipelines 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.gather.log
			qifa_catch_error
  			echo -e 'INFO: '$Multi_pipelines' QC metrics gathered!'
			qifa-qc report -q $Multi_pipelines 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.report.log
			echo -e 'INFO: '$Multi_pipelines' QC metrics reported!'
			qifa-qc package -q $Multi_pipelines 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.package.log
			qifa_catch_error
			echo -e 'INFO: Updated '$Multi_pipelines' package'
			#############################################################################################################################Not working yet
			if [[ "$(grep -wf $Multipipelines  $pipelinelist | head -n1)" == "$Multi_pipelines" ]]; then
				qifa-qc release -q $Multi_pipelines 2>&1> ${Project_ID}_QCreport.$Multi_pipelines.release.log
				qifa_catch_error				
				echo -e 'INFO: Relesase Path for '$Multi_pipelines' generated'
				echo -e 'INFO: This is the only release path that would be generated for Multi-pipelines'
				echo -e 'INFO: All log files are in log folder'
			fi
			#############################################################################################################################Not working yet
			#release=$(cat ${Project_ID}_QCreport.$Multi_pipelines.release.log)
			#echo
			#echo -ne "$release\n"
			ReportFile=$(awk '/##/{p=0}p;/Report @/{p=1}' ${Project_ID}_QCreport.$Multi_pipelines.report.log)
			if echo -ne "$ReportFile\n" | grep -q "Internal Report" ; then
				echo -ne "$ReportFile\n" | grep -v "Internal" | sed '/^$/d'
			else
				echo -ne "$ReportFile\n"
			fi
			if [ ! -d "log" ] ; then
				mkdir log
				mv *.log log
			else
				mv *.log log
			fi
		else 
			echo 'INFO: '$Multi_pipelines' job is either waiting in queue or failed submission'
		fi
	fi
else
	echo -e 'WARNING! "'$Multi_pipelines'" pipeline undefined.'
fi
done
}

##################Copy fastqc *html reports
function fastqc_report {
release=$(echo ${Project_ID}_QCreport.*.release.log)
RelPath=$(grep -oP '(?<=INFO: Release path =).*' log/$release | xargs)
ProjReportDir="${RelPath##*/}"
ProjFileName="${ProjReportDir##*_}"
cd /gt/data/seqdma/Reports
if [ -d "$ProjReportDir" ]; then
	echo
	echo -e 'WARNING! '$ProjReportDir' folder exist @ /gt/data/seqdma/Reports.'
	read -p 'ACTION: Remove folder to replace content: (y/n)' y
	if [[ $y == "y" ]] ; then 
		rm -r -I $ProjReportDir
		mkdir $ProjReportDir
		cd $ProjReportDir
		rsync -va $qcFolder/fastqc/*html . > html.log
		rm html.log
		tar -cjvf ${ProjFileName}_FASTQCreports.tgz *html > html.log
		rm html.log
		echo -e 'INFO: Fastqc html copied and compressed @ /gt/data/seqdma/Reports/'${ProjReportDir}/${ProjFileName}_FASTQCreports.tgz''
	elif
		[[ $y == "n" ]]; then
		echo
		echo "INFO: You opted not to copy fastqc at this time"
	else
		mkdir $ProjReportDir
		cd $ProjReportDir
		rsync -va $qcFolder/fastqc/*html . > html.log
		rm html.log
		tar -cjvf ${ProjFileName}_FASTQCreports.tgz *html > html.log
		rm html.log
		echo -e 'INFO: Fastqc html copied and compressed @ /gt/data/seqdma/Reports/'${ProjReportDir}/${ProjFileName}_FASTQCreports.tgz''
	fi
else
	mkdir $ProjReportDir
	cd $ProjReportDir
	rsync -va $qcFolder/fastqc/*html . > html.log
	rm html.log
	tar -cjvf ${ProjFileName}_FASTQCreports.tgz *html > html.log
	rm html.log
	echo -e 'INFO: Fastqc html copied and compressed @ /gt/data/seqdma/Reports/'${ProjReportDir}/${ProjFileName}_FASTQCreports.tgz''
fi
}

########Copy box folder data
function box_data {
internalfile=$(echo *internal*)
if [[ ! "$internalfile" ]] ; then
	cp $qcFolder/package/* $qcFolder/package/data-to-copy 2>&1 | grep -v 'omitting directory'
	mv $qcFolder/package/data-to-copy/Run_Metric_Summary_${projectFinal}.draft.csv $qcFolder/package/data-to-copy/Run_Metric_Summary_${projectFinal}.csv
	echo -e 'INFO: Box folder data @ '$qcFolder'/package/data-to-copy'
	echo -e 'INFO: Remove data from data-to-copy folder after final edit of the excel sheet'
else
	cp $qcFolder/package/* $qcFolder/package/data-to-copy 2>&1 | grep -v 'omitting directory' 
	cp $qcFolder/$internalfile $qcFolder/package/data-to-copy 2>&1 | grep -v 'cannot stat' 
	mv $qcFolder/package/data-to-copy/Run_Metric_Summary_${projectFinal}.draft.csv $qcFolder/package/data-to-copy/Run_Metric_Summary_${projectFinal}.csv 
	echo -e 'INFO: Box folder data @ '$qcFolder'/package/data-to-copy'
	echo -e 'INFO: Remove data from data-to-copy folder after final edit of the excel sheet'
fi
}

#############################Copy fastqs to the directory

function deliver_fastq {
cd $qcDir
#qcFolder=$(echo $qcDir)
release_logs=$(echo log/${Project_ID}_QCreport.*.release.log)
release=$(printf '%s\n' $release_logs | head -n1)
RelPath=$(grep -oP '(?<=INFO: Release path =).*' $release | xargs)
ProjPath=$(echo /gt/${RelPath})
ProjDir="${ProjPath##*/}"
CustomerDir="${ProjPath%/*}"
cd fastqs
fqfiles=`readlink -f $(ls)`
fqcount=$(echo $(($(printf '%s\n' $fqfiles | grep fastq.gz | wc -l))))
Undeterminedcount=$(echo $(($(printf '%s\n' $fqfiles | grep "Undetermined"| wc -l))))
fqfileDir=`readlink -f $(ls | grep -v "Undetermined" | head -n1)`
fqfileDirParent="${fqfileDir%/*}"

if [[ -d "$ProjPath" ]]; then
	echo ''
	echo -e 'ERROR: '$ProjPath' folder exist.'
	echo -e 'ACTION: Remove folder and relaunch command to proceed.'
	echo "Program aborted"
	echo ''
	exit 1;
else
	mkdir -p $ProjPath
	echo ''
	echo -e 'INFO: Customer directory determined as '$CustomerDir''
	echo -e 'INFO: Customer project folder determined as '$ProjDir''
	cd $CustomerDir
	chmod 0750 $ProjDir
	echo -e 'INFO: Permission set for '$ProjDir' folder'
	echo -e 'INFO: Entering '$ProjDir' folder'
	cd $ProjDir
	printf '%s\n' $fqfiles > fqfiles_to_copy.txt
	echo -e 'INFO: '$fqcount' total fastq files including '$Undeterminedcount' undetermined are being prepared for copy';
	if printf '%s\n' $fqfiles | grep -q "R1_" ; then
		R1=$(printf '%s\n' $fqfiles | grep -c "R1_")
		Undeterminedfq=$(printf '%s\n' $fqfiles | grep "R1_" | grep -c "Undetermined"| wc -l)
		echo -e 'INFO: '$R1' R1 fastq files including '$Undeterminedfq' Undetermined will be copied into '$ProjDir'';
	fi
	if printf '%s\n' $fqfiles | grep -q "R2_" ; then
		R2=$(printf '%s\n' $fqfiles | grep -c "R2_")
		Undeterminedfq=$(printf '%s\n' $fqfiles | grep "R2_" | grep -c "Undetermined"| wc -l)
		echo -e 'INFO: '$R2' R2 fastq files including '$Undeterminedfq' Undetermined will be copied into '$ProjDir'';
	fi
	if printf '%s\n' $fqfiles | grep -q "R3_" ; then
		R3=$(printf '%s\n' $fqfiles | grep -c "R3_")
		Undeterminedfq=$(printf '%s\n' $fqfiles | grep "R3_" | grep -c "Undetermined"| wc -l)
		echo -e 'INFO: '$R3' R3 fastq files including '$Undeterminedfq' Undetermined will be copied into '$ProjDir'';
	fi
	if printf '%s\n' $fqfiles | grep -q "I1_" ; then
		I1=$(printf '%s\n' $fqfiles | grep -c "I1_")
		Undeterminedfq=$(printf '%s\n' $fqfiles | grep "I1_" | grep -c "Undetermined"| wc -l)
		echo -e 'INFO: '$I1' I1 fastq files including '$Undeterminedfq' Undetermined will be copied into '$ProjDir'';
	fi
	if printf '%s\n' $fqfiles | grep -q "I2_" ; then
		I2=$(printf '%s\n' $fqfiles | grep -c "I2_")
		Undeterminedfq=$(printf '%s\n' $fqfiles | grep "I2_" | grep -c "Undetermined"| wc -l)
		echo -e 'INFO: '$I2' I2 fastq files including '$Undeterminedfq' Undetermined will be copied into '$ProjDir'';
	fi
fi


if [[ ! -f "$CustomerDir/$ProjDir/fqfiles_to_copy.txt" ]]; then
	echo ''
	echo -e 'ERROR: "fqfiles_to_copy.txt" containing list of fastq to copy is missing'
	echo "Program aborted"
	echo ''
	exit 1;
fi

echo -e '#!/bin/bash
#SBATCH -J '$username'_fq_delivery
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 14
#SBATCH --mem=4G
#SBATCH -t 08:00:00
#SBATCH --mail-user='$email'
#SBATCH --mail-type=FAIL,END,TIME_LIMIT_50
#SBATCH -p gt_compute
#SBATCH --array=1-'$fqcount'%10
fq=$(head -n $SLURM_ARRAY_TASK_ID '$CustomerDir'/'$ProjDir'/fqfiles_to_copy.txt | tail -n 1)
rsync -vahP $fq .' > copy_fastq.sh
sbatch copy_fastq.sh
echo -e 'INFO: Job submitted.'
echo "INFO: Once fastq finish copying, email will be sent. Then perform below actions"
echo ''
echo '##OPTIONAL: rsync one more time to double check files are 100% copied##'
echo '##'
echo 'paste fqfiles_to_copy.txt | while read fq; do'
echo -e '\trsync -vahP $fq .'
echo 'done'
echo '##'
grp=$(stat -c "%G" .)
echo -e 'ACTION: Count total fastq <ls -ls *_R1_001* | wc -l>;'
echo -e 'ACTION: Set permission <chmod 640 *>'
echo -e 'ACTION: Change group name <e.g. sudo chgrp '$grp' *>'
echo -e 'ACTION: cd .. && chmod 0750 '$ProjDir' && sudo chgrp '$grp' '$ProjDir''
echo ''
echo 'REMINDER: copy QC '$projectFinal'_QCreport.csv, Stats_'$projectFinal'*.json, and Run_Metric_Summary_'$projectFinal'.csv'
echo 
###################
echo -e 'Dear '"${Name^}"',\n\nYour qifa-script job submission for fastq delivery into '$CustomerDir'/'$ProjDir' has been initiated. Once files are succesfully copied, slurm will issue email. Then perform the below actions.\n

##ACTION 1 - OPTIONAL: copy and paste to terminal, the below rsync one more time for files copying accuracy

paste fqfiles_to_copy.txt | while read fq; do
        rsync -vahP $fq .
done

##ACTION 2
1.\tRemove tmp files: <rm fqfiles_to_copy.txt slurm* copy_fastq.sh>.
2.\tCount total fastq and compare to that in '$projectFinal'_QCreport.csv: <ls -ls *_R1_001* | wc -l> and <ls -ls *_R2_001* | wc -l>
3.\tSet permission: <chmod 640 *>
4.\tChange group name < sudo chgrp '$grp' * >
5.\tcd .. && chmod 0750 '$ProjDir' && sudo chgrp '$grp' '$ProjDir'
6.\tcopy QC files: '$projectFinal'_QCreport.csv, Stats_'$projectFinal'*.json, and Run_Metric_Summary_'$projectFinal'.csv\n\trsync -va '$qcDir'/package/data-to-copy/* . 
7.\tremove QC files @ '$qcDir'/package/data-to-copy
\n\nHave a nice day! ' | mail -s "qifa-script fastq delivery job submitted for $projectFinal" $email
###################

$SHELL #This option physically move location of sudo to preferred directory
}

########################################################
function email_out {
echo -e 'Dear '"${Name^}"',\n\nYour qifa-script job submission for '$Project_ID' has completed.\nAdditional instruction might be displayed on your terminal.\n\nHave a nice day!' | mail -s "COMPLETE_${Project_ID}_qifa-script_job" $email
}
########################################################

###########Run functions

if [[ ! "$qcDir" ]]; then
        if [ -d "$basic" ] || [ -d "$Pipeline" ]; then
                grp_id
                qifa_catch_error
                echo
                echo "INFO: Performing a quick initial check..." 
                        if [ -d "$basic" ]; then
                                echo -e 'WARNING! A folder named "'$basic'" exist. Answer (y/n) to remove'
                                read -u 1 answer
                                if [[ $answer = y ]]; then
                                	rm -r $basic
                                fi
                        fi
                        if [ -d "$Pipeline" ]; then
                                echo -e 'WARNING! A folder named "'$Pipeline'" exist. Answer (y/n) to remove'
                                read -u 1 answer
                                if [[ $answer = y ]]; then
                                	rm -r $Pipeline
                                fi
                        fi
                        if [ ! -z "$Multipipelines" ]; then
							paste $Multipipelines | while read Multi_pipelines; do
								if [ -d "$Multi_pipelines" ]; then
									echo -e 'WARNING! A folder named "'$Multi_pipelines'" exist. Answer (y/n) to remove'
									read -u 1 answer
										if [[ $answer = y ]]; then
											rm -r $Multi_pipelines
										else
											continue
										fi
								fi
							done	
						fi
                        read -p 'Do you want to rerun or update existing basic/application(s) qc? (y/n)' y
                        if [[ $y == "y" ]] ; then 
                                echo
                                echo -e 'INFO: Attempt submitting '$basic' QC jobs'
                                echo -e 'INFO: If application(s) qc pipelines were requested, they will be queued'
                                run_basic_qc
                                if [ "$Pipeline" ]; then 
                                        echo -e 'INFO: '$Pipeline' QC pipeline will now be submitted'
                                        run_Pipeline_qc
                                elif [ "$Multipipelines" ]; then
                                	echo
                                	echo -e 'INFO: Multiple QC pipelines detected'
                                	run_Multi_pipelines_qc
                                else
                                        basic_package_release
                                fi
                        fastqc_report
                        box_data
                        email_out
                        echo
                        echo "#######################################################"
                        echo "Execute below to copy fastq files to customer delivery folder"
                        echo "#######################################################"
                        echo "sudo su - svc-gt-delivery"
                        echo "module use --append /gt/research_development/qifa/elion/modulefiles/"
                        echo "module load qifa-ops/"
                        username=$(whoami)
                        echo -e 'qifa-script -d '$qcFolder' -e '$email' -u '$username''
                        echo
                        elif [[ $y == "n" ]] ; then
                                echo
                                echo "Program Exited"
                                echo
                                exit 1
                        else
                                echo "y or n has to be entered"
                        fi
        else
                grp_id
                run_basic_qc
                if [ "$Pipeline" ]; then 
                    run_Pipeline_qc
                elif [ "$Multipipelines" ]; then 
                	run_Multi_pipelines_qc
                else
                        basic_package_release
                fi
                fastqc_report
                box_data
                email_out
                echo
                echo "Execute below command to copy fastq files to customer delivery folder:"
                echo "#######################################################"
                echo "sudo su - svc-gt-delivery"
                echo "module use --append /gt/research_development/qifa/elion/modulefiles/"
                echo "module load qifa-ops/"
                username=$(whoami)
                echo -e 'qifa-script -d '$qcFolder' -e '$email' -u '$username''
                echo
        fi
else
	deliver_fastq;
fi
