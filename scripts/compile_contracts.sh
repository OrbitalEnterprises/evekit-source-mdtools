#!/bin/bash
#
# Compile all contract data for a given date.  This script produces the following files:
#
# contract_snapshots_YYYYMMDD_30.bulk - 30 minute contract list snapshots organized by region (each region is compressed)
# contract_snapshots_YYYYMMDD_30.index - offsets into the snapshots bulkfile by region
# contract_snapshots_YYYYMMDD_30.tgz - tar of gzip'd 30 minute contract list snapshot files by region (one file per region)
# contract_data_YYYYMMDD.bulk - all item exchange and auction data for any contract live on the given day organized by contract ID
# contract_data_YYYYMMDD.index - offsets into the contract data bulkfile by contract ID
# contract_data_YYYYMMDD.tgz - tar of gzip'd contract data files organized by region and contract ID
#
# $1 - YYYYMMDD to compile
# $2 - parent directory for contract region directories
# $3 - directory where compiled output should be written
#
if [ "$#" -lt 3 ]; then
    echo "usage: compile_contracts.sh YYYYMMDD source_dir output_dir"
    exit 1
fi
TMPDIR=/home/orbital/tmp
TMP=/home/orbital/tmp
export TMPDIR
export TMP
STAGE_DIR=${TMP}/compile_contracts_$$
mkdir -p ${STAGE_DIR}
trap "rm -rf ${STAGE_DIR}" EXIT
trap "rm -rf ${STAGE_DIR}" TERM

# Setup
SRC_DIR=${2}
OUT_DIR=${3}
mkdir -p ${OUT_DIR}
YEAR=${1:0:4}
MONTH=${1:4:2}
DAY=${1:6:2}

# Contract snapshot file format
#
# Total number of snapshots - normally 48 unless something is broken.
# Datetime of first snapshot in seconds UTC (Epoch or UNIX time)
# Number of snaphshot rows
# List of rows...
# Datetime of second snapshot
# Number of snapshot rows
# List of rows...

# Return contract file with a timestamp closest to the target (seconds UTC) without going over.

# Return a list of snapshot files for a range of timestamps such that each snapshot file is
# the closest available file before the target snapshot.  If no such file is available, then
# output the file name "NF".
#
# $1 - start timestamp
# $2 - end timestamp
# $3 - step size in seconds
# $4... - list of available snapshot files
build_file_list() {
    start=$1
    end=$2
    step=$3
    shift 3
    files=( "$@" )
    index=0
    max=$#

    if [ ${max} -eq 0 ] ; then
	return
    fi

    # Fast forward to the first file before the starting timestamp (if any)
    while [ ${index} -lt ${max} ] ; do
	next_file=${files[$index]}	
	nt=$(echo $(basename ${next_file}) | awk -F_ '{print $3}')
	nt=$(( ${nt}/1000 ))
	if [ ${nt} -ge ${start} ] ; then
	    if [ ${index} -gt 0 ] ; then
		index=$(( ${index} - 1 ))
	    fi
	    break
	else
	    index=$(( ${index} + 1 ))
	fi
    done

    # Now produce the file list with "NF" for missing files
    last_file=
    while [ ${start} -le ${end} ] ; do
	if [ ${index} -ge ${max} ] ; then
	    next_file=${last_file}
	else
	    next_file=${files[$index]}
	fi
	if [ -z "${next_file}" ] ; then
	    nt=${end}
	else
	    nt=$(echo $(basename ${next_file}) | awk -F_ '{print $3}')
	    nt=$(( ${nt}/1000 ))
	fi
	if [ ${nt} -ge ${start} ] ; then
	    if [ -n "${last_file}" ] ; then
		echo ${last_file}
	    else
		echo "NF"
	    fi
	else
	    echo ${next_file}
	    last_file=${next_file}
	    index=$(( ${index} + 1 ))	    
	fi
	start=$(( ${start} + ${step} ))
    done
}


# Build raw contract snapshot files
step=$(( 30 * 60 ))
for region_dir in $(find ${SRC_DIR} -type d -name '1*' -print) ; do
    region=$(basename ${region_dir})
    echo -n "Building snapshot file for region ${region}..."
    last_file=$(find ${region_dir} -name 'region_contracts_*' -print | sort | head --lines=1)
    if [ -z "${last_file}" ] ; then
	echo "No snapshots for region, skipping"
	continue
    fi

    # Create ordered list of snapshot files in 30 minute increments
    snap_file_list=( $(find ${region_dir} -name 'region_contracts_*' -print | sort) )
    start=$(date -u --date ${YEAR}${MONTH}${DAY} +'%s')
    end=$(date -u --date "${YEAR}${MONTH}${DAY} + 1 day" +'%s')
    out_file=${STAGE_DIR}/contract_data_${YEAR}${MONTH}${DAY}_30_${region}
    echo 48 >> ${out_file}

    # Now output snapshot files
    start=$(( ${start} + ${step} ))
    for bf in $(build_file_list ${start} ${end} ${step} "${snap_file_list[@]}") ; do
	echo ${start} >> ${out_file}
	if [ "${bf}" = "NF" ] ; then
	    echo 0 >> ${out_file}
	else
	    lc=$(( $(zcat ${bf} | wc -l) - 1 ))
	    echo ${lc} >> ${out_file}
	    zcat ${bf} | tail -n +2 >> ${out_file}
	fi
	start=$(( ${start} + ${step} ))
	echo -n "+"
    done
    
    gzip ${out_file}
    echo "...done"
done

# Build bulk and index contract snapshot files
cs_file_list=${STAGE_DIR}/cfl_$$.txt
cd ${STAGE_DIR}
ls contract_data_*_30_*.gz | sort -t_ -k5 -n > ${cs_file_list}
echo -n "Creating contract snapshot index..."
cat ${cs_file_list} | xargs stat -c "%n %s" | awk '{print $1, 0+offset; offset+=$2;}' > ${STAGE_DIR}/contract_snapshots_${YEAR}${MONTH}${DAY}_30.index
echo "done"

echo -n "Creating contract snapshot bulk file..."
cat ${cs_file_list} | xargs cat >> ${STAGE_DIR}/contract_snapshots_${YEAR}${MONTH}${DAY}_30.bulk
echo "done"

# Build contract item and bid files
echo -n "Assembling contract data files..."
cd ${SRC_DIR}
dd_file_list=${STAGE_DIR}/dfl_$$.txt
unsorted_fl=${STAGE_DIR}/unsortedfl_$$.txt
sorted_fl=${STAGE_DIR}/sortedfl_$$.txt
find ${SRC_DIR} -regex '.*contract_.*_.*\.txt' >> ${dd_file_list}
count=0
while IFS="" read -r nf || [ -n "$nf" ] ; do
    bn=$(basename ${nf})
    cid=$(echo ${bn} | awk -F_ '{print $2}')
    cat ${nf} | tail -n +2 > ${STAGE_DIR}/contract_data_${cid}.csv
    gzip ${STAGE_DIR}/contract_data_${cid}.csv
    echo contract_data_${cid}.csv.gz >> ${unsorted_fl}
    count=$(( ${count} + 1 ))
    if (( ${count} % 1000 == 0 )) ; then
	echo -n "+"
    fi
done < ${dd_file_list}
sort -t_ -k3 -n ${unsorted_fl} > ${sorted_fl}
echo "...done"

# Build bulk and index data files
cd ${STAGE_DIR}
echo -n "Creating contract data index..."
cat ${sorted_fl} | xargs stat -c "%n %s" | awk '{print $1, 0+offset; offset+=$2;}' > ${STAGE_DIR}/contract_data_${YEAR}${MONTH}${DAY}.index
gzip ${STAGE_DIR}/contract_data_${YEAR}${MONTH}${DAY}.index
echo "done"

echo -n "Creating contract data bulk file..."
cat ${sorted_fl} | xargs cat >> ${STAGE_DIR}/contract_data_${YEAR}${MONTH}${DAY}.bulk
echo "done"

# Assemble output files
echo -n "Assembling final files..."
cd ${STAGE_DIR}
tar czf ${OUT_DIR}/contract_snapshots_${YEAR}${MONTH}${DAY}_30.tgz contract_data_${YEAR}${MONTH}${DAY}_30_*.gz
cp ${STAGE_DIR}/contract_snapshots_${YEAR}${MONTH}${DAY}_30.index ${OUT_DIR}
cp ${STAGE_DIR}/contract_snapshots_${YEAR}${MONTH}${DAY}_30.bulk ${OUT_DIR}
tar czf ${OUT_DIR}/contract_data_${YEAR}${MONTH}${DAY}_30.tgz contract_data_*.csv.gz
cp ${STAGE_DIR}/contract_data_${YEAR}${MONTH}${DAY}.index.gz ${OUT_DIR}
cp ${STAGE_DIR}/contract_data_${YEAR}${MONTH}${DAY}.bulk ${OUT_DIR}
echo "done"
