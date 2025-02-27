#!/bin/bash
#-------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------

CMD=${1:-"systemds"}
DATADIR=${2:-"temp"}/als
NUMFED=${3:-4}
MAXITR=${4:-100}

FILENAME=$0
err_report() {
  echo "Error in $FILENAME on line $1"
}
trap 'err_report $LINENO' ERR

# Set properties
export SYSDS_QUIET=1

BASEPATH=$(dirname "$0")

${BASEPATH}/../genALSData.sh systemds $DATADIR; # generate the data

# start the federated workers on localhost
${BASEPATH}/utils/startFedWorkers.sh systemds $DATADIR $NUMFED "localhost";

for d in "10k_1k_dense" "10k_1k_sparse" # "100k_1k_dense" "100k_1k_sparse" "1M_1k_dense" "1M_1k_sparse" "10M_1k_dense" "10M_1k_sparse" "100M_1k_dense" "100M_1k_sparse"
do
  # split the generated data into paritions and create a federated object
  ${CMD} -f ${BASEPATH}/data/splitAndMakeFederated.dml \
    --config ${BASEPATH}/../conf/SystemDS-config.xml \
    --nvargs data=${DATADIR}/X${d} nSplit=$NUMFED transposed=FALSE \
      target=${DATADIR}/X${d}_fed.json hosts=${DATADIR}/workers/hosts fmt="csv"

  echo "-- Running ALS-CG with federated data ("$d") on "$NUMFED" federated workers" >> results/times.txt

  # run the als algorithm on the federated object
  ${BASEPATH}/../runALS.sh ${DATADIR}/X${d}_fed.json $MAXITR $DATADIR systemds 0.001 FALSE;
done

${BASEPATH}/utils/killFedWorkers.sh $DATADIR; # kill the federated workers
