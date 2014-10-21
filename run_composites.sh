#!/bin/bash

if [ -f /etc/profile.d/smhi.sh ]
then
. /etc/profile.d/smhi.sh
fi

if [ $SMHI_DIST == 'linda' ]
then
SMHI_MODE='offline'
fi

case $SMHI_MODE in

################################################################################
# UTV

utv)

APPL_HOME="/usr/local"
MESAN_LOG_DIR="/var/log/satellit"

        ;;


################################################################################
# Default

*)
echo "No SMHI_MODE set..."

   ;;

esac


export MESAN_LOG_DIR


echo $PYTHONPATH
source ${APPL_HOME}/etc/.mesan_compositer_profile
echo $PYTHONPATH

timestamp=`date +%Y%m%d%H`
echo "timestamp=$timestamp"
areaid='mesanX'

python ${APPL_HOME}/bin/make_ct_composite.py -d $timestamp -t 30 -a $areaid
python ${APPL_HOME}/bin/ct_quicklooks.py -d $timestamp -a $areaid
python ${APPL_HOME}/bin/prt_nwcsaf_cloudamount.py -d $timestamp -a $areaid --ipar 71 --size 32

