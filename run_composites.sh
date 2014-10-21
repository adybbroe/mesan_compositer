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

# $1 = yyyymmddhh (time and date of analysis in hour resolution)
# $2 = MM (Time window in minutes)
# $3 = area_id (Area id)

source ${APPL_HOME}/etc/.mesan_compositer_profile

python ${APPL_HOME}/bin/make_ct_composite.py -d $1 -t $2 -a $3
python ${APPL_HOME}/bin/ct_quicklooks.py -d $1 -a $3
python ${APPL_HOME}/bin/prt_nwcsaf_cloudamount.py -d $1 -a $3 --ipar 71 --size 32

