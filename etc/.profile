if [ -f /etc/profile.d/smhi.sh -a -z "$SMHI_MODE" ]
# don't touch SMHI_MODE if it is already set.
then
. /etc/profile.d/smhi.sh
fi

case $SMHI_MODE in
offline)

MESAN_COMPOSITE_CONFIG_DIR=/local_disk/laptop/Satsa/Mesan/mesan-sat-preproc/etc
export MESAN_COMPOSITE_CONFIG_DIR
PPP_CONFIG_DIR=/local_disk/laptop/Satsa/Mesan/mesan-sat-preproc/etc
export PPP_CONFIG_DIR

  ;;

utv)


MESAN_COMPOSITE_CONFIG_DIR=/local_disk/laptop/Satsa/Mesan/mesan-sat-preproc/etc
export MESAN_COMPOSITE_CONFIG_DIR
PPP_CONFIG_DIR=/local_disk/laptop/Satsa/Mesan/mesan-sat-preproc/etc
export PPP_CONFIG_DIR

  ;;

###################################################################$
### Jenkins

ci)

CENTRAL=/usr/local/lib64/python2.6/site-packages:/usr/local/lib/python2.6/site-packages
PREFIX=$HOME/opt

PYCOAST=/data/proj/safutv/opt/PYCOAST/
MPOP_ETC=/local_disk/opt/SATPROD/current/etc/

PPP_CONFIG_DIR=$MPOP_ETC
export PPP_CONFIG_DIR


PYTHONPATH=$CENTRAL:$PYCOAST:$MPOP_ETC:${PYTHONPATH}
export PYTHONPATH

##### for scipy
#PYTHONPATH=${PYTHONPATH}:/data/proj/safutv/usr/lib/python2.6/site-packages:/data/proj/safutv/usr/lib64/python2.6/site-packages

MESAN_COMPOSITE_CONFIG_DIR=/data/proj/safutv/mesan_etc
export MESAN_COMPOSITE_CONFIG_DIR
PPP_CONFIG_DIR=/data/proj/safutv/mesan_etc
export PPP_CONFIG_DIR

  ;;

*)
echo "No SMHI_DIST set..."
   ;;

esac