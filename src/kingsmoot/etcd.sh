#!/usr/bin/env bash

TEMP_DIR=/tmp
LOG_FILE=$TEMP_DIR/etcd.log
PID_FILE=$TEMP_DIR/etcd.pid
export ETCD_DATA_DIR=$TEMP_DIR/etcd.test
export ETCD_NAME=test
export ETCD_LISTEN_PEER_URLS=http://localhost:2370
export ETCD_LISTEN_CLIENT_URLS=http://localhost:2369
export ETCD_ADVERTISE_CLIENT_URLS=http://localhost:2369

print_usage() {
	 echo "Usage: "
	 echo "    etcd.sh {start|stop|restart}"
}

start(){
    echo "Removing old data dir"
    rm -r $ETCD_DATA_DIR
    rm $LOG_FILE
    etcd > $LOG_FILE 2>&1 &
    echo $! > $PID_FILE
}
stop(){
    kill `cat $PID_FILE`
}

restart(){
    stop
    start
}

if [ $# -ne 1 ]
then
	 print_usage
	 exit 1
fi

case $1 in
	 start|stop|restart)
		  OP=$1;;
	 *)
		  print_usage
		  exit 1;;
esac

$1