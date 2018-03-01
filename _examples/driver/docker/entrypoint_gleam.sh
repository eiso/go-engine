#!/bin/sh

case "$1" in

  'master')
  	exec /usr/bin/gleam $@
	;;

  'agent')
  	ARGS="--host=`hostname -i`  --dir=/tmp"
  	exec /usr/bin/gleam $@ $ARGS
	;;

  *)
  	exec $@
	;;
esac