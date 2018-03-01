#!/bin/sh

case "$1" in

  'driver')
  	exec ./$@
	;;

  *)
  	exec $@
	;;
	
esac