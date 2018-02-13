#!/bin/sh

case "$1" in

  'driver')
  	exec /usr/bin/driver $@
	;;

  *)
  	exec $@
	;;
	
esac