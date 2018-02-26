#!/bin/sh

case "$1" in

  'download-dataset')
  	exec multitool get-index | tee index.csv | grep -oE '[0-9a-f]{40}\.siva' | multitool get-dataset --workers 10 -o .
	;;

  *)
  	exec $@
	;;
	
esac