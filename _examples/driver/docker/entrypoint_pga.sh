#!/bin/sh

case "$1" in

  'download-dataset')
  	exec pga list -f json | jq -r '.sivaFilenames[]' | pga get -o /data
	;;

  *)
  	exec $@
	;;
	
esac