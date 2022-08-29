#!/bin/sh
# for input tif checks number of bands, if less then 3 deletes and writes name of input to deleted_list

INP=$1

if [ -n "$INP" ] && [ -f $INP ]; then
  bands=`gdalinfo -json $INP | jq '.bands | length'`
  if [ "$bands" -lt "3" ]; then
    echo "$INP bands=$bands" >> deleted_list
    rm -f $INP
  fi
fi