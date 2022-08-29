#!/bin/sh

# ./oam-downloader.sh images bbox=27.44704,53.83303,27.70474,53.97516

OUTPUT=oam.json

# echo "-$WORKDIR"
# echo "-$BBOX"
# echo "$WORKDIR/$OUTPUT"

if [ -n "$WORKDIR" ] && [ -d $WORKDIR ]; then
  if [ -n "$BBOX" ]; then
    curl -s https://api.openaerialmap.org/meta?$BBOX | jq '.meta.found' | \
        awk '{print int($0/100)+1}' | xargs -I {} seq {} | \
        xargs -I {} curl -s https://api.openaerialmap.org/meta?page={}\&$BBOX | jq -c '.results' > $WORKDIR/$OUTPUT
  else
    curl -s https://api.openaerialmap.org/meta | jq '.meta.found' | \
        awk '{print int($0/100)+1}' | xargs -I {} seq {} | \
        xargs -I {} curl -s https://api.openaerialmap.org/meta?page={} | jq -c '.results' > $WORKDIR/$OUTPUT
  fi
  cat $WORKDIR/$OUTPUT | jq -r '.[].uuid' | \
    parallel --progress -j 16 wget -nc -x -nH -q -P $WORKDIR {}

  rm -f $WORKDIR/deleted_list
  find $WORKDIR -type f -name '*.tif' | xargs -l -P 0 ./del-if-one-band.sh
fi