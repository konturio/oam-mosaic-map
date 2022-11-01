#!/usr/bin/env bash

inp1=/var/www/tiles/reor20/cogs/houston_max_uint16.tif
inp2=/var/www/tiles/reor20/cogs/waverly_max_uint16.tif

cmDepth=colormap_depth
cmDischarge=colormap_discharge

out1=$(basename $inp1 .tif)_$cmDepth.vrt
out2=$(basename $inp2 .tif)_$cmDepth.vrt

# step1 apply color mapping
gdaldem color-relief $inp1 -alpha -b 1 $cmDepth $out1
gdaldem color-relief $inp2 -alpha -b 1 $cmDepth $out2

out11=$(basename $out1 .vrt)_warped.vrt
out22=$(basename $out2 .vrt)_warped.vrt

# step2 reproject output to same epsg
gdalwarp -overwrite -t_srs EPSG:3857 $out1 $out11
gdalwarp -overwrite -t_srs EPSG:3857 $out2 $out22

# step3 generating result vrt
gdalbuildvrt -overwrite -resolution highest resDepth_v2.vrt $out11 $out22