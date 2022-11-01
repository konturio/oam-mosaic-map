#!/usr/bin/env bash
inp1=/var/www/tiles/reor20/cogs/houston_max_uint16.tif
inp2=/var/www/tiles/reor20/cogs/waverly_max_uint16.tif

cmDepth=colormap_depth
cmDischarge=colormap_discharge

out1=$(basename $inp1 .tif)_$cmDepth.vrt
out2=$(basename $inp2 .tif)_$cmDepth.vrt


# step1 apply collor mapping
gdaldem color-relief $inp1 -alpha -b 1 $cmDepth $out1
gdaldem color-relief $inp2 -alpha -b 1 $cmDepth $out2

# step2 collect vrt's into mosaic vrt layer
gdalbuildvrt -overwrite -allow_projection_difference resDepth.vrt $out1 $out2


#
# same steps for discharge layers
out1=$(basename $inp1 .tif)_$cmDischarge.vrt
out2=$(basename $inp2 .tif)_$cmDischarge.vrt


gdaldem color-relief $inp1 -alpha -b 2 $cmDischarge $out1
gdaldem color-relief $inp2 -alpha -b 2 $cmDischarge $out2

gdalbuildvrt -overwrite -allow_projection_difference resDischarge.vrt $out1 $out2
