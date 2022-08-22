# Directory for storing oam-images, symlink to dir
ln -sf /mnt/evo4tb/oam_images data/in/oam_images/

curl https://api.openaerialmap.org/meta | jq '.meta.found' | \
    awk '{print int($0/100)+1}' | xargs -I {} seq {} | \
    xargs -I {} curl https://api.openaerialmap.org/meta?page={} | jq -c '.results' > data/in/oam_images/oam_meta.json

cat data/in/oam_images/oam_meta.json | jq -r '.[].uuid' | \
    parallel --progress -j 16 wget -nc -q -P oam-images {}
