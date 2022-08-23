SHELL := /bin/sh

WDIR = /home/oam-downloader/images/
BBOX = bbox=27.41600,53.84698,27.65060,53.94107

all: oam_meta_update_loop

.PHONY: oam_meta_update_loop run_oam_meta_update oam_meta_update oam_meta_update_debug


oam_meta_update_loop: ## try to run update
	while true; do make run_oam_meta_update; sleep 1h; done


run_oam_meta_update: ## try to run update if file older then 6h
	[ -f $(WDIR)oam_meta.json ] || touch $(WDIR)oam_meta.json
	if [ $(shell find $(WDIR) -type f -name oam_meta.json -mmin +360) ]; then make oam_meta_update_debug; fi


oam_meta_update: ## Download images from OAM
	curl https://api.openaerialmap.org/meta | jq '.meta.found' | \
		awk '{print int($0/100)+1}' | xargs -I {} seq {} | \
		xargs -I {} curl https://api.openaerialmap.org/meta?page={} | jq -c '.results' > $(WDIR)oam_meta.json

oam_meta_update_debug: ## Download images from OAM
	curl https://api.openaerialmap.org/meta?$(BBOX) | jq -c '.results' > $(WDIR)oam_meta.json

	cat $(WDIR)oam_meta.json | jq -r '.[].uuid' | \
		parallel --progress -j 16 wget -nc -x -nH -q -P $(WDIR) {}