#!/bin/bash
rsync -a --rsync-path="mkdir -p /srv/nfs/files.data.gouv.fr/geo-sirene/$(date +%Y-%m) && rsync" /srv/sirene/data-sirene/$(date +%Y-%m)/ root@files.data.gouv.fr:/srv/nfs/files.data.gouv.fr/geo-sirene/$(date +%Y-%m)/
