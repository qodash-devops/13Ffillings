#!/usr/bin/env bash
docker cp ./positions_update.js 13fmongo:/positions_update.js
docker exec 13fmongo sh -c 'mongo /positions_update.js'