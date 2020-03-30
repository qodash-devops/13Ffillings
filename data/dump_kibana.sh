#!/usr/bin/env bash
elasticdump --overwrite --input http://localhost:9200/.kibana_1 --output kibana_1.json
elasticdump --overwrite --input http://localhost:9200/.kibana_task_manager_1 --output kibana_task_manager_1.json