#stopping kibana
docker stop 13fkibana


#Making sure backup env is setup
docker exec 13fes sh -c 'chown -R  elasticsearch:elasticsearch /usr/share/elasticsearch/backup'
data="{\"type\": \"fs\",\"settings\": {\"location\": \"/usr/share/elasticsearch/backup\"}}"
echo $data
curl -i -XPOST http:/localhost:9200/_snapshot/kibana_backup?verify=false -H "Content-Type: application/json" -d "$data"


#copying snapshot to container
rm -rf /tmp/kibana_backup
tar -xzvf kibana_backup.tar.gz -C /
docker  /tmp/kibana_backup cp 13fes:/usr/share/elasticsearch/backup



#Restoring snapshot
curl -i -XDELETE http:/localhost:9200/.kibana_*
curl -i -XPOST http:/localhost:9200//_snapshot/kibana_backup/kibana_snapshot/_restore

echo "starting kibana"
docker start 13fkib





