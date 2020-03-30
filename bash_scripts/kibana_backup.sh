#stopping kibana
docker stop 13fkibana

#Making sure backup env is setup
docker exec 13fes sh -c 'chown -R  elasticsearch:elasticsearch /usr/share/elasticsearch/backup'
data="{\"type\": \"fs\",\"settings\": {\"location\": \"/usr/share/elasticsearch/backup\"}}"
echo $data
curl -i -XPOST http:/localhost:9200/_snapshot/kibana_backup?verify=false -H "Content-Type: application/json" -d "$data"




#Making snapshot
data="
{
  \"indices\": \".kibana_1,.kibana_task_manager_1\",
  \"ignore_unavailable\": true,
  \"include_global_state\": false
}
"
curl -i -XPUT http:/localhost:9200/_snapshot/kibana_backup/kibana_snapshot?wait_for_completion=true -H "Content-Type: application/json" -d "$data"


#copying snapshot
rm -rf /tmp/kibana_backup
docker cp 13fes:/usr/share/elasticsearch/backup /tmp/kibana_backup
tar -zcvf kibana_backup.tar.gz /tmp/kibana_backup

echo "starting kibana"
docker start 13fkibana