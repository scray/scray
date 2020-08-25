#!/bin/sh

source "./config.sh"

dashboard_id="h8acj9eWz"
datasource_id="1"

#curl -X POST -H "Content-Type: application/json" -d '{"name":"scray-importer", "role": "Admin"}' http://admin:admin@${GRAFANA_HOST}:3000/api/auth/keys

function download_configuration {
    curl https://raw.githubusercontent.com/scray/scray/master/projects/persistent-traffic-data/monitoring/grafana-configurator/dashboard_h8acj9eWz.json -o dashboard_h8acj9eWz.json
    curl https://raw.githubusercontent.com/scray/scray/master/projects/persistent-traffic-data/monitoring/grafana-configurator/datasource_1.json -o datasource_1.json 
}

function get_grafana_base_url {
	url=http://$GRAFANA_USER:$GRAFANA_PASSWORD@${GRAFANA_HOST}:$GRAFANA_PORT
	echo "$url"
}

function get_dashboard {
	dashboard_uid=$1
	curl -s -H "Content-Type: application/json" $(get_grafana_base_url)/api/dashboards/uid/${dashboard_uid} > dashboard_$dashboard_uid.json
}

function push_dashboard {
	dashboard_uid=$1
	curl -s -H "Content-Type: application/json"  -X POST -d "$(cat dashboard_$dashboard_uid.json)" $(get_grafana_base_url)/api/dashboards/db
}

function get_data_source {
	data_source_id=$1
	curl -s -H "Content-Type: application/json" $(get_grafana_base_url)/api/datasources/${data_source_id} > datasource_$data_source_id.json
}

function push_data_source {
	data_source_id=$1
	curl -s -H "Content-Type: application/json"  -X POST -d "$(cat datasource_$data_source_id.json)" $(get_grafana_base_url)/api/datasources/
}

download_configuration
#echo $(get_data_source  $datasource_id)
echo $(push_data_source  $datasource_id)
echo $(push_dashboard $dashboard_id)

