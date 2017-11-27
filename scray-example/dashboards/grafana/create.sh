#!/bin/bash

curl -X POST  "http://admin:admin@localhost:3001/api/datasources" -d "@./Datasource_DEMO_graphite.json" -H "Content-Type: application/json"

curl -X POST  "http://admin:admin@localhost:3001/api/dashboards/db/" -d "@./Dasboard_null_.json" -H "Content-Type: application/json"
ID=$(curl -X GET  "http://admin:admin@localhost:3001/api/dashboards/db/null_" | awk '{split($0,a,"id\":"); print a[2]}' | cut -d',' -f1)

sed 's/title_/Facilities/' Template_Dashboard_Facilities.json | sed "s/id_/${ID}/" > Dashboard_Facilities.json

curl -X POST  "http://admin:admin@localhost:3001/api/dashboards/db/" -d "@./Dashboard_Facilities.json" -H "Content-Type: application/json"
