#!/bin/bash
echo "--Creating Archive file--"
tar -cvf secrets.tar ./airflow/dags/google_credentials.json otrust.txt key.asc .env ./helm/secrets.yaml
echo "--Encrypting Archive file--"
echo "yes" | travis encrypt-file secrets.tar
echo "--Done--"