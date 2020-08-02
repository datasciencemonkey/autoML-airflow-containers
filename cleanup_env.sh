#! /bin/bash
docker stop $(docker ps -a -q --filter ancestor=jupyter4sas:v01 --format="{{.ID}}")
echo "Stopped Jupyter Container"
docker stop $(docker ps -a -q --filter ancestor=h2o:v01 --format="{{.ID}}")
echo "Stopped h2o Container"
docker stop $(docker ps -a -q --filter ancestor=cas_35:v01 --format="{{.ID}}")
echo "Stopped CAS Container"
echo "SUCCESS --> Clean Up Complete"