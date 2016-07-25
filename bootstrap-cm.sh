#!/usr/bin/env bash
set -e
echo "Set cloudera-manager.repo to CM v5"
yum clean all
rpm --import http://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera
wget http://archive.cloudera.com/cm5/redhat/6/x86_64/cm/cloudera-manager.repo -O /etc/yum.repos.d/cloudera-manager.repo
yum install -y oracle-j2sdk* cloudera-manager-{daemons,server,server-db*}

echo "start cloudera-scm-server-db and cloudera-scm-server services"
service cloudera-scm-server-db start
service cloudera-scm-server start

export PGPASSWORD=$(head -1 /var/lib/cloudera-scm-server-db/data/generated_password.txt)
SCHEMA=("hive" "sentry")
for DB in "${SCHEMA[@]}"; do
    echo "Create $DB Database in Cloudera embedded PostgreSQL"
    SQLCMD=("CREATE ROLE $DB LOGIN PASSWORD 'cloudera';" "CREATE DATABASE $DB OWNER $DB ENCODING 'UTF8';" "ALTER DATABASE $DB SET standard_conforming_strings = off;")
    for SQL in "${SQLCMD[@]}"; do
        psql -A -t -d scm -U cloudera-scm -h localhost -p 7432 -c """${SQL}"""
    done
done
while ! (exec 6<>/dev/tcp/$(hostname)/7180) 2> /dev/null ; do echo 'Waiting for Cloudera Manager to start accepting connections...'; sleep 10; done
