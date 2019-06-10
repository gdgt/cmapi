#!/usr/bin/env bash
set -e
echo "Set cloudera-manager.repo to CM v6"
yum clean all

rpm --import https://archive.cloudera.com/cdh6/6.2.0/redhat7/yum/RPM-GPG-KEY-cloudera
wget https://archive.cloudera.com/cm6/6.2.0/redhat7/yum/cloudera-manager.repo -O /etc/yum.repos.d/cloudera-manager.repo
yum install -y oracle-j2sdk*
export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera/
yum install -y cloudera-manager-server-db*
# yum install -y cloudera-manager-{daemons,server}

echo "start cloudera-scm-server-db and cloudera-scm-server services"
service cloudera-scm-server-db start
service cloudera-scm-server start

export PGPASSWORD=$(head -1 /var/lib/cloudera-scm-server-db/data/generated_password.txt)
SCHEMA=("hive" "sentry" "oozie")
for DB in "${SCHEMA[@]}"; do
    echo "Create $DB Database in Cloudera embedded PostgreSQL"
    SQLCMD=("CREATE ROLE $DB LOGIN PASSWORD 'cloudera';" "CREATE DATABASE $DB OWNER $DB ENCODING 'UTF8';" "ALTER DATABASE $DB SET standard_conforming_strings = off;")
    for SQL in "${SQLCMD[@]}"; do
        psql -A -t -d scm -U cloudera-scm -h localhost -p 7432 -c """${SQL}"""
    done
done
while ! (exec 6<>/dev/tcp/$(hostname)/7180) 2> /dev/null ; do echo 'Waiting for Cloudera Manager to start accepting connections...'; sleep 10; done
