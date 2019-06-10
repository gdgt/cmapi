----
``` bash
usage: cmxDeploy.py --cmhost CM-SERVER-IP --hosts ip1,ip2,ip3,...
```
----

#### pre-requisite

You have a running Cloudera Manager Server. You can use the preferred option as documented in ["Installation Path B - Manual Installation Using Cloudera Manager Packages"] [1](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM5/latest/Cloudera-Manager-Installation-Guide/cm5ig_install_path_B.html?scroll=cmig_topic_6_6). And also Cloudera Manager API python (cm-api) client installed [2](http://cloudera.github.io/cm_api/docs/python-client-swagger/)

#### TL;DR: copy-and-paste below example in RHEL/CentOS
``` bash
curl -L https://raw.githubusercontent.com/gdgt/cmapi/1.0.0/bootstrap-cm.sh | bash
yum install -y git python-pip python-setuptools
sudo pip install cm_client
curl -L https://raw.githubusercontent.com/gdgt/cmapi/1.0.0/cmxDeploy.py -o $HOME/cmxDeploy.py && chmod +x $HOME/cmxDeploy.py
python $HOME/cmxDeploy.py --cmhost $(hostname -f) --hosts $(hostname -f)
# OR python $HOME/cmxDeploy.py --cmhost "cm-ip" --hosts "ip1,ip2,ip3,..."
```
- SSH credentials - this allows CM to configure the rest of the hosts -u/--username, -p/--password
- create Hive in embedded PosgreSQL database, bash script provided below (also included in [bootstrap-cm.sh](https://github.com/gdgt/cmapi/blob/1.0.0/bootstrap-cm.sh#L17-L25)). 

#### Usage
``` bash
Usage: cmxDeploy.py [OPTIONS]

Options:
  --hosts TEXT           *Set target node(s) list, separate with comma  eg:
                         --hosts host1,host2,...,host(n).
  --cmhost TEXT          *Set Cloudera Manager Server Host.
  -i, --cdhversion TEXT  *Set CDH version.
  -d, --teardown         Teardown
  -u, --username TEXT    SSH username
  -p, --password TEXT    SSH password
  --help                 Show this message and exit.
```

----
