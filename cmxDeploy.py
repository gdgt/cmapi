#!/usr/bin/env python
#
__author__ = 'Michalis'
__version__ = '0.13.2702'

import socket
import re
import urllib2
from optparse import OptionParser
import hashlib
import os
import sys
import random

from cm_api.api_client import ApiResource, ApiException, API_CURRENT_VERSION
from cm_api.http_client import HttpClient, RestException
from cm_api.endpoints.hosts import *
from cm_api.endpoints.services import ApiServiceSetupInfo, ApiService


def init_cluster():
    """
    Initialise Cluster
    :return:
    """
    print "> Initialise Cluster"
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    # Update Cloudera Manager configuration
    cm = api.get_cloudera_manager()

    def manifest_to_dict(manifest_json):
        if manifest_json:
            dir_list = json.load(
                urllib2.urlopen(manifest_json))['parcels'][0]['parcelName']
            parcel_part = re.match(r"^(.*?)-(.*)-(.*?)$", dir_list).groups()
            print "{'product': %s, 'version': %s}" % (str(parcel_part[0]).upper(), str(parcel_part[1]).lower())
            return {'product': str(parcel_part[0]).upper(), 'version': str(parcel_part[1]).lower()}
        else:
            raise Exception("Invalid manifest.json")

    # Install CDH5 latest version
    repo_url = ["%s/cdh5/parcels/%s" % (cmx.archive_url, cmx.cdh_version)]
    print "CDH5 Parcel URL: %s" % repo_url[0]
    cmx.parcel.append(manifest_to_dict(repo_url[0] + "/manifest.json"))

    # Install GPLEXTRAS5 to match CDH5 version
    repo_url.append('%s/gplextras5/parcels/%s' %
                    (cmx.archive_url, cmx.parcel[0]['version'].split('-')[0]))
    print "GPL Extras parcel URL: %s" % repo_url[1]
    cmx.parcel.append(manifest_to_dict(repo_url[1] + "/manifest.json"))

    cm.update_config({"REMOTE_PARCEL_REPO_URLS": "http://archive.cloudera.com/impala/parcels/latest/,"
                                                 "http://archive.cloudera.com/search/parcels/latest/,"
                                                 "http://archive.cloudera.com/spark/parcels/latest/,"
                                                 "http://archive.cloudera.com/sqoop-connectors/parcels/latest/,"
                                                 "http://archive.cloudera.com/accumulo-c5/parcels/latest,"
                                                 "%s" % ",".join([url for url in repo_url if url]),
                      "PHONE_HOME": False, "PARCEL_DISTRIBUTE_RATE_LIMIT_KBS_PER_SECOND": "102400"})

    if cmx.cluster_name in [x.name for x in api.get_all_clusters()]:
        print "Cluster name: '%s' already exists" % cmx.cluster_name
    else:
        print "Creating cluster name '%s'" % cmx.cluster_name
        api.create_cluster(name=cmx.cluster_name, version=cmx.cluster_version)


def add_hosts_to_cluster():
    """
    Add hosts to cluster
    :return:
    """
    print "> Add hosts to Cluster: %s" % cmx.cluster_name
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    cm = api.get_cloudera_manager()

    # deploy agents into host_list
    host_list = list(set([socket.getfqdn(x) for x in cmx.host_names] + [socket.getfqdn(cmx.cm_server)]) -
                     set([x.hostname for x in api.get_all_hosts()]))
    if host_list:
        cmd = cm.host_install(user_name=cmx.ssh_root_user, host_names=host_list,
                              password=cmx.ssh_root_password, private_key=cmx.ssh_private_key, unlimited_jce=True)

        # TODO: Temporary fix to flag for unlimited strength JCE policy files installation (If unset, defaults to false)
        # host_install_args = {"userName": cmx.ssh_root_user, "hostNames": host_list, "password": cmx.ssh_root_password,
        #                     "privateKey": cmx.ssh_private_key, "unlimitedJCE": True}
        # cmd = cm._cmd('hostInstall', data=host_install_args)
        print "Installing host(s) to cluster '%s' - [ http://%s:7180/cmf/command/%s/details ]" % \
              (socket.getfqdn(cmx.cm_server), cmx.cm_server, cmd.id)
        check.status_for_command("Hosts: %s " % host_list, cmd)

    hosts = []
    for host in api.get_all_hosts():
        if host.hostId not in [x.hostId for x in cluster.list_hosts()]:
            print "Adding {'ip': '%s', 'hostname': '%s', 'hostId': '%s'}" % (host.ipAddress, host.hostname, host.hostId)
            hosts.append(host.hostId)

    if hosts:
        print "Adding hostId(s) to '%s'" % cmx.cluster_name
        print "%s" % hosts
        cluster.add_hosts(hosts)


def host_rack():
    """
    Add host to rack
    :return:
    """
    # TODO: Add host to rack
    print "> Add host to rack"
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    hosts = []
    for h in api.get_all_hosts():
        # host = api.create_host(h.hostId, h.hostname,
        # socket.gethostbyname(h.hostname),
        # "/default_rack")
        h.set_rack_id("/default_rack")
        hosts.append(h)

    cluster.add_hosts(hosts)


def _check_parcel_stage(parcel_item, expected_stage, action_description):
    # def wait_for_parcel_stage(cluster, parcel, wanted_stages, action_description):
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)

    while True:
        cdh_parcel = cluster.get_parcel(product=parcel_item['product'], version=parcel_item['version'])
        if cdh_parcel.stage in expected_stage:
            break
        if cdh_parcel.state.errors:
            raise Exception(str(cdh_parcel.state.errors))

        msg = " [%s: %s / %s]" % (cdh_parcel.stage, cdh_parcel.state.progress, cdh_parcel.state.totalProgress)
        sys.stdout.write(msg + " " * (78 - len(msg)) + "\r")
        sys.stdout.flush()
        time.sleep(1)


def parcel_action(parcel_item, function, expected_stage, action_description):
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    print "%s [%s-%s]" % (action_description, parcel_item['product'], parcel_item['version'])
    cdh_parcel = cluster.get_parcel(product=parcel_item['product'], version=parcel_item['version'])

    cmd = getattr(cdh_parcel, function)()
    if not cmd.success:
        print "ERROR: %s failed!" % action_description
        exit(0)
    return _check_parcel_stage(parcel_item, expected_stage, action_description)


def setup_zookeeper():
    """
    Zookeeper
    > Waiting for ZooKeeper Service to initialize
    Starting ZooKeeper Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "ZOOKEEPER"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "zookeeper"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()
        service.update_config({"zookeeper_datadir_autocreate": False})

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SERVER":
                rcg.update_config({"maxClientCnxns": "1024", "zookeeper_server_java_heapsize": "492830720"})
                # Pick 3 hosts and deploy Zookeeper Server role
                for host in random.sample(hosts, 3 if len(hosts) >= 3 else 1):
                    cdh.create_service_role(service, rcg.roleType, host)

        # init_zookeeper not required as the API performs this when adding Zookeeper
        # check.status_for_command("Waiting for ZooKeeper Service to initialize", service.init_zookeeper())
        check.status_for_command("Starting ZooKeeper Service", service.start())


def setup_hdfs():
    """
    HDFS
    > Checking if the name directories of the NameNode are empty. Formatting HDFS only if empty.
    Starting HDFS Service
    > Creating HDFS /tmp directory
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "HDFS"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hdfs"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service_config = cdh.dependencies_for(service)
        service_config.update({"dfs_replication": "3",
                               "dfs_block_local_path_access_user": "impala,hbase,mapred,spark"})
        service.update_config(service_config)

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "NAMENODE":
                # hdfs-NAMENODE - Default Group
                rcg.update_config({"dfs_name_dir_list": "/data/dfs/nn",
                                   "namenode_java_heapsize": "1073741824",
                                   "dfs_namenode_handler_count": "30",
                                   "dfs_namenode_service_handler_count": "30",
                                   "dfs_namenode_servicerpc_address": "8022"})
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "SECONDARYNAMENODE":
                # hdfs-SECONDARYNAMENODE - Default Group
                rcg.update_config({"fs_checkpoint_dir_list": "/data/dfs/snn",
                                   "secondary_namenode_java_heapsize": "1073741824"})
                # chose a server that it's not NN, easier to enable HDFS-HA later
                secondary_nn = random.choice([host for host in hosts if host.hostId not in
                                              [x.hostRef.hostId for x in service.get_roles_by_type("NAMENODE")]]) \
                    if len(hosts) > 1 else random.choice(hosts)

                cdh.create_service_role(service, rcg.roleType, secondary_nn)

            if rcg.roleType == "DATANODE":
                # hdfs-DATANODE - Default Group
                rcg.update_config({"datanode_java_heapsize": "127926272",
                                   "dfs_data_dir_list": "/data/dfs/dn",
                                   "dfs_datanode_data_dir_perm": "755",
                                   "dfs_datanode_du_reserved": "3218866585",
                                   "dfs_datanode_failed_volumes_tolerated": "0",
                                   "dfs_datanode_max_locked_memory": "316669952", })
            if rcg.roleType == "BALANCER":
                # hdfs-BALANCER - Default Group
                rcg.update_config({"balancer_java_heapsize": "492830720"})
            if rcg.roleType == "GATEWAY":
                # hdfs-GATEWAY - Default Group
                rcg.update_config({"dfs_client_use_trash": True})

        for role_type in ['DATANODE', 'GATEWAY']:
            for host in manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        nn_role_type = service.get_roles_by_type("NAMENODE")[0]
        commands = service.format_hdfs(nn_role_type.name)
        for cmd in commands:
            check.status_for_command("Format NameNode", cmd)

        check.status_for_command("Starting HDFS.", service.start())
        check.status_for_command("Creating HDFS /tmp directory", service.create_hdfs_tmp())


def setup_hbase():
    """
    HBase
    > Creating HBase root directory
    Starting HBase Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "HBASE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hbase"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service_config = {"hbase_enable_indexing": True, "hbase_enable_replication": True,
                          "zookeeper_session_timeout": "30000"}
        service_config.update(cdh.dependencies_for(service))
        service.update_config(service_config)

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "MASTER":
                rcg.update_config({"hbase_master_java_heapsize": "492830720"})
            if rcg.roleType == "REGIONSERVER":
                rcg.update_config({"hbase_regionserver_java_heapsize": "365953024",
                                   "hbase_regionserver_java_opts": "-XX:+UseParNewGC -XX:+UseConcMarkSweepGC "
                                                                   "-XX:-CMSConcurrentMTEnabled "
                                                                   "-XX:CMSInitiatingOccupancyFraction=70 "
                                                                   "-XX:+CMSParallelRemarkEnabled -verbose:gc "
                                                                   "-XX:+PrintGCDetails -XX:+PrintGCDateStamps"})

        for role_type in ['MASTER', 'HBASETHRIFTSERVER', 'HBASERESTSERVER']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        for role_type in ['GATEWAY', 'REGIONSERVER']:
            for host in manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Creating HBase root directory", service.create_hbase_root())
        # This service is started later on
        # check.status_for_command("Starting HBase Service", service.start())


def setup_solr():
    """
    Solr
    > Initializing Solr in ZooKeeper
    > Creating HDFS home directory for Solr
    Starting Solr Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "SOLR"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "solr"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SOLR_SERVER":
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "GATEWAY":
                for host in manager.get_hosts(include_cm_host=True):
                    cdh.create_service_role(service, rcg.roleType, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        # check.status_for_command("Initializing Solr in ZooKeeper", service._cmd('initSolr'))
        # check.status_for_command("Creating HDFS home directory for Solr", service._cmd('createSolrHdfsHomeDir'))
        check.status_for_command("Initializing Solr in ZooKeeper", service.init_solr())
        check.status_for_command("Creating HDFS home directory for Solr",
                                 service.create_solr_hdfs_home_dir())
        # This service is started later on
        # check.status_for_command("Starting Solr Service", service.start())


def setup_ks_indexer():
    """
    KS_INDEXER
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "KS_INDEXER"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "ks_indexer"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Pick 1 host to deploy Lily HBase Indexer Default Group
        cdh.create_service_role(service, "HBASE_INDEXER", random.choice(hosts))

        # HBase Service-Wide configuration
        hbase = cdh.get_service_type('HBASE')
        hbase.stop()
        hbase.update_config({"hbase_enable_indexing": True, "hbase_enable_replication": True})
        hbase.start()

        # This service is started later on
        # check.status_for_command("Starting Lily HBase Indexer Service", service.start())


def setup_spark():
    """
    Spark
    > Execute command CreateSparkUserDirCommand on service Spark
    > Execute command CreateSparkHistoryDirCommand on service Spark
    > Execute command SparkUploadJarServiceCommand on service Spark
    Starting Spark Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "SPARK"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "spark"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        cdh.create_service_role(service, "SPARK_MASTER", [x for x in hosts if x.id == 0][0])
        cdh.create_service_role(service, "SPARK_HISTORY_SERVER", random.choice(hosts))

        for role_type in ['GATEWAY', 'SPARK_WORKER']:
            for host in manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Execute command CreateSparkUserDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkUserDirCommand'))
        check.status_for_command("Execute command CreateSparkHistoryDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkHistoryDirCommand'))
        check.status_for_command("Execute command SparkUploadJarServiceCommand on service Spark",
                                 service.service_command_by_name('SparkUploadJarServiceCommand'))

        # This service is started later on
        # check.status_for_command("Starting Spark Service", service.start())


def setup_spark_on_yarn():
    """
    Sqoop Client
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "SPARK_ON_YARN"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "spark_on_yarn"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SPARK_YARN_HISTORY_SERVER":
                rcg.update_config({"history_server_max_heapsize": "153092096"})

        cdh.create_service_role(service, "SPARK_YARN_HISTORY_SERVER", random.choice(hosts))

        for host in manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, "GATEWAY", host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Execute command CreateSparkUserDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkUserDirCommand'))
        check.status_for_command("Execute command CreateSparkHistoryDirCommand on service Spark",
                                 service.service_command_by_name('CreateSparkHistoryDirCommand'))
        check.status_for_command("Execute command SparkUploadJarServiceCommand on service Spark",
                                 service.service_command_by_name('SparkUploadJarServiceCommand'))

        # This service is started later on
        # check.status_for_command("Starting Spark Service", service.start())


def setup_yarn():
    """
    Yarn
    > Creating MR2 job history directory
    > Creating NodeManager remote application log directory
    Starting YARN (MR2 Included) Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "YARN"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "yarn"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "RESOURCEMANAGER":
                # yarn-RESOURCEMANAGER - Default Group
                rcg.update_config({"resource_manager_java_heapsize": "492830720",
                                   "yarn_scheduler_maximum_allocation_mb": "2568",
                                   "yarn_scheduler_maximum_allocation_vcores": "2"})
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "JOBHISTORY":
                # yarn-JOBHISTORY - Default Group
                rcg.update_config({"mr2_jobhistory_java_heapsize": "492830720"})
                cdh.create_service_role(service, rcg.roleType, random.choice(hosts))
            if rcg.roleType == "NODEMANAGER":
                # yarn-NODEMANAGER - Default Group
                rcg.update_config({"yarn_nodemanager_heartbeat_interval_ms": "100",
                                   "yarn_nodemanager_local_dirs": "/data/yarn/nm",
                                   "yarn_nodemanager_resource_cpu_vcores": "2",
                                   "yarn_nodemanager_resource_memory_mb": "2568",
                                   "node_manager_java_heapsize": "127926272"})
                for host in hosts:
                    cdh.create_service_role(service, rcg.roleType, host)
            if rcg.roleType == "GATEWAY":
                # yarn-GATEWAY - Default Group
                rcg.update_config({"mapred_reduce_tasks": "505413632", "mapred_submit_replication": "1",
                                   "mapred_reduce_tasks": "3"})
                for host in manager.get_hosts(include_cm_host=True):
                    cdh.create_service_role(service, rcg.roleType, host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Creating MR2 job history directory", service.create_yarn_job_history_dir())
        check.status_for_command("Creating NodeManager remote application log directory",
                                 service.create_yarn_node_manager_remote_app_log_dir())
        # This service is started later on
        # check.status_for_command("Starting YARN (MR2 Included) Service", service.start())


def setup_mapreduce():
    """
    MapReduce
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "MAPREDUCE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "mapreduce"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "JOBTRACKER":
                # mapreduce-JOBTRACKER - Default Group
                rcg.update_config({"jobtracker_mapred_local_dir_list": "/data/mapred/jt",
                                   "jobtracker_java_heapsize": "492830720",
                                   "mapred_job_tracker_handler_count": "22"})
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])
            if rcg.roleType == "TASKTRACKER":
                # mapreduce-TASKTRACKER - Default Group
                rcg.update_config({"tasktracker_mapred_local_dir_list": "/data/mapred/local",
                                   "mapred_tasktracker_map_tasks_maximum": "1",
                                   "mapred_tasktracker_reduce_tasks_maximum": "1",
                                   "task_tracker_java_heapsize": "127926272"})
            if rcg.roleType == "GATEWAY":
                # mapreduce-GATEWAY - Default Group
                rcg.update_config({"mapred_reduce_tasks": "1", "mapred_submit_replication": "1"})

        for role_type in ['GATEWAY', 'TASKTRACKER']:
            for host in manager.get_hosts(include_cm_host=(role_type == 'GATEWAY')):
                cdh.create_service_role(service, role_type, host)

                # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
                # cdh.deploy_client_config_for(service)

                # This service is started later on
                # check.status_for_command("Starting MapReduce Service", service.start())


def setup_hive():
    """
    Hive
    > Creating Hive Metastore Database
    > Creating Hive Metastore Database Tables
    > Creating Hive user directory
    > Creating Hive warehouse directory
    Starting Hive Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "HIVE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hive"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        # hive_metastore_database_host: Assuming embedded DB is running from where embedded-db is located.
        service_config = {"hive_metastore_database_host": socket.getfqdn(cmx.cm_server),
                          "hive_metastore_database_user": "hive",
                          "hive_metastore_database_name": "hive",
                          "hive_metastore_database_password": "cloudera",
                          "hive_metastore_database_port": "7432",
                          "hive_metastore_database_type": "postgresql"}
        service_config.update(cdh.dependencies_for(service))
        service.update_config(service_config)

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "HIVEMETASTORE":
                rcg.update_config({"hive_metastore_java_heapsize": "492830720"})
            if rcg.roleType == "HIVESERVER2":
                rcg.update_config({"hiveserver2_java_heapsize": "144703488"})

        for role_type in ['HIVEMETASTORE', 'HIVESERVER2']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        for host in manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, "GATEWAY", host)

        # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
        # cdh.deploy_client_config_for(service)

        check.status_for_command("Creating Hive Metastore Database Tables", service.create_hive_metastore_tables())
        check.status_for_command("Creating Hive user directory", service.create_hive_userdir())
        check.status_for_command("Creating Hive warehouse directory", service.create_hive_warehouse())
        # This service is started later on
        # check.status_for_command("Starting Hive Service", service.start())


def setup_sqoop():
    """
    Sqoop 2
    > Creating Sqoop 2 user directory
    > Creating Sqoop 2 Database
    Starting Sqoop 2 Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "SQOOP"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "sqoop"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "SQOOP_SERVER":
                rcg.update_config({"sqoop_java_heapsize": "492830720"})

        cdh.create_service_role(service, "SQOOP_SERVER", [x for x in hosts if x.id == 0][0])

        check.status_for_command("Creating Sqoop 2 user directory", service.create_sqoop_user_dir())
        # CDH Version check if greater than 5.3.0
        vc = lambda v: tuple(map(int, (v.split("."))))
        if vc(cmx.parcel[0]['version'].split('-')[0]) >= vc("5.3.0"):
            check.status_for_command("Creating Sqoop 2 Database", service._cmd('SqoopCreateDatabase'))
            # This service is started later on
            # check.status_for_command("Starting Sqoop 2 Service", service.start())


def setup_sqoop_client():
    """
    Sqoop Client
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "SQOOP_CLIENT"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "sqoop_client"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        # hosts = get_cluster_hosts()

        # Service-Wide
        service.update_config({})

        for host in manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, "GATEWAY", host)

            # Example of deploy_client_config. Recommended to Deploy Cluster wide client config.
            # cdh.deploy_client_config_for(service)


def setup_impala(enable_llama=False):
    """
    Impala
    > Creating Impala user directory
    Starting Impala Service
    :param enable_llama:
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "IMPALA"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "impala"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "IMPALAD":
                rcg.update_config({"impalad_memory_limit": "618659840",
                                   "enable_audit_event_log": True,
                                   "scratch_dirs": "/data/impala/impalad"})

        for role_type in ['CATALOGSERVER', 'STATESTORE']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        # Install ImpalaD
        for host in hosts:
            cdh.create_service_role(service, "IMPALAD", host)

        check.status_for_command("Creating Impala user directory", service.create_impala_user_dir())
        # Impala will be started/stopped when we enable_llama_rm
        # This service is started later on
        # check.status_for_command("Starting Impala Service", service.start())

        # Enable YARN and Impala Integrated Resource Management
        # http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/admin_llama.html
        yarn = cdh.get_service_type('YARN')
        if yarn is not None and enable_llama is True:
            # enable cgroup-based resource management for all hosts with NodeManager roles.
            cm = api.get_cloudera_manager()
            cm.update_all_hosts_config({"rm_enabled": True})
            yarn.update_config({"yarn_service_cgroups": True, "yarn_service_lce_always": True})
            role_group = yarn.get_role_config_group("%s-RESOURCEMANAGER-BASE" % yarn.name)
            role_group.update_config({"yarn_scheduler_minimum_allocation_mb": 0,
                                      "yarn_scheduler_minimum_allocation_vcores": 0})
            check.status_for_command("Enable YARN and Impala Integrated Resource Management",
                                     service.enable_llama_rm(random.choice(hosts).hostId))


def setup_oozie():
    """
    Oozie
    > Creating Oozie database
    > Installing Oozie ShareLib in HDFS
    Starting Oozie Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "OOZIE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "oozie"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "OOZIE_SERVER":
                rcg.update_config({"oozie_java_heapsize": "492830720"})
                cdh.create_service_role(service, rcg.roleType, [x for x in hosts if x.id == 0][0])

        check.status_for_command("Creating Oozie database", service.create_oozie_db())
        check.status_for_command("Installing Oozie ShareLib in HDFS", service.install_oozie_sharelib())
        # This service is started later on
        # check.status_for_command("Starting Oozie Service", service.start())


def setup_oozie_ha(load_balancer_host_port):
    """
    Setup oozie-ha
    :return:
    """
    # TODO: test setup_oozie_ha
    print "> Setup OOZIE-HA"
    service = cdh.get_service_type('OOZIE')
    # pre-requisites
    service.update_config({"oozie_load_balancer": load_balancer_host_port})
    rcg = service.get_role_config_group("{0}-OOZIE_SERVER-BASE".format(service.name))
    # CM5.4/OPSAPS-25778
    rcg.update_config({"oozie_plugins_list": "org.apache.oozie.service.ZKLocksService,"
                                             "org.apache.oozie.service.ZKXLogStreamingService,"
                                             "org.apache.oozie.service.ZKJobsConcurrencyService,"
                                             "org.apache.oozie.service.ZKUUIDService"})

    if len(service.get_roles_by_type("OOZIE_SERVER")) != 2:
        # Choose random node for the second Oozie Server
        hosts = manager.get_hosts()
        rnd_host = random.choice([x.hostId for x in hosts if x.hostId
                                  is not service.get_roles_by_type("OOZIE_SERVER")[0].hostRef.hostId])

        cmd = service.enable_oozie_ha(rnd_host.hostRef.hostId)
        check.status_for_command("Enable YARN-HA - [ http://%s:7180/cmf/command/%s/details ]" %
                                 (socket.getfqdn(cmx.cm_server), cmd.id), cmd)


def setup_hue():
    """
    Hue
    Starting Hue Service
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "HUE"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "hue"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Role Config Group equivalent to Service Default Group
        for rcg in [x for x in service.get_all_role_config_groups()]:
            if rcg.roleType == "HUE_SERVER":
                rcg.update_config({})
                cdh.create_service_role(service, "HUE_SERVER", [x for x in hosts if x.id == 0][0])
                # This service is started later on
                # check.status_for_command("Starting Hue Service", service.start())


def setup_flume():
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "FLUME"
    if cdh.get_service_type(service_type) is None:
        service_name = "flume"
        cluster.create_service(service_name.lower(), service_type)
        service = cluster.get_service(service_name)

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))
        hosts = manager.get_hosts()
        cdh.create_service_role(service, "AGENT", [x for x in hosts if x.id == 0][0])
        # This service is started later on
        # check.status_for_command("Starting Flume Agent", service.start())


def setup_accumulo():
    """
    Accumulo 1.6
    > Deploy Client Configuration
    > Create Accumulo Home Dir on service Accumulo 1.6
    > Create Accumulo User Dir on service Accumulo 1.6
    > Initialize Accumulo on service Accumulo 1.6
    Start Accumulo 1.6
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "ACCUMULO16"
    if cdh.get_service_type(service_type) is None:
        print "> %s" % service_type
        service_name = "accumulo16"
        print "Create %s service" % service_name
        cluster.create_service(service_name, service_type)
        service = cluster.get_service(service_name)
        hosts = manager.get_hosts()

        # Deploy ACCUMULO16 Parcel
        parcel = [x for x in cluster.get_all_parcels() if x.product == 'ACCUMULO' and
                  'cdh5' in x.version][0]

        accumulo_parcel = {'product': str(parcel.product.upper()), 'version': str(parcel.version).lower()}
        print "> Parcel action for parcel: [ %s-%s ]" % (parcel.product, parcel.version)
        cluster_parcel = cluster.get_parcel(product=parcel.product, version=parcel.version)
        if "ACTIVATED" not in cluster_parcel.stage:
            parcel_action(parcel_item=accumulo_parcel, function="start_removal_of_distribution",
                          expected_stage=['DOWNLOADED', 'AVAILABLE_REMOTELY', 'ACTIVATING'],
                          action_description="Un-Distribute Parcel")
            parcel_action(parcel_item=accumulo_parcel, function="start_download",
                          expected_stage=['DOWNLOADED'], action_description="Download Parcel")
            parcel_action(parcel_item=accumulo_parcel, function="start_distribution", expected_stage=['DISTRIBUTED'],
                          action_description="Distribute Parcel")
            parcel_action(parcel_item=accumulo_parcel, function="activate", expected_stage=['ACTIVATED'],
                          action_description="Activate Parcel")

        # Service-Wide
        service.update_config(cdh.dependencies_for(service))

        # Create Accumulo roles
        for role_type in ['ACCUMULO16_MASTER', 'ACCUMULO16_TRACER', 'ACCUMULO16_GC',
                          'ACCUMULO16_TSERVER', 'ACCUMULO16_MONITOR']:
            cdh.create_service_role(service, role_type, random.choice(hosts))

        # Create Accumulo gateway roles
        for host in manager.get_hosts(include_cm_host=True):
            cdh.create_service_role(service, 'GATEWAY', host)

        print "Deploy Client Configuration"
        cluster.deploy_client_config()
        check.status_for_command("Execute command Create Accumulo Home Dir on service Accumulo 1.6",
                                 service.service_command_by_name('CreateHdfsDirCommand'))
        check.status_for_command("Execute command Create Accumulo User Dir on service Accumulo 1.6",
                                 service.service_command_by_name('CreateAccumuloUserDirCommand'))
        check.status_for_command("Execute command Initialize Accumulo on service Accumulo 1.6",
                                 service.service_command_by_name('AccumuloInitServiceCommand'))
        # check.status_for_command("Starting Accumulo Service", service.start())


def setup_hdfs_ha():
    """
    Setup hdfs-ha
    :return:
    """
    try:
        print "> Setup HDFS-HA"
        hdfs = cdh.get_service_type('HDFS')
        zookeeper = cdh.get_service_type('ZOOKEEPER')
        # Requirement Hive/Hue
        hive = cdh.get_service_type('HIVE')
        hue = cdh.get_service_type('HUE')
        hosts = manager.get_hosts()

        if len(hdfs.get_roles_by_type("NAMENODE")) != 2:
            # QJM require 3 nodes
            jn = random.sample([x.hostRef.hostId for x in hdfs.get_roles_by_type("DATANODE")], 3)
            # get NAMENODE and SECONDARYNAMENODE hostId
            nn_host_id = hdfs.get_roles_by_type("NAMENODE")[0].hostRef.hostId
            sndnn_host_id = hdfs.get_roles_by_type("SECONDARYNAMENODE")[0].hostRef.hostId

            # Occasionally SECONDARYNAMENODE is also installed on the NAMENODE
            if nn_host_id == sndnn_host_id:
                standby_host_id = random.choice([x.hostId for x in jn if x.hostId not in [nn_host_id, sndnn_host_id]])
            elif nn_host_id is not sndnn_host_id:
                standby_host_id = sndnn_host_id
            else:
                standby_host_id = random.choice([x.hostId for x in hosts if x.hostId is not nn_host_id])

            # hdfs-JOURNALNODE - Default Group
            role_group = hdfs.get_role_config_group("%s-JOURNALNODE-BASE" % hdfs.name)
            role_group.update_config({"dfs_journalnode_edits_dir": "/data/dfs/jn"})

            cmd = hdfs.enable_nn_ha(hdfs.get_roles_by_type("NAMENODE")[0].name, standby_host_id,
                                    "nameservice1", [dict(jnHostId=jn[0]), dict(jnHostId=jn[1]), dict(jnHostId=jn[2])],
                                    zk_service_name=zookeeper.name)
            check.status_for_command("Enable HDFS-HA - [ http://%s:7180/cmf/command/%s/details ]" %
                                     (socket.getfqdn(cmx.cm_server), cmd.id), cmd)

            # hdfs-HTTPFS
            cdh.create_service_role(hdfs, "HTTPFS", [x for x in hosts if x.id == 0][0])
            # Configure HUE service dependencies
            cdh(*['HDFS', 'HIVE', 'HUE', 'ZOOKEEPER']).stop()
            if hue is not None:
                hue.update_config(cdh.dependencies_for(hue))
            if hive is not None:
                check.status_for_command("Update Hive Metastore NameNodes", hive.update_metastore_namenodes())
            cdh(*['ZOOKEEPER', 'HDFS', 'HIVE', 'HUE']).start()

    except ApiException as err:
        print " ERROR: %s" % err.message


def setup_yarn_ha():
    """
    Setup yarn-ha
    :return:
    """
    print "> Setup YARN-HA"
    yarn = cdh.get_service_type('YARN')
    zookeeper = cdh.get_service_type('ZOOKEEPER')
    # hosts = api.get_all_hosts()
    if len(yarn.get_roles_by_type("RESOURCEMANAGER")) != 2:
        # Choose random node for standby RM
        rm = random.choice([nm for nm in yarn.get_roles_by_type("NODEMANAGER")
                            if nm.hostRef.hostId != yarn.get_roles_by_type("RESOURCEMANAGER")[0].hostRef.hostId])
        cmd = yarn.enable_rm_ha(rm.hostRef.hostId, zookeeper.name)
        check.status_for_command("Enable YARN-HA - [ http://%s:7180/cmf/command/%s/details ]" %
                                 (socket.getfqdn(cmx.cm_server), cmd.id), cmd)


def enable_kerberos():
    """
    Enable Kerberos
    > Import KDC Account Manager Credentials
    > Generate Credentials
    > Stop cluster
    > Stop Cloudera Management Services
    > Configure all services to use Kerberos
    > Wait for credentials to be generated
    > Deploy client configuration
    > Start Cloudera Management Services
    > Start cluster
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cm = api.get_cloudera_manager()
    cluster = api.get_cluster(cmx.cluster_name)
    print "> Setup Kerberos"
    cm.update_config({"KDC_HOST": cmx.kerberos['kdc_host'],
                      "SECURITY_REALM": cmx.kerberos['security_realm']})

    if cmx.api_version >= 11:
        check.status_for_command("Configure Kerberos for Cluster",
                                 cluster.configure_for_kerberos(datanode_transceiver_port=1004,
                                                                datanode_web_port=1006))
        check.status_for_command("Stop Cloudera Management Services", cm.get_service().stop())
        # check.status_for_command("Wait for credentials to be generated", cm.generate_credentials())
        check.status_for_command("Start Cloudera Management Services", cm.get_service().start())
    else:
        hdfs = cdh.get_service_type('HDFS')
        zookeeper = cdh.get_service_type('ZOOKEEPER')
        hue = cdh.get_service_type('HUE')
        hosts = manager.get_hosts()

        check.status_for_command("Import Admin Credentials",
                                 cm.import_admin_credentials(username=str(cmx.kerberos['kdc_user']),
                                                             password=str(cmx.kerberos['kdc_password'])))
        check.status_for_command("Wait for credentials to be generated", cm.generate_credentials())
        time.sleep(10)
        check.status_for_command("Stop cluster: %s" % cmx.cluster_name, cluster.stop())
        check.status_for_command("Stop Cloudera Management Services", cm.get_service().stop())

        # Configure all services to use MIT Kerberos
        # HDFS Service-Wide
        hdfs.update_config({"hadoop_security_authentication": "kerberos", "hadoop_security_authorization": True})

        # hdfs-DATANODE-BASE - Default Group
        role_group = hdfs.get_role_config_group("%s-DATANODE-BASE" % hdfs.name)
        role_group.update_config({"dfs_datanode_http_port": "1006", "dfs_datanode_port": "1004",
                                  "dfs_datanode_data_dir_perm": "700"})

        # Zookeeper Service-Wide
        zookeeper.update_config({"enableSecurity": True})
        cdh.create_service_role(hue, "KT_RENEWER", [x for x in hosts if x.id == 0][0])

        # Example deploying cluster wide Client Config
        check.status_for_command("Deploy client config for %s" % cmx.cluster_name, cluster.deploy_client_config())
        check.status_for_command("Start Cloudera Management Services", cm.get_service().start())
        # check.status_for_command("Start cluster: %s" % cmx.cluster_name, cluster.start())


def disable_kerberos():
    """
    Disable Kerberos
    > Stop cluster
    > Stop Cloudera Management Services
    > Configure all services to not use Kerberos
    > Deploy client configuration
    > Start Cloudera Management Services
    > Start cluster
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cm = api.get_cloudera_manager()
    cluster = api.get_cluster(cmx.cluster_name)
    print "> Setup Kerberos"
    cm.update_config({"KDC_HOST": None, "SECURITY_REALM": None})
    hdfs = cdh.get_service_type('HDFS')
    zookeeper = cdh.get_service_type('ZOOKEEPER')
    hue = cdh.get_service_type('HUE')

    check.status_for_command("Stop cluster: %s" % cmx.cluster_name, cluster.stop())
    check.status_for_command("Stop Cloudera Management Services", cm.get_service().stop())

    # Configure all services to use simple authentication
    # HDFS Service-Wide
    hdfs.update_config({"hadoop_security_authentication": "simple", "hadoop_security_authorization": False})

    # hdfs-DATANODE-BASE - Default Group
    role_group = hdfs.get_role_config_group("%s-DATANODE-BASE" % hdfs.name)
    role_group.update_config({"dfs_datanode_http_port": "50075", "dfs_datanode_port": "50010",
                              "dfs_datanode_data_dir_perm": "700"})

    # Zookeeper Service-Wide
    zookeeper.update_config({"enableSecurity": False})
    kt_renewer_role = hue.get_roles_by_type("HUE_SERVER")[0].name
    check.status_for_command("Delete KT_RENEWER role: %s" % kt_renewer_role, hue.delete_role(kt_renewer_role))

    # Example deploying cluster wide Client Config
    check.status_for_command("Deploy client config for %s" % cmx.cluster_name, cluster.deploy_client_config())
    check.status_for_command("Start Cloudera Management Services", cm.get_service().start())
    check.status_for_command("Start cluster: %s" % cmx.cluster_name, cluster.start())


def setup_sentry():
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    service_type = "SENTRY"
    if cdh.get_service_type(service_type) is None:
        service_name = "sentry"
        cluster.create_service(service_name.lower(), service_type)
        service = cluster.get_service(service_name)

        # Service-Wide
        # sentry_server_database_host: Assuming embedded DB is running from where embedded-db is located.
        service_config = {"sentry_server_database_host": socket.getfqdn(cmx.cm_server),
                          "sentry_server_database_user": "sentry",
                          "sentry_server_database_name": "sentry",
                          "sentry_server_database_password": "cloudera",
                          "sentry_server_database_port": "7432",
                          "sentry_server_database_type": "postgresql"}

        service_config.update(cdh.dependencies_for(service))
        service.update_config(service_config)
        hosts = manager.get_hosts()

        cdh.create_service_role(service, "SENTRY_SERVER", random.choice(hosts))
        check.status_for_command("Creating Sentry Database Tables", service.create_sentry_database_tables())

        # Update configuration for Hive service
        hive = cdh.get_service_type('HIVE')
        hive.update_config(cdh.dependencies_for(hive))

        # Disable HiveServer2 Impersonation - hive-HIVESERVER2-BASE - Default Group
        role_group = hive.get_role_config_group("%s-HIVESERVER2-BASE" % hive.name)
        role_group.update_config({"hiveserver2_enable_impersonation": False})

        # This service is started later on
        # check.status_for_command("Starting Sentry Server", service.start())


def setup_easy():
    """
    An example using auto_assign_roles() and auto_configure()
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    print "> Easy setup for cluster: %s" % cmx.cluster_name
    # Do not install these services
    do_not_install = ['KEYTRUSTEE', 'KMS', 'KS_INDEXER', 'ISILON', 'FLUME', 'MAPREDUCE', 'ACCUMULO',
                      'ACCUMULO16', 'SPARK_ON_YARN', 'SPARK', 'SOLR', 'SENTRY']
    service_types = list(set(cluster.get_service_types()) - set(do_not_install))
    [cluster.create_service(name=service.lower(), service_type=service.upper()) for service in service_types]

    cluster.auto_assign_roles()
    cluster.auto_configure()

    # Hive Metastore DB and dependencies ['YARN', 'ZOOKEEPER']
    service = cdh.get_service_type('HIVE')
    service_config = {"hive_metastore_database_host": socket.getfqdn(cmx.cm_server),
                      "hive_metastore_database_user": "hive",
                      "hive_metastore_database_name": "hive",
                      "hive_metastore_database_password": "hive",
                      "hive_metastore_database_port": "7432",
                      "hive_metastore_database_type": "postgresql"}
    service_config.update(cdh.dependencies_for(service))
    service.update_config(service_config)
    check.status_for_command("Executing first run command. This might take a while.", cluster.first_run())


def teardown(keep_cluster=True):
    """
    Teardown the Cluster
    :return:
    """
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    try:
        cluster = api.get_cluster(cmx.cluster_name)
        service_list = cluster.get_all_services()
        print "> Teardown Cluster: %s Services and keep_cluster: %s" % (cmx.cluster_name, keep_cluster)
        check.status_for_command("Stop %s" % cmx.cluster_name, cluster.stop())

        for service in service_list[:None:-1]:
            try:
                check.status_for_command("Stop Service %s" % service.name, service.stop())
            except ApiException as err:
                print " ERROR: %s" % err.message

            # Unset service dependencies and configuration settings
            service_config = {}
            for k, v in service.get_config()[0].items():
                service_config[k] = None

        for service in service_list[:None:-1]:
            # Remove service roles
            print "Processing service %s" % service.name
            for role in service.get_all_roles():
                print " Delete role %s" % role.name
                service.delete_role(role.name)

            cluster.delete_service(service.name)

        print "Deactivate and Un-Distribute CDH Parcel and GPL Extras Parcel"
        for cdh_parcel in cluster.get_all_parcels():
            if cdh_parcel.stage == 'ACTIVATED':
                print "> Parcel action for parcel: [ %s-%s ]" % (cdh_parcel.product, cdh_parcel.version)
                parcel_action(parcel_item={"product": cdh_parcel.product, "version": cdh_parcel.version},
                              function="deactivate", expected_stage=['DISTRIBUTED'],
                              action_description="Deactivate Parcel")
                parcel_action(parcel_item={"product": cdh_parcel.product, "version": cdh_parcel.version},
                              function="start_removal_of_distribution", expected_stage=['DOWNLOADED'],
                              action_description="Un-Distribute Parcel")

    except ApiException as err:
        print err.message
        exit(1)

    # Delete Management Services
    try:
        mgmt = api.get_cloudera_manager()
        check.status_for_command("Stop Management services", mgmt.get_service().stop().wait())
        mgmt.delete_mgmt_service()
    except ApiException as err:
        print " ERROR: %s" % err.message

    # cluster.remove_all_hosts()
    if not keep_cluster:
        print "Deleting cluster: %s" % cmx.cluster_name
        api.delete_cluster(cmx.cluster_name)


class ManagementActions:
    """
    Example stopping 'ACTIVITYMONITOR', 'REPORTSMANAGER' Management Role
    :param role_list:
    :param action:
    :return:
    """

    def __init__(self, *role_list):
        self._role_list = role_list
        self._api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                                version=cmx.api_version)
        self._cm = self._api.get_cloudera_manager()
        try:
            self._service = self._cm.get_service()
        except ApiException:
            self._service = self._cm.create_mgmt_service(ApiServiceSetupInfo())
        self._role_types = [x.type for x in self._service.get_all_roles()]

    def stop(self):
        self._role_action('stop_roles')

    def start(self):
        self._role_action('start_roles')

    def restart(self):
        self._role_action('restart_roles')

    def _role_action(self, action):
        state = {'start_roles': ['STOPPED'], 'stop_roles': ['STARTED'], 'restart_roles': ['STARTED', 'STOPPED']}
        for mgmt_role in [x for x in self._role_list if x in self._role_types]:
            for role in [x for x in self._service.get_roles_by_type(mgmt_role) if x.roleState in state[action]]:
                [check.status_for_command("%s role %s" % (action.split("_")[0].upper(), mgmt_role), cmd)
                 for cmd in getattr(self._service, action)(role.name)]

    def setup(self):
        """
        Setup Management Roles
        'ACTIVITYMONITOR', 'ALERTPUBLISHER', 'EVENTSERVER', 'HOSTMONITOR', 'SERVICEMONITOR'
        Requires License: 'NAVIGATOR', 'NAVIGATORMETASERVER', 'REPORTSMANAGER"
        :return:
        """
        print "> Setup Management Services"
        self._cm.update_config({"TSQUERY_STREAMS_LIMIT": 1000})
        hosts = manager.get_hosts(include_cm_host=True)
        # pick hostId that match the ipAddress of cm_server
        # mgmt_host may be empty then use the 1st host from the -w
        try:
            mgmt_host = [x for x in hosts if x.ipAddress == socket.gethostbyname(cmx.cm_server)][0]
        except IndexError:
            mgmt_host = [x for x in hosts if x.id == 0][0]

        for role_type in [x for x in self._service.get_role_types() if x in self._role_list]:
            try:
                if not [x for x in self._service.get_all_roles() if x.type == role_type]:
                    print "Creating Management Role %s " % role_type
                    role_name = "mgmt-%s-%s" % (role_type, mgmt_host.md5host)
                    for cmd in self._service.create_role(role_name, role_type, mgmt_host.hostId).get_commands():
                        check.status_for_command("Creating %s" % role_name, cmd)
            except ApiException as err:
                print "ERROR: %s " % err.message

        # now configure each role
        for group in [x for x in self._service.get_all_role_config_groups() if x.roleType in self._role_list]:
            if group.roleType == "ACTIVITYMONITOR":
                group.update_config({"firehose_database_host": "%s:7432" % socket.getfqdn(cmx.cm_server),
                                     "firehose_database_user": "amon",
                                     "firehose_database_password": cmx.amon_password,
                                     "firehose_database_type": "postgresql",
                                     "firehose_database_name": "amon",
                                     "firehose_heapsize": "615514112"})
            elif group.roleType == "ALERTPUBLISHER":
                group.update_config({})
            elif group.roleType == "EVENTSERVER":
                group.update_config({"event_server_heapsize": "492830720"})
            elif group.roleType == "HOSTMONITOR":
                group.update_config({"firehose_non_java_memory_bytes": "1610612736",
                                     "firehose_heapsize": "268435456"})
            elif group.roleType == "SERVICEMONITOR":
                group.update_config({"firehose_non_java_memory_bytes": "1610612736",
                                     "firehose_heapsize": "268435456"})
            elif group.roleType == "NAVIGATOR" and manager.licensed():
                group.update_config({"navigator_heapsize": "492830720"})
            elif group.roleType == "NAVIGATORMETASERVER" and manager.licensed():
                group.update_config({"navigator_heapsize": "1232076800"})
            elif group.roleType == "NAVIGATORMETADATASERVER" and manager.licensed():
                group.update_config({})
            elif group.roleType == "REPORTSMANAGER" and manager.licensed():
                group.update_config({"headlamp_database_host": "%s:7432" % socket.getfqdn(cmx.cm_server),
                                     "headlamp_database_name": "rman",
                                     "headlamp_database_password": cmx.rman_password,
                                     "headlamp_database_type": "postgresql",
                                     "headlamp_database_user": "rman",
                                     "headlamp_heapsize": "492830720"})

    @classmethod
    def licensed(cls):
        """
        Check if Cluster is licensed
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cm = api.get_cloudera_manager()
        try:
            return bool(cm.get_license().uuid)
        except ApiException as err:
            return "Express" not in err.message

    @classmethod
    def upload_license(cls):
        """
        Upload License file
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cm = api.get_cloudera_manager()
        if cmx.license_file and not manager.licensed():
            print "Upload license"
            with open(cmx.license_file, 'r') as f:
                license_contents = f.read()
                print "Upload CM License: \n %s " % license_contents
                cm.update_license(license_contents)
                # REPORTSMANAGER required after applying license
                manager("REPORTSMANAGER").setup()
                manager("REPORTSMANAGER").start()

    @classmethod
    def begin_trial(cls):
        """
        Begin Trial
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        print "def begin_trial"
        if not manager.licensed():
            try:
                api.post("/cm/trial/begin")
                # REPORTSMANAGER required after applying license
                manager("REPORTSMANAGER").setup()
                # manager("REPORTSMANAGER").start()
            except ApiException as err:
                print err.message

    @classmethod
    def get_mgmt_password(cls, role_type):
        """
        Get password for "ACTIVITYMONITOR', 'REPORTSMANAGER', 'NAVIGATOR"
        :param role_type:
        :return False if db.mgmt.properties is missing
        """
        contents = []
        mgmt_password = False

        if os.path.isfile('/etc/cloudera-scm-server/db.mgmt.properties'):
            try:
                print "> Reading %s password from /etc/cloudera-scm-server/db.mgmt.properties" % role_type
                with open(os.path.join('/etc/cloudera-scm-server', 'db.mgmt.properties')) as f:
                    contents = f.readlines()

                # role_type expected to be in
                # "ACTIVITYMONITOR', 'REPORTSMANAGER', 'NAVIGATOR"
                if role_type in ['ACTIVITYMONITOR', 'REPORTSMANAGER', 'NAVIGATOR']:
                    idx = "com.cloudera.cmf.%s.db.password=" % role_type
                    match = [s.rstrip('\n') for s in contents if idx in s][0]
                    mgmt_password = match[match.index(idx) + len(idx):]

            except IOError:
                print "Unable to open file: /etc/cloudera-scm-server/db.mgmt.properties"

        return mgmt_password

    @classmethod
    def get_hosts(cls, include_cm_host=False):
        """
        because api.get_all_hosts() returns all the hosts as instanceof ApiHost: hostId hostname ipAddress
        and cluster.list_hosts() returns all the cluster hosts as instanceof ApiHostRef: hostId
        we only need Cluster hosts with instanceof ApiHost: hostId hostname ipAddress + md5host
        preserve host order in -w
        hashlib.md5(host.hostname).hexdigest()
        attributes = {'id': None, 'hostId': None, 'hostname': None, 'md5host': None, 'ipAddress': None, }
        return a list of hosts
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)

        w_hosts = set(enumerate(cmx.host_names))
        if include_cm_host and socket.gethostbyname(cmx.cm_server) \
                not in [socket.gethostbyname(x) for x in cmx.host_names]:
            w_hosts.add((len(w_hosts), cmx.cm_server))

        hosts = []
        for idx, host in w_hosts:
            _host = [x for x in api.get_all_hosts() if x.ipAddress == socket.gethostbyname(host)][0]
            hosts.append({
                'id': idx,
                'hostId': _host.hostId,
                'hostname': _host.hostname,
                'md5host': hashlib.md5(_host.hostname).hexdigest(),
                'ipAddress': _host.ipAddress,
            })

        return [type('', (), x) for x in hosts]

    @classmethod
    def restart_management(cls):
        """
        Restart Management Services
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        mgmt = api.get_cloudera_manager().get_service()

        check.status_for_command("Stop Management services", mgmt.stop())
        check.status_for_command("Start Management services", mgmt.start())


class ServiceActions:
    """
    Example stopping/starting services ['HBASE', 'IMPALA', 'SPARK', 'SOLR']
    :param service_list:
    :param action:
    :return:
    """

    def __init__(self, *service_list):
        self._service_list = service_list
        self._api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                                version=cmx.api_version)
        self._cluster = self._api.get_cluster(cmx.cluster_name)

    def stop(self):
        self._action('stop')

    def start(self):
        self._action('start')

    def restart(self):
        self._action('restart')

    def _action(self, action):
        state = {'start': ['STOPPED'], 'stop': ['STARTED'], 'restart': ['STARTED', 'STOPPED']}
        for services in [x for x in self._cluster.get_all_services()
                         if x.type in self._service_list and x.serviceState in state[action]]:
            check.status_for_command("%s service %s" % (action.upper(), services.type),
                                     getattr(self._cluster.get_service(services.name), action)())

    @classmethod
    def get_service_type(cls, name):
        """
        Returns service based on service type name
        :param name:
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cluster = api.get_cluster(cmx.cluster_name)
        try:
            service = [x for x in cluster.get_all_services() if x.type == name][0]
        except IndexError:
            service = None

        return service

    @classmethod
    def deploy_client_config_for(cls, obj):
        """
        Example deploying GATEWAY Client Config on each host
        Note: only recommended if you need to deploy on a specific hostId.
        Use the cluster.deploy_client_config() for normal use.
        example usage:
        # hostId
        for host in get_cluster_hosts(include_cm_host=True):
            deploy_client_config_for(host.hostId)

        # cdh service
        for service in cluster.get_all_services():
            deploy_client_config_for(service)

        :param host.hostId, or ApiService:
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        # cluster = api.get_cluster(cmx.cluster_name)
        if isinstance(obj, str) or isinstance(obj, unicode):
            for role_name in [x.roleName for x in api.get_host(obj).roleRefs if 'GATEWAY' in x.roleName]:
                service = cdh.get_service_type('GATEWAY')
                print "Deploying client config for service: %s - host: [%s]" % \
                      (service.type, api.get_host(obj).hostname)
                check.status_for_command("Deploy client config for role %s" %
                                         role_name, service.deploy_client_config(role_name))
        elif isinstance(obj, ApiService):
            for role in obj.get_roles_by_type("GATEWAY"):
                check.status_for_command("Deploy client config for role %s" %
                                         role.name, obj.deploy_client_config(role.name))

    @classmethod
    def create_service_role(cls, service, role_type, host):
        """
        Helper function to create a role
        :return:
        """
        service_name = service.name[:4] + hashlib.md5(service.name).hexdigest()[:8] \
            if len(role_type) > 24 else service.name

        role_name = "-".join([service_name, role_type, host.md5host])[:64]
        print "Creating role: %s on host: [%s]" % (role_name, host.hostname)
        if not [role for role in service.get_all_roles() if role_name in role.name]:
            [check.status_for_command("Creating role: %s on host: [%s]" % (role_name, host.hostname), cmd)
             for cmd in service.create_role(role_name, role_type, host.hostId).get_commands()]

    @classmethod
    def restart_cluster(cls):
        """
        Restart Cluster and Cluster wide deploy client config
        :return:
        """
        api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                          version=cmx.api_version)
        cluster = api.get_cluster(cmx.cluster_name)
        print "Restart cluster: %s" % cmx.cluster_name
        check.status_for_command("Stop %s" % cmx.cluster_name, cluster.stop())
        check.status_for_command("Start %s" % cmx.cluster_name, cluster.start())
        # Example deploying cluster wide Client Config
        check.status_for_command("Deploy client config for %s" % cmx.cluster_name, cluster.deploy_client_config())

    @classmethod
    def dependencies_for(cls, service):
        """
        Utility function returns dict of service dependencies
        :return:
        """
        service_config = {}
        config_types = {"hue_webhdfs": ['NAMENODE', 'HTTPFS'], "hdfs_service": "HDFS", "sentry_service": "SENTRY",
                        "zookeeper_service": "ZOOKEEPER", "hbase_service": "HBASE",
                        "hue_hbase_thrift": "HBASETHRIFTSERVER", "solr_service": "SOLR",
                        "hive_service": "HIVE", "sqoop_service": "SQOOP",
                        "impala_service": "IMPALA", "oozie_service": "OOZIE",
                        "mapreduce_yarn_service": ['MAPREDUCE', 'YARN'], "yarn_service": "YARN"}

        dependency_list = []
        # get required service config
        for k, v in service.get_config(view="full")[0].items():
            if v.required:
                dependency_list.append(k)

        # Extended dependence list, adding the optional ones as well
        if service.type == 'HUE':
            dependency_list.extend(['hbase_service', 'solr_service', 'sqoop_service',
                                    'impala_service', 'hue_hbase_thrift'])
        if service.type in ['HIVE', 'HDFS', 'HUE', 'OOZIE', 'MAPREDUCE', 'YARN', 'ACCUMULO16']:
            dependency_list.append('zookeeper_service')
        if service.type in ['HIVE']:
            dependency_list.append('sentry_service')
        if service.type == 'OOZIE':
            dependency_list.append('hive_service')
        if service.type in ['FLUME', 'IMPALA']:
            dependency_list.append('hbase_service')
        if service.type in ['FLUME', 'SPARK', 'SENTRY', 'ACCUMULO16']:
            dependency_list.append('hdfs_service')
        if service.type == 'FLUME':
            dependency_list.append('solr_service')

        for key in dependency_list:
            if key == "hue_webhdfs":
                hdfs = cdh.get_service_type('HDFS')
                if hdfs is not None:
                    service_config[key] = [x.name for x in hdfs.get_roles_by_type('NAMENODE')][0]
                    # prefer HTTPS over NAMENODE
                    if [x.name for x in hdfs.get_roles_by_type('HTTPFS')]:
                        service_config[key] = [x.name for x in hdfs.get_roles_by_type('HTTPFS')][0]
            elif key == "mapreduce_yarn_service":
                for _type in config_types[key]:
                    if cdh.get_service_type(_type) is not None:
                        service_config[key] = cdh.get_service_type(_type).name
                    # prefer YARN over MAPREDUCE
                    if cdh.get_service_type(_type) is not None and _type == 'YARN':
                        service_config[key] = cdh.get_service_type(_type).name
            elif key == "hue_hbase_thrift":
                hbase = cdh.get_service_type('HBASE')
                if hbase is not None:
                    service_config[key] = [x.name for x in hbase.get_roles_by_type(config_types[key])][0]
            else:
                if cdh.get_service_type(config_types[key]) is not None:
                    service_config[key] = cdh.get_service_type(config_types[key]).name

        return service_config


class ActiveCommands:
    def __init__(self):
        self._api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
                                version=cmx.api_version)

    def status_for_command(self, message, command):
        """
        Helper to check active command status
        :param message:
        :param command:
        :return:
        """
        _state = 0
        _bar = ['[|]', '[/]', '[-]', '[\\]']
        while True:
            if self._api.get("/commands/%s" % command.id)['active']:
                sys.stdout.write(_bar[_state % 4] + ' ' + message + ' ' + ('\b' * (len(message) + 5)))
                sys.stdout.flush()
                _state += 1
                time.sleep(0.5)
            else:
                print "\n [%s] %s" % (command.id, self._api.get("/commands/%s" % command.id)['resultMessage'])
                self._child_cmd(self._api.get("/commands/%s" % command.id)['children']['items'])
                break

    def _child_cmd(self, cmd):
        """
        Helper cmd has child objects
        :param cmd:
        :return:
        """
        if len(cmd) != 0:
            print " Sub tasks result(s):"
            for resMsg in cmd:
                if resMsg.get('resultMessage'):
                    print "  [%s] %s" % (resMsg['id'], resMsg['resultMessage']) if not resMsg.get('roleRef') \
                        else "  [%s] %s - %s" % (resMsg['id'], resMsg['resultMessage'], resMsg['roleRef']['roleName'])
                self._child_cmd(self._api.get("/commands/%s" % resMsg['id'])['children']['items'])


def parse_options():
    global cmx
    global check, cdh, manager

    cmx_config_options = {'ssh_root_password': None, 'ssh_root_user': 'root', 'ssh_private_key': None,
                          'cluster_name': 'Cluster 1', 'cluster_version': 'CDH5',
                          'username': 'admin', 'password': 'admin', 'cm_server': None,
                          'host_names': None, 'license_file': None,
                          'parcel': [], 'archive_url': 'http://archive.cloudera.com'}

    cmx_config_options.update({'kerberos': {'kdc_host': None, 'security_realm': None,
                                            'kdc_user': None, 'kdc_password': None}})

    def cmx_args(option, opt_str, value, *args, **kwargs):
        if option.dest == 'host_names':
            print "switch %s value check: %s" % (opt_str, value)
            for host in value.split(','):
                if not hostname_resolves(host.strip()):
                    exit(1)
            else:
                cmx_config_options[option.dest] = [socket.gethostbyname(x.strip()) for x in value.split(',')]
        elif option.dest == 'cm_server':
            print "switch %s value check: %s" % (opt_str, value.strip())
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cmx_config_options[option.dest] = socket.gethostbyname(value.strip()) if \
                hostname_resolves(value.strip()) else exit(1)

            if not s.connect_ex((socket.gethostbyname(value), 7180)) == 0:
                print "Cloudera Manager Server is not started on %s " % value
                s.close()
                exit(1)

            # Determine the CM API version
            api_version = get_cm_api_version(cmx_config_options[option.dest],
                                             cmx_config_options['username'],
                                             cmx_config_options['password'])
            print "CM API version: %s" % api_version
            cmx_config_options.update({'api_version': api_version})

            # from CM 5.4+ API v10 we specify 'latest' CDH version with {latest_supported}
            if int(cmx_config_options['api_version'].strip("v")) >= 10:
                cmx_config_options.update({'cdh_version': '5'})
            else:
                cmx_config_options.update({'cdh_version': 'latest'})

        elif option.dest == 'ssh_private_key':
            with open(value, 'r') as f:
                cmx_config_options[option.dest] = f.read()
        elif option.dest == 'cdh_version':
            print "switch %s value check: %s" % (opt_str, value)
            _cdh_repo = urllib2.urlopen("%s/cdh5/parcels/" % cmx_config_options["archive_url"]).read()
            _cdh_ver = [link.replace('/', '') for link in re.findall(r"<a.*?\s*href=\".*?\".*?>(.*?)</a>", _cdh_repo)
                        if link not in ['Name', 'Last modified', 'Size', 'Description', 'Parent Directory']]
            cmx_config_options[option.dest] = value
            if value not in _cdh_ver:
                print "Invalid CDH version: %s" % value
                exit(1)
        else:
            cmx_config_options[option.dest] = value

    def hostname_resolves(hostname):
        """
        Check if hostname resolves
        :param hostname:
        :return:
        """
        try:
            if socket.gethostbyname(hostname) == '0.0.0.0':
                print "Error [{'host': '%s', 'fqdn': '%s'}]" % \
                      (socket.gethostbyname(hostname), socket.getfqdn(hostname))
                return False
            else:
                print "Success [{'host': '%s', 'fqdn': '%s'}]" % \
                      (socket.gethostbyname(hostname), socket.getfqdn(hostname))
                return True
        except socket.error:
            print "Error 'host': '%s'" % hostname
            return False

    def get_cm_api_version(cm_server, username, password):
        """
        Get supported API version from CM
        :param cm_server:
        :param username:
        :param password:
        :return version:
        """
        base_url = "%s://%s:%s/api" % ("http", cm_server, 7180)
        client = HttpClient(base_url, exc_class=ApiException)
        client.set_basic_auth(username, password, "Cloudera Manager")
        client.set_headers({"Content-Type": "application/json"})
        return client.execute("GET", "/version").read().strip('v')

    parser = OptionParser()
    parser.add_option('-d', '--teardown', dest='teardown', action="store", type="string",
                      help='Teardown Cloudera Manager Cluster. Required arguments "keep_cluster" or "remove_cluster".')
    parser.add_option('-i', '--cdh-version', dest='cdh_version', type="string", action='callback',
                      callback=cmx_args, default='latest', help='Install CDH version. Default "latest"')
    parser.add_option('-k', '--ssh-private-key', dest='ssh_private_key', type="string", action='callback',
                      callback=cmx_args, help='The private key to authenticate with the hosts. '
                                              'Specify either this or a password.')
    parser.add_option('-l', '--license-file', dest='license_file', type="string", action='callback',
                      callback=cmx_args, help='Cloudera Manager License file name')
    parser.add_option('-m', '--cm-server', dest='cm_server', type="string", action='callback', callback=cmx_args,
                      help='*Set Cloudera Manager Server Host. '
                           'Note: This is the host where the Cloudera Management Services get installed.')
    parser.add_option('-n', '--cluster-name', dest='cluster_name', type="string", action='callback',
                      callback=cmx_args, default='Cluster 1',
                      help='Set Cloudera Manager Cluster name enclosed in double quotes. Default "Cluster 1"')
    parser.add_option('-p', '--ssh-root-password', dest='ssh_root_password', type="string", action='callback',
                      callback=cmx_args, help='*Set target node(s) ssh password..')
    parser.add_option('-u', '--ssh-root-user', dest='ssh_root_user', type="string", action='callback',
                      callback=cmx_args, default='root', help='Set target node(s) ssh username. Default root')
    parser.add_option('-w', '--host-names', dest='host_names', type="string", action='callback',
                      callback=cmx_args,
                      help='*Set target node(s) list, separate with comma eg: -w host1,host2,...,host(n). '
                           'Note:'
                           ' - enclose in double quote.'
                           ' - CM_SERVER excluded in this list, if you want install CDH Services in CM_SERVER'
                           ' add the host to this list.')

    (options, args) = parser.parse_args()

    msg_req_args = "Please specify the required arguments: "
    if cmx_config_options['cm_server'] is None:
        parser.error(msg_req_args + "-m/--cm-server")
    else:
        if not (cmx_config_options['ssh_private_key'] or cmx_config_options['ssh_root_password']):
            parser.error(msg_req_args + "-p/--ssh-root-password or -k/--ssh-private-key")
        elif cmx_config_options['host_names'] is None:
            parser.error(msg_req_args + "-w/--host-names")
        elif cmx_config_options['ssh_private_key'] and cmx_config_options['ssh_root_password']:
            parser.error(msg_req_args + "-p/--ssh-root-password _OR_ -k/--ssh-private-key")

    # Management services password. They are required when adding Management services
    manager = ManagementActions
    if not (bool(manager.get_mgmt_password("ACTIVITYMONITOR"))
            and bool(manager.get_mgmt_password("REPORTSMANAGER"))):
        cmx_config_options['amon_password'] = bool(manager.get_mgmt_password("ACTIVITYMONITOR"))
        cmx_config_options['rman_password'] = bool(manager.get_mgmt_password("REPORTSMANAGER"))
    else:
        cmx_config_options['amon_password'] = manager.get_mgmt_password("ACTIVITYMONITOR")
        cmx_config_options['rman_password'] = manager.get_mgmt_password("REPORTSMANAGER")

    cmx = type('', (), cmx_config_options)
    check = ActiveCommands()
    cdh = ServiceActions
    if cmx_config_options['cm_server'] and options.teardown:
        if options.teardown.lower() in ['remove_cluster', 'keep_cluster']:
            teardown(keep_cluster=(options.teardown.lower() == 'keep_cluster'))
            print "Bye!"
            exit(0)
        else:
            print 'Teardown Cloudera Manager Cluster. Required arguments "keep_cluster" or "remove_cluster".'
            exit(1)

    # Uncomment here to see cmx configuration options
    # print cmx_config_options
    return options


def main():
    # Parse user options
    parse_options()

    # Prepare Cloudera Manager Server:
    # 1. Initialise Cluster and set Cluster name: 'Cluster 1'
    # 3. Add hosts into: 'Cluster 1'
    # 4. Deploy latest parcels into : 'Cluster 1'
    init_cluster()
    add_hosts_to_cluster()

    # Deploy CDH Parcel and GPL Extra Parcel skip if they are ACTIVATED
    api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password, version=cmx.api_version)
    cluster = api.get_cluster(cmx.cluster_name)
    for cdh_parcel in cmx.parcel:
        print "> Parcel action for parcel: [ %s-%s ]" % (cdh_parcel['product'], cdh_parcel['version'])
        parcel = cluster.get_parcel(product=cdh_parcel['product'], version=cdh_parcel['version'])
        if "ACTIVATED" not in parcel.stage:
            parcel_action(parcel_item=cdh_parcel, function="start_removal_of_distribution",
                          expected_stage=['DOWNLOADED', 'AVAILABLE_REMOTELY', 'ACTIVATING'],
                          action_description="Un-Distribute Parcel")
            parcel_action(parcel_item=cdh_parcel, function="start_download",
                          expected_stage=['DOWNLOADED'], action_description="Download Parcel")
            parcel_action(parcel_item=cdh_parcel, function="start_distribution", expected_stage=['DISTRIBUTED'],
                          action_description="Distribute Parcel")
            parcel_action(parcel_item=cdh_parcel, function="activate", expected_stage=['ACTIVATED'],
                          action_description="Activate Parcel")

    # Skip MGMT role installation if amon_password and rman_password password are False
    mgmt_roles = ['SERVICEMONITOR', 'ALERTPUBLISHER', 'EVENTSERVER', 'HOSTMONITOR']
    if cmx.amon_password and cmx.rman_password:
        if manager.licensed():
            mgmt_roles.append('REPORTSMANAGER')
        manager(*mgmt_roles).setup()
        # "STOP" Management roles
        # manager(*mgmt_roles).stop()
        # "START" Management roles
        manager(*mgmt_roles).start()

    # Upload license
    if cmx.license_file:
        manager.upload_license()

    # Begin Trial
    # manager.begin_trial()

    # Step-Through - Setup services in order of service dependencies
    # Zookeeper, hdfs, HBase, Solr, Spark, Yarn,
    # Hive, Sqoop, Sqoop Client, Impala, Oozie, Hue
    setup_zookeeper()
    setup_hdfs()
    setup_hbase()
    # setup_accumulo()
    # setup_solr()
    # setup_ks_indexer()
    setup_yarn()
    # setup_mapreduce()
    # setup_spark()
    setup_flume()
    setup_spark_on_yarn()
    setup_hive()
    # setup_sentry()
    setup_sqoop()
    setup_sqoop_client()
    setup_impala()
    setup_oozie()
    setup_hue()

    # Note: setup_easy() is alternative to Step-Through above
    # This this provides an example of alternative method of
    # using CM API to setup CDH services.
    # setup_easy()

    # Example setting hdfs-HA and yarn-HA
    # You can uncomment below after you've setup the CDH services.
    # setup_hdfs_ha()
    # setup_yarn_ha()

    # Example enable Kerberos
    # cmx.kerberos = {'kdc_host': 'mko.vpc.cloudera.com',
    #                 'security_realm': 'HADOOP.EXAMPLE.COM',
    #                 'kdc_user': 'mko/admin@HADOOP.EXAMPLE.COM',
    #                 'kdc_password': 'Had00p'}
    # enable_kerberos()
    # OR
    # disable_kerberos()

    # Restart Cluster and Deploy Cluster wide client config
    cdh.restart_cluster()

    # Other examples of CM API
    # eg: "STOP" Services or "START"
    cdh('HBASE', 'IMPALA', 'SPARK', 'SOLR', 'FLUME').stop()

    print "Enjoy!"


if __name__ == "__main__":
    print "%s" % '- ' * 20
    print "Version: %s" % __version__
    print "%s" % '- ' * 20
    main()

    # def setup_template():
    #     api = ApiResource(server_host=cmx.cm_server, username=cmx.username, password=cmx.password,
    #                       version=cmx.api_version)
    #     cluster = api.get_cluster(cmx.cluster_name)
    #     service_type = ""
    #     if cdh.get_service_type(service_type) is None:
    #         service_name = ""
    #         cluster.create_service(service_name.lower(), service_type)
    #         service = cluster.get_service(service_name)
    #
    #         # Service-Wide
    #         service.update_config(cdh.dependencies_for(service))
    #
    #         hosts = sorted([x for x in api.get_all_hosts()], key=lambda x: x.ipAddress, reverse=False)
    #
    #         # - Default Group
    #         role_group = service.get_role_config_group("%s-x-BASE" % service.name)
    #         role_group.update_config({})
    #         cdh.create_service_role(service, "X", [x for x in hosts if x.id == 0][0])
    #
    #         check.status_for_command("Starting x Service", service.start())
