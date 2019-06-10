#!/usr/bin/env python
__author__ = 'Michalis'
__version__ = '1.0.0'

import socket
import hashlib
import random
import time
import cm_client
from cm_client.rest import ApiException
import click
import urllib2
import json
import re
from HTMLParser import HTMLParser
from itertools import chain


@click.command()
@click.option('--hosts', help='*Set target node(s) list, separate with comma '
                              ' eg: --hosts host1,host2,...,host(n).')
@click.option('--cmhost', help='*Set Cloudera Manager Server Host. ')
@click.option('--cdhversion', '-i', help='*Set CDH version.')
@click.option('--teardown', '-d', is_flag=True, help='Teardown')
@click.option('--username', '-u', default='root', help='SSH username')
@click.option('--password', '-p', default='cloudera', help='SSH password')
def main(username, password, cmhost, hosts, cdhversion, teardown):
    cluster_name = 'Cluster 1'
    # parse options: check if --cmhost and --hosts can be resolved.
    if not hostname_resolves(cmhost.strip()):
        exit(1)
    for host in hosts.split(','):
        if not hostname_resolves(host.strip()):
            exit(1)
    # parse options: check if the parcel version is available.
    try:
        cdh_repo_url = 'https://archive.cloudera.com/cdh6/'
        parser = LinkParser()
        parser.feed(urllib2.urlopen(cdh_repo_url).read())
        cdh_version = [x for x in parser.links if cdhversion.strip() in x]
        cdh_parcel = manifest_to_dict("%s%s/parcels/manifest.json" % (cdh_repo_url, cdh_version[0]))
    except Exception as e:
        print("Exception: %s\n" % e)
        exit(1)

    # pre-setup
    cm_host = cmhost
    host_list = [host.strip().lower() for host in hosts.split(',')]

    # Configure HTTP basic authorization: basic
    cm_client.configuration.username = 'admin'
    cm_client.configuration.password = 'admin'
    # Construct base URL for API
    port = '7180'
    api_version = 'v32'
    # http://cmhost:7180/api/v32
    api_url = 'http://%s:%s/api/%s' % (cm_host, port, api_version)

    # Create an instance of the API class
    api_client = cm_client.ApiClient(api_url)

    if not teardown:
        # Class responsible for setting up the cluster.
        cmx = CmxApi(api_client, username, password, cluster_name, cm_host, host_list)

        # pre-init
        cmx.create_cluster()
        cmx.install_host(host_list=host_list)
        cmx.add_host_to_cluster(host_list=host_list)
        cmx.parcel(product=cdh_parcel['product'], version=cdh_parcel['version'])
        # setup services
        cmx.setup_zookeeper()
        cmx.setup_hdfs()
        cmx.setup_hbase()
        cmx.setup_yarn()
        cmx.setup_hive()
        cmx.setup_impala()
        cmx.setup_oozie()
        cmx.setup_hue()
        # execute first run
        cmx.cluster_first_run()

        # setup CMS
        cmx.setup_mgmt()
    else:
        remove_all(api_client, cluster_name)


def manifest_to_dict(manifest_json):
    if manifest_json:
        dir_list = json.load(
            urllib2.urlopen(manifest_json))['parcels'][0]['parcelName']
        parcel_part = re.match(r"^(.*?)-(.*)-(.*?)$", dir_list).groups()
        return {'product': str(parcel_part[0]).upper(), 'version': str(parcel_part[1]).lower()}
    else:
        raise Exception("Invalid manifest.json")


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


def remove_all(api_client, cluster_name):
    print "Teardown..."
    for service in cm_client.ServicesResourceApi(api_client).read_services(cluster_name=cluster_name).items:
        cm_client.ServicesResourceApi(api_client).delete_service(cluster_name, service.name)

    cm_client.ClustersResourceApi(api_client).delete_cluster(cluster_name='Cluster 1')
    cm_client.MgmtServiceResourceApi(api_client).delete_cms()


class CmxApi:
    def __init__(self, api_client, username, password, cluster_name, cm_host, host_list):
        self.api_client = api_client
        self.username = username
        self.password = password
        self.cluster_name = cluster_name
        self.cm_host = cm_host
        self.host_list = host_list

    def wait(self, cmd, timeout=None):
        SYNCHRONOUS_COMMAND_ID = -1
        if cmd.id == SYNCHRONOUS_COMMAND_ID:
            return cmd

        SLEEP_SECS = 5
        if timeout is None:
            deadline = None
        else:
            deadline = time.time() + timeout

        try:
            cmd_api_instance = cm_client.CommandsResourceApi(self.api_client)
            while True:
                cmd = cmd_api_instance.read_command(long(cmd.id))
                if not cmd.active:
                    return cmd

                if deadline is not None:
                    now = time.time()
                    if deadline < now:
                        return cmd
                    else:
                        time.sleep(min(SLEEP_SECS, deadline - now))
                else:
                    time.sleep(SLEEP_SECS)
        except ApiException as e:
            print "Exception reading and waiting for command %s\n" % e

    def create_roles(self, service_name, role_type, hostname):
        roles_instance = cm_client.RolesResourceApi(self.api_client)

        host_id = getattr(self.get_host_resource(hostname), 'host_id', None)
        first_part = service_name[:4] + hashlib.md5(service_name).hexdigest()[:8] \
            if len(role_type) > 24 else service_name
        role_name = "-".join([first_part, role_type, hashlib.md5(hostname).hexdigest()])[:64]
        # print "Creating role: %s on host: [%s]" % (role_name, hostname)
        api_response = roles_instance.create_roles(
            self.cluster_name, service_name, body=cm_client.ApiRoleList(
                [{'name': role_name,
                  'type': role_type,
                  'hostRef': {'hostId': host_id}
                  }])
        )
        return api_response.items[0]

    def get_host_resource(self, hostname):
        api_instance = cm_client.HostsResourceApi(self.api_client)
        try:
            # Returns the hostIds for all hosts in the system.
            # api_response = api_instance.read_hosts(view=view)
            api_host_response = [x for x in api_instance.read_hosts(view='summary').items
                                 if hostname == x.hostname]
        except ApiException as e:
            print("Exception when calling HostsResourceApi->read_hosts: %s\n" % e)

        return api_host_response[0]

    def dependencies_for(self, service_name):
        """
        Utility function returns dict of service dependencies
        :return:
        """
        service_config = []
        config_types = {"hue_webhdfs": ['NAMENODE', 'HTTPFS'], "hdfs_service": "HDFS", "sentry_service": "SENTRY",
                        "zookeeper_service": "ZOOKEEPER", "hbase_service": "HBASE",
                        "hue_hbase_thrift": "HBASETHRIFTSERVER", "solr_service": "SOLR",
                        "hive_service": "HIVE", "sqoop_service": "SQOOP",
                        "impala_service": "IMPALA", "oozie_service": "OOZIE",
                        "mapreduce_yarn_service": ['MAPREDUCE', 'YARN'], "yarn_service": "YARN"}

        dependency_list = []
        # get required service config
        services_instance = cm_client.ServicesResourceApi(self.api_client)
        service_conf = services_instance.read_service_config(self.cluster_name, service_name, view='full')

        for name, required in [(config.name, config.required) for config in service_conf.items if config.required]:
            if required:
                dependency_list.append(name)

        service = services_instance.read_service(self.cluster_name, service_name)
        # print service.type

        # Extended dependence list, adding the optional ones as well
        if service.type == 'HUE':
            dependency_list.extend(['hbase_service', 'solr_service', 'sqoop_service',
                                    'impala_service', 'hue_hbase_thrift', 'hive_service'])
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

        role_instance = cm_client.RolesResourceApi(self. api_client)
        for key in dependency_list:
            if key == "hue_webhdfs":
                service_name = self.get_service_type('HDFS')

                if service_name is not None:
                    service_roles = role_instance.read_roles(self.cluster_name, service_name.name,
                                                             filter='', view='full')
                    service_config.append({'name': key,
                                           'value': [role for role in service_roles.items
                                                     if 'NAMENODE' == role.type][0].name})

                if [role for role in service_roles.items if 'HTTPFS' == role.type][0]:
                    service_config.remove({'name': key,
                                           'value': [role for role in service_roles.items
                                                     if 'NAMENODE' == role.type][0].name})
                    service_config.append({'name': key,
                                           'value': [role for role in service_roles.items
                                                     if 'HTTPFS' == role.type][0].name})

            elif key == "mapreduce_yarn_service":
                for _type in config_types[key]:
                    if self.get_service_type(_type) is not None:
                        service_config.append({'name': key, 'value': self.get_service_type(_type).name})
                    # prefer YARN over MAPREDUCE
                    if self.get_service_type(_type) is not None and _type == 'YARN':
                        service_config.remove({'name': key, 'value': self.get_service_type(_type).name})
                        service_config.append({'name': key, 'value': self.get_service_type(_type).name})

            elif key == "hue_hbase_thrift":
                service_name = self.get_service_type('HBASE')
                if service_name is not None:
                    service_roles = role_instance.read_roles(self.cluster_name, service_name.name,
                                                             filter='', view='full')
                    service_config.append({'name': key, 'value':
                        [role for role in service_roles.items if config_types[key] == role.type][0].name})
            else:
                if self.get_service_type(config_types[key]) is not None:
                    service_config.append({'name': key, 'value': self.get_service_type(config_types[key]).name})

        return service_config

    def get_service_type(self, name):
        """
        Returns service based on service type name
        :param name:
        :return:
        """

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        try:
            service = [x for x in services_instance.read_services('Cluster 1').items if name.upper() == x.type][0]
        except IndexError:
            service = None

        return service

    def create_cluster(self):
        cluster_api_instance = cm_client.ClustersResourceApi(self.api_client)
        # Creates a collection of clusters.
        print "Creating cluster name: %s" % self.cluster_name
        try:
            api_response = cluster_api_instance.create_clusters(
                body=cm_client.ApiClusterList([{'name': self.cluster_name,
                                                'fullVersion': '6.0.0',
                                                'clusterType': 'BASE_CLUSTER'}]))
        except ApiException as e:
            print("Exception %s\n" % e)

    def install_host(self, host_list):
        # Install Host to CM
        cm_instance = cm_client.ClouderaManagerResourceApi(self.api_client)
        print "Installing Host(s)..."

        try:
            cm_instance.update_config(message=None, body=cm_client.ApiConfigList(
                [{'name': 'SESSION_TIMEOUT', 'value': '99999999999999'},
                 {'name': 'PHONE_HOME', 'value': False}
                 ]))

            # Perform installation on a set of hosts.
            api_response = cm_instance.host_install_command(
                body=cm_client.ApiHostInstallArguments(host_names=host_list, user_name=self.username,
                                                       password=self.password, unlimited_jce=True))
            self.wait(api_response)
        except ApiException as e:
            print("Exception %s\n" % e)

    def add_host_to_cluster(self, host_list):
        # Add host to cluster
        hosts = []
        api_instance = cm_client.ClustersResourceApi(self.api_client)
        print "Adding host(s) to Cluster."
        try:
            for host in host_list:
                hosts.append({'hostname': host, 'hostId': getattr(self.get_host_resource(host), 'host_id', None)})
            api_response = api_instance.add_hosts(self.cluster_name, body=cm_client.ApiHostRefList(hosts))
        except ApiException as e:
            print("Exception: %s\n" % e)

    def _check_parcel_stage(self, product, version, expected_stage):

        parcel_instance = cm_client.ParcelResourceApi(self.api_client)

        while True:
            cdh_parcel = parcel_instance.read_parcel(cluster_name=self.cluster_name,
                                                     product=product, version=version)
            if cdh_parcel.stage in expected_stage:
                break
            if cdh_parcel.state.errors:
                raise Exception(str(cdh_parcel.state.errors))

            # print " [%s: %s / %s]" % (cdh_parcel.stage, cdh_parcel.state.progress, cdh_parcel.state.total_progress)
            time.sleep(1)

    def parcel_action(self, product, version, function, expected_stage, action_description):
        parcel_instance = cm_client.ParcelResourceApi(self.api_client)

        print "%s [%s-%s]" % (action_description, product, version)
        cdh_parcel = parcel_instance

        cmd = getattr(cdh_parcel, function)(cluster_name=self.cluster_name, product=product, version=version)
        if not cmd.success:
            print "ERROR: %s failed!" % action_description
            exit(0)
        return self._check_parcel_stage(product, version, expected_stage)

    def parcel(self, product, version):
        # Deploy Parcel > Download> Distribute> Activate
        parcel_instance = cm_client.ParcelResourceApi(self.api_client)
        try:
            # A synchronous command that starts the parcel download.
            cluster_parcel = parcel_instance.read_parcel(self.cluster_name,
                                                         product=product,
                                                         version=version)
            if "ACTIVATED" not in cluster_parcel.stage:
                self.parcel_action(product=product, version=version, function="start_removal_of_distribution_command",
                                   expected_stage=['DOWNLOADED', 'AVAILABLE_REMOTELY', 'ACTIVATING'],
                                   action_description="Un-Distribute Parcel")
                self.parcel_action(product=product, version=version, function="start_download_command",
                                   expected_stage=['DOWNLOADED'], action_description="Download Parcel")
                self.parcel_action(product=product, version=version, function="start_distribution_command",
                                   expected_stage=['DISTRIBUTED'],
                                   action_description="Distribute Parcel")
                self.parcel_action(product=product, version=version, function="activate_command",
                                   expected_stage=['ACTIVATED'], action_description="Activate Parcel")
            elif 'ACTIVATED' == cluster_parcel.stage:
                self.parcel_action(product=product, version=version,
                                   function="deactivate", expected_stage=['DISTRIBUTED'],
                                   action_description="Deactivate Parcel")
                self.parcel_action(product=product, version=version,
                                   function="start_removal_of_distribution", expected_stage=['DOWNLOADED'],
                                   action_description="Un-Distribute Parcel")

        except ApiException as e:
            print("Exception when calling ParcelResourceApi->start_download_command: %s\n" % e)

    def setup_zookeeper(self):
        service_name = 'ZooKeeper'
        service_type = 'ZOOKEEPER'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )

            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'SERVER':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'maxClientCnxns', 'value': '1024'},
                            {'name': 'zookeeper_server_java_heapsize', 'value': '492830720'},
                            {'name': 'maxSessionTimeout', 'value': '60000'}
                        ])
                    )

                    # Create new roles in a given service.
                    for hostname in random.sample(self.host_list, 3 if len(self.host_list) >= 3 else 1):
                        role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                                 hostname=hostname)

        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_hdfs(self):
        service_name = 'hdfs'
        service_type = 'HDFS'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )

            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'NAMENODE':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'dfs_name_dir_list', 'value': '/data/dfs/nn'},
                            {'name': 'namenode_java_heapsize', 'value': '1073741824'},
                            {'name': 'dfs_namenode_handler_count', 'value': '30'},
                            {'name': 'dfs_namenode_service_handler_count', 'value': '30'},
                            {'name': 'dfs_namenode_servicerpc_address', 'value': '8022'},
                        ])
                    )
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name,
                                             role_type=rcg.role_type,
                                             hostname=self.host_list[0])

                if rcg.role_type == "SECONDARYNAMENODE":
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'fs_checkpoint_dir_list', 'value': '/data/dfs/snn'},
                            {'name': 'secondary_namenode_java_heapsize', 'value': '1073741824'},
                        ])
                    )
                    # Create new roles in a given service.
                    secondary_nn = random.choice([host for host in self.host_list[1:len(self.host_list)]]) \
                        if len(self.host_list) > 1 else random.choice(self.host_list)

                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=secondary_nn)
                if rcg.role_type == "DATANODE":
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'datanode_java_heapsize', 'value': '127926272'},
                            {'name': 'dfs_data_dir_list', 'value': '/data/dfs/dn'},
                            {'name': 'dfs_datanode_data_dir_perm', 'value': '755'},
                            {'name': 'dfs_datanode_du_reserved', 'value': '3218866585'},
                            {'name': 'dfs_datanode_failed_volumes_tolerated', 'value': '0'},
                            {'name': 'dfs_datanode_max_locked_memory', 'value': '316669952'},
                        ])
                    )
                    # Create new roles in a given service.
                    for hostname in self.host_list:
                        role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                                 hostname=hostname)

                if rcg.role_type == "BALANCER":
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'balancer_java_heapsize', 'value': '492830720'},
                        ])
                    )
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
                if rcg.role_type == "GATEWAY":
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'dfs_client_use_trash', 'value': True},
                        ])
                    )
                    # Create new roles in a given service.
                    for hostname in self.host_list:
                        role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                                 hostname=hostname)
                if rcg.role_type == "HTTPFS":
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))

        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_hbase(self):
        service_name = 'HBase'
        service_type = 'HBASE'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )

            # Update hbase service dependency
            services_instance.update_service_config(self.cluster_name, service_name, message=None,
                                                    body=cm_client.ApiServiceConfig(
                                                        self.dependencies_for(service_name=service_name))
                                                    )

            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'MASTER':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'hbase_master_java_heapsize', 'value': '492830720'},
                        ])
                    )

                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=self.host_list[0])

                if rcg.role_type == 'REGIONSERVER':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'hbase_regionserver_java_heapsize', 'value': '365953024'},
                            {'name': 'hbase_regionserver_java_opts', 'value': '-XX:+UseParNewGC '
                                                                              '-XX:+UseConcMarkSweepGC '
                                                                              '-XX:-CMSConcurrentMTEnabled '
                                                                              '-XX:CMSInitiatingOccupancyFraction=70 '
                                                                              '-XX:+CMSParallelRemarkEnabled '
                                                                              '-verbose:gc -XX:+PrintGCDetails '
                                                                              '-XX:+PrintGCDateStamps'},
                        ])
                    )

                    # Create new roles in a given service.
                    for hostname in self.host_list:
                        role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                                 hostname=hostname)
                elif rcg.role_type in ['HBASETHRIFTSERVER', 'HBASERESTSERVER']:
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))

        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_yarn(self):
        service_name = 'yarn'
        service_type = 'YARN'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )

            # Update yarn service dependency
            services_instance.update_service_config(self.cluster_name, service_name, message=None,
                                                    body=cm_client.ApiServiceConfig(
                                                        self.dependencies_for(service_name=service_name))
                                                    )

            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'RESOURCEMANAGER':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'resource_manager_java_heapsize', 'value': '492830720'},
                            {'name': 'yarn_scheduler_maximum_allocation_mb', 'value': '2568'},
                            {'name': 'yarn_scheduler_maximum_allocation_vcores', 'value': '2'}
                        ])
                    )

                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
                if rcg.role_type == 'JOBHISTORY':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'mr2_jobhistory_java_heapsize', 'value': '492830720'},
                        ])
                    )

                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
                if rcg.role_type == 'NODEMANAGER':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'yarn_nodemanager_heartbeat_interval_ms', 'value': '100'},
                            {'name': 'yarn_nodemanager_local_dirs', 'value': '/data/yarn/nm'},
                            {'name': 'yarn_nodemanager_resource_cpu_vcores', 'value': '2'},
                            {'name': 'yarn_nodemanager_resource_memory_mb', 'value': '2568'},
                            {'name': 'node_manager_java_heapsize', 'value': '127926272'},
                        ])
                    )

                    # Create new roles in a given service.
                    for hostname in self.host_list:
                        role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                                 hostname=hostname)

        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_hive(self):
        service_name = 'hive'
        service_type = 'HIVE'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )

            # Update hive service dependency
            # hive_metastore_database_host: Assuming embedded DB is running from where embedded-db is located.
            service_config = [{'name': 'hive_metastore_database_host', 'value': self.cm_host},
                              {'name': 'hive_metastore_database_user', 'value': 'hive'},
                              {'name': 'hive_metastore_database_name', 'value': 'hive'},
                              {'name': 'hive_metastore_database_password', 'value': 'cloudera'},
                              {'name': 'hive_metastore_database_port', 'value': '7432'},
                              {'name': 'hive_metastore_database_type', 'value': 'postgresql'}, ]

            for kv in self.dependencies_for(service_name=service_name):
                service_config.append(kv)
            services_instance.update_service_config(self.cluster_name, service_name, message=None,
                                                    body=cm_client.ApiServiceConfig(service_config))

            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'HIVEMETASTORE':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'hive_metastore_java_heapsize', 'value': '492830720'},
                        ])
                    )

                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))

                if rcg.role_type == 'HIVESERVER2':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'hiveserver2_java_heapsize', 'value': '144703488'},
                            {'name': 'hiveserver2_spark_executor_cores', 'value': '4'},
                        ])
                    )

                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_impala(self):
        service_name = 'impala'
        service_type = 'IMPALA'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )
            # Update impala service dependency
            services_instance.update_service_config(self.cluster_name, service_name, message=None,
                                                    body=cm_client.ApiServiceConfig(
                                                        self.dependencies_for(service_name=service_name))
                                                    )

            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'IMPALAD':
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([
                            {'name': 'impalad_memory_limit', 'value': '618659840'},
                            {'name': 'enable_audit_event_log', 'value': True},
                            {'name': 'scratch_dirs', 'value': '/data/impala/impalad'},
                        ])
                    )

                    # Create new roles in a given service.
                    for hostname in self.host_list:
                        role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                                 hostname=hostname)
                if rcg.role_type == 'CATALOGSERVER':
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
                if rcg.role_type == 'STATESTORE':
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_oozie(self):
        service_name = 'oozie'
        service_type = 'OOZIE'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )
            # Update oozie service dependency
            services_instance.update_service_config(self.cluster_name, service_name, message=None,
                                                    body=cm_client.ApiServiceConfig(
                                                        self.dependencies_for(service_name=service_name))
                                                    )
            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'OOZIE_SERVER':
                    # oozie_database_host: Assuming embedded DB is running from where embedded-db is located.
                    rcg_instance.update_config(
                        self.cluster_name, rcg.name, service_name, message=None,
                        body=cm_client.ApiConfigList([{'name': 'oozie_database_user', 'value': 'oozie'},
                                                      {'name': 'oozie_database_host', 'value':
                                                          '%s:%s' % (self.cm_host, "7432")},
                                                      {'name': 'oozie_database_password', 'value': 'cloudera'},
                                                      {'name': 'oozie_database_type', 'value': 'postgresql'},
                                                      {'name': 'oozie_java_heapsize', 'value': '492830720'},])
                    )

                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))

        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_hue(self):
        service_name = 'hue'
        service_type = 'HUE'.upper()

        services_instance = cm_client.ServicesResourceApi(self.api_client)
        rcg_instance = cm_client.RoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a list of services.
            print "Create a %s service and its associated roles." % service_name
            services_instance.create_services(self.cluster_name,
                                              body=cm_client.ApiServiceList(
                                                  [{'name': service_name,
                                                    'type': service_type}])
                                              )
            # Update hue service dependency
            services_instance.update_service_config(self.cluster_name, service_name, message=None,
                                                    body=cm_client.ApiServiceConfig(
                                                        self.dependencies_for(service_name=service_name))
                                                    )
            # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups(self.cluster_name, service_name)
            for rcg in api_response.items:
                if rcg.role_type == 'HUE_SERVER':
                    # Create new roles in a given service.
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))
                if rcg.role_type == 'HUE_LOAD_BALANCER':
                    role = self.create_roles(service_name=service_name, role_type=rcg.role_type,
                                             hostname=random.choice(self.host_list))


        except ApiException as e:
            print("Exception: %s\n" % e)

    def setup_mgmt(self):
        service_name = 'mgmt'
        service_type = 'MGMT'.upper()
        hostname = self.cm_host
        host_id = getattr(self.get_host_resource(self.cm_host), 'host_id', None)

        services_instance = cm_client.MgmtServiceResourceApi(self.api_client)
        mgmt_role_instance = cm_client.MgmtRolesResourceApi(self.api_client)
        rcg_instance = cm_client.MgmtRoleConfigGroupsResourceApi(self.api_client)

        try:
            # Creates a CMS service.
            print "Create a CMS service and its associated roles."
            services_instance.setup_cms(body=cm_client.ApiService(name=service_name,
                                                                  type=service_type,
                                                                  display_name='Cloudera Management Service'))

            # TODO: "REPORTSMANAGER"
            role_list = []
            for role_type in ["EVENTSERVER", "HOSTMONITOR", "ALERTPUBLISHER", "SERVICEMONITOR"]:
                role_name = "%s-%s-%s" % (service_name, role_type, hashlib.md5(hostname).hexdigest())
                role_list.append({"name": role_name, "type": role_type, "hostRef": {"hostId": host_id}})

            body = cm_client.ApiRoleList(role_list)
            api_response = mgmt_role_instance.create_roles(body=body)

            # # Updates the config for the given role config group.
            api_response = rcg_instance.read_role_config_groups()
            for rcg in api_response.items:
                if rcg.role_type == 'EVENTSERVER':
                    rcg_instance.update_config(rcg.name, message=None,
                                               body=cm_client.ApiConfigList([
                                                   {'name': 'event_server_heapsize', 'value': '492830720'},
                                               ]))
                if rcg.role_type == 'HOSTMONITOR':
                    rcg_instance.update_config(rcg.name, message=None,
                                               body=cm_client.ApiConfigList([
                                                   {'name': 'firehose_non_java_memory_bytes', 'value': '1610612736'},
                                                   {'name': 'firehose_heapsize', 'value': '268435456'},
                                               ]))
                if rcg.role_type == 'SERVICEMONITOR':
                    rcg_instance.update_config(rcg.name, message=None,
                                               body=cm_client.ApiConfigList([
                                                   {'name': 'firehose_non_java_memory_bytes', 'value': '1610612736'},
                                                   {'name': 'firehose_heapsize', 'value': '268435456'},
                                               ]))

            services_instance.start_command()
        except ApiException as e:
            print("Exception: %s\n" % e)

    def cluster_first_run(self):
        # create an instance of the API class
        api_instance = cm_client.ClustersResourceApi(self.api_client)
        print "First Run command..."
        try:
            # Prepare and start services in a cluster.
            self.wait(api_instance.first_run(self.cluster_name))
        except ApiException as e:
            print("Exception when calling ClustersResourceApi->first_run: %s\n" % e)


class LinkParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.links = iter([])

    def reset(self):
        HTMLParser.reset(self)

    def handle_starttag(self, tag, attrs):
        # Only parse the 'anchor' tag.

        if tag == "a":
            for name, value in attrs:
                if '6' in value:
                    self.links = chain(self.links, [value.strip('/')])


if __name__ == "__main__":
    main()
