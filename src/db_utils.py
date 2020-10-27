import sqlalchemy
from sqlalchemy import Table, Column, String, MetaData, Integer, Boolean, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import json

import pandas as pd
import numpy as np

pd.options.display.max_columns = 999
pd.set_option('display.width', 1000)
pd.options.display.max_rows = 100


class dbUtils():

    def __init__(self, db_settings):
        self.engine = None
        self.connect(db_settings)
        self.get_providers()
        self.get_data_ids()
        self.create_view_table()

    def connect(self, db_settings):
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(db_settings['user'], db_settings['password'], db_settings['host'], db_settings['port'],
                         db_settings['db'])

        # The return value of create_engine() is our connection object
        self.engine = sqlalchemy.create_engine(url, client_encoding='utf8')

        # We then bind the connection to MetaData()
        # meta = sqlalchemy.MetaData(bind=con, reflect=True)

        return self.engine  # , meta

    def get_providers(self):
        self.providers = list()
        self.providers = pd.read_sql_query('select * from framework.tb_provider', self.engine)['prvd_id']
        return self.providers

    def get_data_ids(self):
        self.dataid_dict = dict()
        for prvd_id in self.providers:
            self.dataid_dict[prvd_id] = pd.read_sql_query('''
                select tdm.data_id as data_id, tdm.data_tp as data_type, tdm.nm as data_name, 
        tpl.tplg_type as toplogy_type, tpl.toplogy_id, tpl.nm as toplogy_name, tpl.level as level
        from framework.tb_data_mgmt tdm,
        (with recursive code_list(toplogy_id, nm, tplg_type, p_toplogy_id, level, path, cycle) as (
        select tdt.toplogy_id,
        tdt.nm,tdt.tplg_type,
        tdt.p_toplogy_id,1,
        array[tdt.toplogy_id::text],
        false
        from framework.tb_data_toplogy tdt, framework.tb_provider tp 
        where tdt.toplogy_id = tp.toplogy_id and tp.prvd_id = '{}'
        union all
        select tdt.toplogy_id,
        tdt.nm,
        tdt.tplg_type,
        tdt.p_toplogy_id,
        cl.level+1,
        array_append(cl.path, tdt.toplogy_id::text),
        tdt.toplogy_id = any(cl.path)
        from framework.tb_data_toplogy tdt, code_list cl
        where tdt.p_toplogy_id = cl.toplogy_id
        and not cycle)
        select toplogy_id, nm, tplg_type, p_toplogy_id, level
        from code_list) tpl
        where tdm.toplogy_id = tpl.toplogy_id
        --and tdm.data_id = 'e17d36e1ba954e7c97a5e9d46c259383';
        order by tpl.level;
        '''.format(prvd_id), self.engine)
        return self.dataid_dict

    def create_view_table(self):
        self.provider_env_dict = dict()
        tb_provider = pd.read_sql_query("select prvd_id, nm from framework.tb_provider", self.engine)
        for prov_id, data_ids in self.get_data_ids().items():
            if "TEST" in prov_id:
                pass
            elif 'vmware' in prov_id:
                datacenter_env_id = list(data_ids[data_ids['data_name'] == 'vmware_datacenter_env']['data_id'])[0]
                cluster_env_id = list(data_ids[data_ids['data_name'] == 'vmware_cluster_env']['data_id'])[0]
                host_env_id = list(data_ids[data_ids['data_name'] == 'vmware_host_env']['data_id'])[0]
                vm_env_id = list(data_ids[data_ids['data_name'] == 'vmware_vm_env']['data_id'])[0]

                datacenter_env = pd.read_sql_query(
                    "select resource_id as datacenter_resource_id, name as datacenter_name from vmware.tb_metric_datacenter_env_{datacenter_env_id} where time=(select max(time) from vmware.tb_metric_datacenter_env_{datacenter_env_id})".format(
                        datacenter_env_id=datacenter_env_id), self.engine)
                cluster_env = pd.read_sql_query(
                    "select resource_id as cluster_resource_id, name as cluster_name, parent_resource_id as datacenter_resource_id from vmware.tb_metric_cluster_env_{cluster_env_id} where time=(select max(time) from vmware.tb_metric_cluster_env_{cluster_env_id})".format(
                        cluster_env_id=cluster_env_id), self.engine)
                host_env = pd.read_sql_query(
                    "select resource_id as host_resource_id, name as host_name, parent_resource_id as cluster_resource_id from vmware.tb_metric_host_env_{host_env_id} where time=(select max(time) from vmware.tb_metric_host_env_{host_env_id})".format(
                        host_env_id=host_env_id), self.engine)
                vm_env = pd.read_sql_query(
                    "select resource_id as vm_resource_id, name as vm_name , parent_resource_id as host_resource_id  from vmware.tb_metric_vm_env_{vm_env_id} where time=(select max(time) from vmware.tb_metric_vm_env_{vm_env_id})".format(
                        vm_env_id=vm_env_id), self.engine)

                merged_1 = pd.merge(datacenter_env, cluster_env)
                merged_2 = pd.merge(merged_1, host_env)
                merged_3 = pd.merge(merged_2, vm_env)
                merged_3['provider_id'] = prov_id
                merged_3['virtualcenter_resource_id'] = prov_id
                merged_3['virtualcenter_name'] = tb_provider[tb_provider['prvd_id'] == prov_id]['nm'].iloc[0]
                self.provider_env_dict[prov_id] = merged_3
            elif 'openstack' in prov_id:
                region_env_id = list(data_ids[data_ids['data_name'] == 'openstack_region_env']['data_id'])[0]
                zone_env_id = list(data_ids[data_ids['data_name'] == 'openstack_zone_env']['data_id'])[0]
                domain_env_id = list(data_ids[data_ids['data_name'] == 'openstack_domain_env']['data_id'])[0]
                project_env_id = list(data_ids[data_ids['data_name'] == 'openstack_project_env']['data_id'])[0]
                host_env_id = list(data_ids[data_ids['data_name'] == 'openstack_host_env']['data_id'])[0]
                vm_env_id = list(data_ids[data_ids['data_name'] == 'openstack_vm_env']['data_id'])[0]

                region_env = pd.read_sql_query(
                    "select resource_id as region_resource_id, resource_id as region_name from openstack.tb_metric_region_env_{region_env_id} where time=(select max(time) from openstack.tb_metric_region_env_{region_env_id})".format(
                        region_env_id=region_env_id), self.engine)
                zone_env = pd.read_sql_query(
                    "select resource_id as zone_resource_id, resource_id as zone_name, region_id as region_resource_id from openstack.tb_metric_zone_env_{zone_env_id} where time=(select max(time) from openstack.tb_metric_zone_env_{zone_env_id})".format(
                        zone_env_id=zone_env_id), self.engine)
                domain_env = pd.read_sql_query(
                    "select resource_id as domain_resource_id, name as domain_name, region_id as region_resource_id from openstack.tb_metric_domain_env_{domain_env_id} where time=(select max(time) from openstack.tb_metric_domain_env_{domain_env_id})".format(
                        domain_env_id=domain_env_id), self.engine)

                project_env = pd.read_sql_query(
                    "select resource_id as project_resource_id, name as project_name, domain_id as domain_resource_id from openstack.tb_metric_project_env_{project_env_id} where time=(select max(time) from openstack.tb_metric_project_env_{project_env_id})".format(
                        project_env_id=project_env_id), self.engine)

                host_env = pd.read_sql_query(
                    "select resource_id as host_resource_id, name as host_name, availability_zone as zone_resource_id from openstack.tb_metric_host_env_{host_env_id} where time=(select max(time) from openstack.tb_metric_host_env_{host_env_id})".format(
                        host_env_id=host_env_id), self.engine)
                vm_env_physical = pd.read_sql_query(
                    "select resource_id as vm_resource_id, name as vm_name , hypervisor_host_name as host_resource_id  from openstack.tb_metric_vm_env_{vm_env_id} where time=(select max(time) from openstack.tb_metric_vm_env_{vm_env_id})".format(
                        vm_env_id=vm_env_id), self.engine)

                vm_env_logical = pd.read_sql_query(
                    "select resource_id as vm_resource_id, name as vm_name , project_id as project_resource_id  from openstack.tb_metric_vm_env_{vm_env_id} where time=(select max(time) from openstack.tb_metric_vm_env_{vm_env_id})".format(
                        vm_env_id=vm_env_id), self.engine)

                merged_1 = pd.merge(region_env, zone_env)
                merged_2 = pd.merge(merged_1, host_env)
                merged_3 = pd.merge(merged_2, vm_env_physical)
                merged_3['provider_id'] = prov_id
                merged_3['provider_resource_id'] = prov_id
                merged_3['provider_name'] = tb_provider[tb_provider['prvd_id'] == prov_id]['nm'].iloc[0]
                self.provider_env_dict['{prov_id}_physical'.format(prov_id=prov_id)] = merged_3

                merged_1 = pd.merge(region_env, domain_env)
                merged_2 = pd.merge(merged_1, project_env)
                merged_3 = pd.merge(merged_2, vm_env_logical)
                merged_3['provider_id'] = prov_id
                merged_3['provider_resource_id'] = prov_id
                merged_3['provider_name'] = tb_provider[tb_provider['prvd_id'] == prov_id]['nm'].iloc[0]
                self.provider_env_dict['{prov_id}_logical'.format(prov_id=prov_id)] = merged_3

    def get_anomaly_status(self, prvdType='openstack', prvdId='openstack20200709080543', tplgType='host',
                           metricType='network', task='anomaly',
                           resource_id=None, model=None, startdate=None, enddate=None, sort=None,
                           aggr=None, timebucket=None, column=None, measure=None):

        data_ids = self.get_data_ids()[prvdId]
        if resource_id:
            sub = False
            if ":sub" in resource_id:
                sub = True
                originType = tplgType
                resource_id, _ = resource_id.split(":")
                if tplgType == 'virtualcenter':
                    tplgType = 'datacenter'
                elif tplgType == 'datacenter':
                    tplgType = 'cluster'
                elif tplgType == 'cluster':
                    tplgType = 'host'
                elif tplgType == 'provider':
                    tplgType = 'region'
                elif tplgType == 'region':
                    tplgType = 'zone'
                elif tplgType == 'zone':
                    tplgType = 'host'
                elif tplgType == 'domain':
                    tplgType = 'project'
                elif tplgType == 'project':
                    tplgType = 'vm'
        else:
            sub = False

        if tplgType in ['vm', 'project', 'domain']:
            datatype = 'vm'
            data_id = list(data_ids[data_ids['data_name'] == '{prvdType}_{datatype}_{metricType}'
                           .format(prvdType=prvdType, datatype=datatype, metricType=metricType)]['data_id'])[0]
        else:
            datatype = 'host'
            data_id = list(data_ids[data_ids['data_name'] == '{prvdType}_{datatype}_{metricType}'
                           .format(prvdType=prvdType, datatype=datatype, metricType=metricType)]['data_id'])[0]
        where_clause = ''
        where_params = []
        # if resource_id:
        #     where_params.append("resource_id = '{resource_id}'".format(resource_id=resource_id)) #resource_id는 조회하고자 하는것의 resource_id일듯
        if model:
            where_params.append("model = '{model}'".format(model=model))
        if startdate:
            where_params.append("time > '{startdate}'".format(startdate=startdate))
        if enddate:
            where_params.append("time < '{enddate}'".format(enddate=enddate))

        if measure:
            measure = measure.split(",")
            for measure_type in measure:
                where_params.append("metrictype = '{measure_type}'".format(measure_type=measure_type))

        if where_params:
            where_clause = ' where ' + ' and '.join(where_params)

        aggr_value = 'avg'
        aggr_score = 'avg'
        if aggr:
            aggr_list = aggr.split(',')
            aggr_dict = {}
            for items in aggr_list:
                value, parameter = items.split(':')[0], items.split(':')[1]
                aggr_dict[value] = parameter
            if 'value' in aggr_dict:
                aggr_value = aggr_dict['value']
            if 'score' in aggr_dict:
                aggr_score = aggr_dict['score']

        if timebucket:
            if task == 'anomaly':
                query = "select time_bucket('{timebucket}',time) as bucket_time , resource_id, metrictype, {aggr_value}(value) as value, {aggr_score}(score)  as score from analytic_{prvdType}.{task}_{datatype}_{metricType}_{data_id} {where_clause} group by bucket_time, resource_id, metrictype order by bucket_time" \
                    .format(prvdType=prvdType, tplgType=tplgType, metricType=metricType, data_id=data_id, task=task,
                            where_clause=where_clause, timebucket=timebucket, aggr_value=aggr_value,
                            aggr_score=aggr_score, datatype=datatype)
            if task == 'precision':
                query = "select time_bucket('{timebucket}',time) as bucket_time , resource_id, metrictype, {aggr_score}(score)  as score from analytic_{prvdType}.{task}_{datatype}_{metricType}_{data_id} {where_clause} group by bucket_time, resource_id, metrictype order by bucket_time" \
                    .format(prvdType=prvdType, tplgType=tplgType, metricType=metricType, data_id=data_id, task=task,
                            where_clause=where_clause, timebucket=timebucket, aggr_score=aggr_score, datatype=datatype)

        else:
            query = "select * from analytic_{prvdType}.{task}_{datatype}_{metricType}_{data_id} {where_clause}" \
                .format(prvdType=prvdType, tplgType=tplgType, metricType=metricType, data_id=data_id, task=task,
                        where_clause=where_clause, datatype=datatype)

        self.data = pd.read_sql_query(query, self.engine)

        if len(self.data) == 0:
            self.anomaly_status_data = "[]"
            return self.anomaly_status_data
        if prvdType == 'openstack':
            if tplgType in ['domain', 'project', 'vm']:
                self.provider_env_dict[prvdId] = self.provider_env_dict['{prvdId}_logical'.format(prvdId=prvdId)]
            else:
                self.provider_env_dict[prvdId] = self.provider_env_dict['{prvdId}_physical'.format(prvdId=prvdId)]
        if tplgType == datatype:
            if sub:
                env_dict = self.provider_env_dict[prvdId][
                    ['{originType}_resource_id'.format(originType=originType),
                     '{originType}_name'.format(originType=originType),
                     '{tplgType}_resource_id'.format(tplgType=tplgType),
                     '{tplgType}_name'.format(tplgType=tplgType)]]

            else:
                env_dict = self.provider_env_dict[prvdId][
                    ['{tplgType}_resource_id'.format(tplgType=tplgType),
                     '{tplgType}_name'.format(tplgType=tplgType)]]

        else:
            if sub:
                env_dict = self.provider_env_dict[prvdId][
                    ['{originType}_resource_id'.format(originType=originType),
                     '{originType}_name'.format(originType=originType),
                     '{datatype}_resource_id'.format(datatype=datatype),
                     '{tplgType}_resource_id'.format(tplgType=tplgType),
                     '{tplgType}_name'.format(tplgType=tplgType)]]
            else:
                env_dict = self.provider_env_dict[prvdId][
                    ['{datatype}_resource_id'.format(datatype=datatype),
                     '{tplgType}_resource_id'.format(tplgType=tplgType),
                     '{tplgType}_name'.format(tplgType=tplgType)]]

        env_dict.drop_duplicates(inplace=True)

        self.anomaly_status_data = pd.merge(env_dict, self.data,
                                            left_on='{datatype}_resource_id'.format(datatype=datatype),
                                            right_on='resource_id')

        if timebucket:
            if sub:
                if measure:
                    self.anomaly_status_data = self.anomaly_status_data.groupby(
                        ['{originType}_resource_id'.format(originType=originType),
                         '{originType}_name'.format(originType=originType),
                         '{tplgType}_resource_id'.format(tplgType=tplgType),
                         '{tplgType}_name'.format(tplgType=tplgType),
                         'metrictype', 'bucket_time']).mean()
                else:
                    self.anomaly_status_data = self.anomaly_status_data.groupby(
                        ['{originType}_resource_id'.format(originType=originType),
                         '{originType}_name'.format(originType=originType),
                         '{tplgType}_resource_id'.format(tplgType=tplgType),
                         '{tplgType}_name'.format(tplgType=tplgType), 'bucket_time']).mean()
            else:
                if measure:
                    self.anomaly_status_data = self.anomaly_status_data.groupby(
                        ['{tplgType}_resource_id'.format(tplgType=tplgType),
                         '{tplgType}_name'.format(tplgType=tplgType), 'metrictype', 'bucket_time']).mean()
                else:
                    self.anomaly_status_data = self.anomaly_status_data.groupby(
                        ['{tplgType}_resource_id'.format(tplgType=tplgType),
                         '{tplgType}_name'.format(tplgType=tplgType), 'bucket_time']).mean()
        self.anomaly_status_data.reset_index(inplace=True)
        self.anomaly_status_data.rename(columns={'bucket_time': 'time'}, inplace=True)

        if resource_id:
            if sub:
                self.anomaly_status_data = self.anomaly_status_data[
                    self.anomaly_status_data['{originType}_resource_id'.format(originType=originType)] == resource_id]
            else:
                self.anomaly_status_data = self.anomaly_status_data[
                    self.anomaly_status_data['{tplgType}_resource_id'.format(tplgType=tplgType)] == resource_id]

        self.anomaly_status_data.rename(columns={'{tplgType}_resource_id'.format(tplgType=tplgType): 'resource_id',
                                                 '{tplgType}_name'.format(tplgType=tplgType): 'name'}, inplace=True)

        if sort:
            sort_list = sort.split(',')
            sort_value_list = []
            sort_parameter_list = []
            for items in sort_list:
                value, parameter = items.split(':')[0], items.split(':')[1]
                if parameter == 'asc':
                    parameter = True
                else:
                    parameter = False
                sort_value_list.append(value)
                sort_parameter_list.append(parameter)
            self.anomaly_status_data.sort_values(by=sort_value_list, ascending=sort_parameter_list, inplace=True)

        if column:
            columns = column.split(',')
            self.anomaly_status_data = self.anomaly_status_data[columns]

        self.anomaly_status_data = self.anomaly_status_data.to_json(orient='records')

        return self.anomaly_status_data

    def sunburst_chart_navigation(self, path, env_dict, hierarchy_number, provider_id, startdate, enddate):
        hierarchy_list=['virtualcenter','datacenter','cluster','host'] # 계층리스트
        hierarchy = hierarchy_list[hierarchy_number]
        selected_path = path
        selected_env= env_dict[env_dict['{hierarchy}_resource_id'.format(hierarchy=hierarchy)] == selected_path]  ###already  here 
        data_ids = self.get_data_ids()[provider_id]
        cpu_id = data_ids[data_ids['data_name'] == 'vmware_host_cpu']['data_id'].iloc[0]
        memory_id = data_ids[data_ids['data_name'] == 'vmware_host_memory']['data_id'].iloc[0]
        network_id = data_ids[data_ids['data_name'] == 'vmware_host_network']['data_id'].iloc[0]
        disk_id = data_ids[data_ids['data_name'] == 'vmware_host_disk']['data_id'].iloc[0]

        if hierarchy_number == 3 :
            data = dict()
            data['name'] = \
                selected_env[selected_env['{hierarchy}_resource_id'.format(hierarchy=hierarchy)] == selected_path] \
                    ['{hierarchy}_name'.format(hierarchy=hierarchy)].iloc[0]

            data['resourceId'] = \
                selected_env[selected_env['{hierarchy}_resource_id'.format(hierarchy=hierarchy)] == selected_path] \
                    ['{hierarchy}_resource_id'.format(hierarchy=hierarchy)].iloc[0]


            data['resource'] = hierarchy

            cpu_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_cpu_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=cpu_id, startdate=startdate, enddate=enddate), self.engine)
            memory_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_memory_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=memory_id, startdate=startdate, enddate=enddate), self.engine)
            network_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_network_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=network_id, startdate=startdate, enddate=enddate), self.engine)
            disk_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_disk_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=disk_id, startdate=startdate, enddate=enddate), self.engine)
            cpu_merged = pd.merge(selected_env, cpu_data, left_on='host_resource_id', right_on='resource_id')
            memory_merged = pd.merge(selected_env, memory_data, left_on='host_resource_id', right_on='resource_id')
            network_merged = pd.merge(selected_env, network_data, left_on='host_resource_id', right_on='resource_id')
            disk_merged = pd.merge(selected_env, disk_data, left_on='host_resource_id', right_on='resource_id')
            cpu_anomaly = cpu_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            memory_anomaly = \
            memory_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            network_anomaly = \
            network_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            disk_anomaly = disk_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            anomaly_score = np.mean([cpu_anomaly, memory_anomaly, network_anomaly, disk_anomaly])

            data['score'] = int(anomaly_score * 100)


            return data
        else:
            next_hierarchy = hierarchy_list[hierarchy_number + 1]
            data = dict()

            data['name'] = \
                selected_env[selected_env['{hierarchy}_resource_id'.format(hierarchy=hierarchy)] == selected_path] \
                ['{hierarchy}_name'.format(hierarchy=hierarchy)].iloc[0]

            data['resourceId'] = \
                selected_env[selected_env['{hierarchy}_resource_id'.format(hierarchy=hierarchy)] == selected_path]\
                ['{hierarchy}_resource_id'.format(hierarchy=hierarchy)].iloc[0]


            data['resource'] = hierarchy

            cpu_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_cpu_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id" .format(
                    data_id=cpu_id, startdate=startdate, enddate=enddate), self.engine)
            memory_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_memory_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=memory_id, startdate=startdate, enddate=enddate), self.engine)
            network_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_network_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=network_id, startdate=startdate, enddate=enddate), self.engine)
            disk_data = pd.read_sql_query(
                "select avg(score), resource_id from analytic_vmware.anomaly_host_disk_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id".format(
                    data_id=disk_id, startdate=startdate, enddate=enddate), self.engine)
            cpu_merged = pd.merge(selected_env,cpu_data, left_on='host_resource_id', right_on='resource_id')
            memory_merged = pd.merge(selected_env, memory_data, left_on='host_resource_id', right_on='resource_id')
            network_merged = pd.merge(selected_env, network_data, left_on='host_resource_id', right_on='resource_id')
            disk_merged = pd.merge(selected_env, disk_data, left_on='host_resource_id', right_on='resource_id')
            cpu_anomaly = cpu_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            memory_anomaly = memory_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            network_anomaly = network_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            disk_anomaly = disk_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=hierarchy)).mean()['avg'][
                selected_path]
            anomaly_score = np.mean([cpu_anomaly, memory_anomaly, network_anomaly, disk_anomaly])

            data['score'] = int(anomaly_score*100)

            data['children'] = [
                self.sunburst_chart_navigation(path, env_dict, hierarchy_number + 1, provider_id, startdate, enddate) \
                for path in np.unique(selected_env['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)])]

        self.sunburst_data = data

        return self.sunburst_data

    def hierarchy_anomaly(self, path_list, startdate, enddate):
        self.hierarchy_anomaly_data= dict()

        anomaly_status_dict=dict()

        ### status table
        status_table=pd.read_sql_query("select tp, nm, var_val from framework.tb_reg_variable",self.engine)
        status_table =status_table[status_table['tp']=='ANOMALY_STATUS']
        anomaly_status_dict['normal']= int(status_table[status_table['nm']=='SCORE_NORMAL']['var_val'].iloc[0])
        anomaly_status_dict['warning']= int(status_table[status_table['nm']=='SCORE_WARNING']['var_val'].iloc[0])
        anomaly_status_dict['critical'] = int(status_table[status_table['nm'] == 'SCORE_CRITICAL']['var_val'].iloc[0])

        def color_mapping(value):
            if value> anomaly_status_dict['critical']:
                return 'red'
            elif value> anomaly_status_dict['warning']:
                return 'yellow'
            else :
                return 'green'

        ###hierarchy setting
        hierarchy_list=['virtualcenter','datacenter','cluster','host']
        provider_id = path_list[1]
        selected_path = path_list[-1]
        current_hierarchy = hierarchy_list[len(path_list)-2]
        next_hierarchy =  hierarchy_list[len(path_list)-1]


        env_dict = self.provider_env_dict[provider_id]
        env_dict = env_dict.drop(labels=['vm_name','vm_resource_id'], axis=1)
        env_dict.drop_duplicates(inplace=True)
        env_dict = env_dict[env_dict['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)] == selected_path]

        data_ids= self.get_data_ids()[provider_id]

        cpu_id = data_ids[data_ids['data_name'] == 'vmware_host_cpu']['data_id'].iloc[0]
        memory_id = data_ids[data_ids['data_name'] == 'vmware_host_memory']['data_id'].iloc[0]
        network_id = data_ids[data_ids['data_name'] == 'vmware_host_network']['data_id'].iloc[0]
        disk_id = data_ids[data_ids['data_name'] == 'vmware_host_disk']['data_id'].iloc[0]

        cpu_data = pd.read_sql_query(
            "select avg(score), resource_id, time_bucket('1 hour',time) as bucket_time \
             from analytic_vmware.anomaly_host_cpu_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=cpu_id, startdate=startdate, enddate=enddate), self.engine)
        memory_data = pd.read_sql_query(
            "select avg(score), resource_id, time_bucket('1 hour',time) as bucket_time \
             from analytic_vmware.anomaly_host_memory_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=memory_id, startdate=startdate, enddate=enddate), self.engine)
        network_data = pd.read_sql_query(
            "select avg(score), resource_id, time_bucket('1 hour',time) as bucket_time \
             from analytic_vmware.anomaly_host_network_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=network_id, startdate=startdate, enddate=enddate), self.engine)
        disk_data = pd.read_sql_query(
            "select avg(score), resource_id, time_bucket('1 hour',time) as bucket_time \
            from analytic_vmware.anomaly_host_disk_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=disk_id, startdate=startdate, enddate=enddate), self.engine)

        cpu_merged = pd.merge(env_dict, cpu_data, left_on='host_resource_id', right_on='resource_id')
        memory_merged = pd.merge(env_dict, memory_data, left_on='host_resource_id', right_on='resource_id')
        network_merged = pd.merge(env_dict, network_data, left_on='host_resource_id', right_on='resource_id')
        disk_merged = pd.merge(env_dict, disk_data, left_on='host_resource_id', right_on='resource_id')

        # cpu_merged= cpu_merged[cpu_merged['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)] == selected_path]
        # memory_merged= memory_merged[memory_merged['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)] == selected_path]
        # network_merged= network_merged[network_merged['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)] == selected_path]
        # disk_merged= disk_merged[disk_merged['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)] == selected_path]


        cpu_merged['score']= cpu_merged['avg'] * 100
        memory_merged['score'] = memory_merged['avg'] * 100
        network_merged['score'] = network_merged['avg'] * 100
        disk_merged['score'] = disk_merged['avg'] * 100

        cpu_merged['sign'] = cpu_merged['score'].apply(color_mapping)
        memory_merged['sign'] = memory_merged['score'].apply(color_mapping)
        network_merged['sign'] = network_merged['score'].apply(color_mapping)
        disk_merged['sign'] = network_merged['score'].apply(color_mapping)

        ### anomaly scores for current path

        cpu_anomaly = cpu_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)).mean()['score'][
            selected_path]
        memory_anomaly = memory_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)).mean()['score'][
            selected_path]
        network_anomaly = network_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)).mean()['score'][
            selected_path]
        disk_anomaly = disk_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)).mean()['score'][
            selected_path]

        total_anomaly_score = np.mean([cpu_anomaly,memory_anomaly,network_anomaly,disk_anomaly])

        cpu_anomaly_count= cpu_merged[cpu_merged['sign']=='red'].groupby(
            by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy))\
        .count()['{hierarchy}_name'.format(hierarchy=current_hierarchy)][selected_path]

        memory_anomaly_count = memory_merged[memory_merged['sign'] == 'red'].groupby(
            by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)) \
            .count()['{hierarchy}_name'.format(hierarchy=current_hierarchy)][selected_path]

        network_anomaly_count = network_merged[network_merged['sign'] == 'red'].groupby(
            by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)) \
            .count()['{hierarchy}_name'.format(hierarchy=current_hierarchy)][selected_path]

        disk_anomaly_count = disk_merged[disk_merged['sign'] == 'red'].groupby(
            by='{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)) \
            .count()['{hierarchy}_name'.format(hierarchy=current_hierarchy)][selected_path]

        total_anomaly_counts= int(cpu_anomaly_count + memory_anomaly_count + network_anomaly_count + disk_anomaly_count)

        score_by_metric = [cpu_anomaly, memory_anomaly, network_anomaly, disk_anomaly]

        cpu_green = int(len(cpu_merged[cpu_merged['sign']=='green'])*100/len(cpu_merged))
        cpu_yellow = int(len(cpu_merged[cpu_merged['sign']=='yellow'])*100/len(cpu_merged))
        cpu_red = int(len(cpu_merged[cpu_merged['sign']=='red'])*100/len(cpu_merged))

        memory_green = int(len(memory_merged[memory_merged['sign']=='green'])*100/len(memory_merged))
        memory_yellow = int(len(memory_merged[memory_merged['sign']=='yellow'])*100/len(memory_merged))
        memory_red = int(len(memory_merged[memory_merged['sign']=='red'])*100/len(memory_merged))

        network_green = int(len(network_merged[network_merged['sign']=='green'])*100/len(network_merged))
        network_yellow = int(len(network_merged[network_merged['sign']=='yellow'])*100/len(network_merged))
        network_red = int(len(network_merged[network_merged['sign']=='red'])*100/len(network_merged))

        disk_green = int(len(disk_merged[disk_merged['sign']=='green'])*100/len(disk_merged))
        disk_yellow = int(len(disk_merged[disk_merged['sign']=='yellow'])*100/len(disk_merged))
        disk_red = int(len(disk_merged[disk_merged['sign']=='red'])*100/len(disk_merged))


        score_board = [total_anomaly_score, total_anomaly_counts]
        anomaly_distribution_by_metric = [
            {
                'label':'cpu',
                'data':[cpu_red, cpu_yellow, cpu_green]
            },
            {
                'label': 'memory',
                'data': [memory_red, memory_yellow, memory_green]
            },
            {
                'label': 'network',
                'data': [network_red, network_yellow, network_green]
            },
            {
                'label': 'disk',
                'data': [disk_red, disk_yellow, disk_green]
            }
        ]




        #### anomaly scores for next hierarchy
        anomaly_distribution = list()
        anomaly_score = list()


        for next_path in np.unique(env_dict['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)]): ###다음 path 들을 하나씩 가져온다
            next_path_name = env_dict[env_dict['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)]==next_path]['{hierarchy}_name'.format(hierarchy=next_hierarchy)].iloc[0]


            cpu_anomaly_next = cpu_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).mean()['score'][next_path]
            memory_anomaly_next = memory_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).mean()['score'][next_path]
            network_anomaly_next = network_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).mean()['score'][next_path]
            disk_anomaly_next = disk_merged.groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).mean()['score'][next_path]

            cpu_anomaly_next_count = cpu_merged[cpu_merged['sign']=='red'].groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).count()['score'][next_path]
            memory_anomaly_next_count = memory_merged[memory_merged['sign']=='red'].groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).count()['score'][next_path]
            network_anomaly_next_count = network_merged[network_merged['sign']=='red'].groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).count()['score'][next_path]
            disk_anomaly_next_count = disk_merged[disk_merged['sign']=='red'].groupby(by='{hierarchy}_resource_id'.format(hierarchy=next_hierarchy)).count()['score'][next_path]

            total_anomaly_counts_next= cpu_anomaly_next_count + memory_anomaly_next_count + network_anomaly_next_count + disk_anomaly_next_count

            cpu_anomaly_timestamp = cpu_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy),'bucket_time']).mean()['score'][next_path]
            memory_anomaly_timestamp = memory_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy),'bucket_time']).mean()['score'][next_path]
            network_anomaly_timestamp = network_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy),'bucket_time']).mean()['score'][next_path]
            disk_anomaly_timestamp = disk_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=next_hierarchy),'bucket_time']).mean()['score'][next_path]

            cpu_anomaly_timestamp = cpu_anomaly_timestamp.reset_index()
            cpu_anomaly_timestamp['bucket_time'] = (cpu_anomaly_timestamp['bucket_time'].astype(np.int64)/1000000000).astype(int)
            cpu_anomaly_timestamp = cpu_anomaly_timestamp.values.tolist()

            memory_anomaly_timestamp = memory_anomaly_timestamp.reset_index()
            memory_anomaly_timestamp['bucket_time'] = (memory_anomaly_timestamp['bucket_time'].astype(np.int64)/1000000000).astype(int)
            memory_anomaly_timestamp = memory_anomaly_timestamp.values.tolist()

            network_anomaly_timestamp = network_anomaly_timestamp.reset_index()
            network_anomaly_timestamp['bucket_time'] = (network_anomaly_timestamp['bucket_time'].astype(np.int64)/1000000000).astype(int)
            network_anomaly_timestamp = network_anomaly_timestamp.values.tolist()

            disk_anomaly_timestamp = disk_anomaly_timestamp.reset_index()
            disk_anomaly_timestamp['bucket_time'] = (disk_anomaly_timestamp['bucket_time'].astype(np.int64)/1000000000).astype(int)
            disk_anomaly_timestamp = disk_anomaly_timestamp.values.tolist()


            anomaly_distribution_data_cpu=dict()
            anomaly_distribution_data_cpu['risk'] = color_mapping(cpu_anomaly_next)
            anomaly_distribution_data_cpu['name'] = next_path_name
            anomaly_distribution_data_cpu['metric'] = 'cpu'
            anomaly_distribution_data_cpu['score'] = cpu_anomaly_next
            anomaly_distribution_data_cpu['graph'] = {
                'default' : anomaly_status_dict['critical'],
                'data' : cpu_anomaly_timestamp
            }

            anomaly_distribution_data_memory=dict()
            anomaly_distribution_data_memory['risk'] = color_mapping(memory_anomaly_next)
            anomaly_distribution_data_memory['name'] = next_path_name
            anomaly_distribution_data_memory['metric'] = 'memory'
            anomaly_distribution_data_memory['score'] = memory_anomaly_next
            anomaly_distribution_data_memory['graph'] = {
                'default' : anomaly_status_dict['critical'],
                'data' : memory_anomaly_timestamp
            }

            anomaly_distribution_data_network=dict()
            anomaly_distribution_data_network['risk'] = color_mapping(network_anomaly_next)
            anomaly_distribution_data_network['name'] = next_path_name
            anomaly_distribution_data_network['metric'] = 'network'
            anomaly_distribution_data_network['score'] = network_anomaly_next
            anomaly_distribution_data_network['graph'] = {
                'default' : anomaly_status_dict['critical'],
                'data' : network_anomaly_timestamp
            }

            anomaly_distribution_data_disk=dict()
            anomaly_distribution_data_disk['risk'] = color_mapping(disk_anomaly_next)
            anomaly_distribution_data_disk['name'] = next_path_name
            anomaly_distribution_data_disk['metric'] = 'disk'
            anomaly_distribution_data_disk['score'] = disk_anomaly_next
            anomaly_distribution_data_disk['graph'] = {
                'default' : anomaly_status_dict['critical'],
                'data' : disk_anomaly_timestamp
            }

            anomaly_distribution.extend([anomaly_distribution_data_cpu, anomaly_distribution_data_memory, anomaly_distribution_data_network, anomaly_distribution_data_disk])

            anomaly_score_data = {
                'name': next_path_name,
                'cpu': cpu_anomaly_next,
                'memory': memory_anomaly_next,
                'network' : network_anomaly_next,
                'disk': disk_anomaly_next,
                'anomaly_num': int(total_anomaly_counts_next),
                'metric_id': '/'.join(path_list)+'/'+next_path
            }
            anomaly_score.append(anomaly_score_data)

        self.hierarchy_anomaly_data['score_board'] = score_board
        self.hierarchy_anomaly_data['score_by_metric'] = score_by_metric
        self.hierarchy_anomaly_data['anomaly_distribution_by_metric'] = anomaly_distribution_by_metric
        self.hierarchy_anomaly_data['anomaly_distribution'] = anomaly_distribution
        self.hierarchy_anomaly_data['anomaly_score'] = anomaly_score
        self.hierarchy_anomaly_data = json.dumps(self.hierarchy_anomaly_data)
        return self.hierarchy_anomaly_data

    def hierarchy_anomaly_metric(self, path_list, startdate, enddate):
        self.hierarchy_anomaly_metric_data = dict()

        ###hierarchy setting
        hierarchy_list = ['virtualcenter', 'datacenter', 'cluster', 'host']
        provider_id = path_list[1]
        selected_path = path_list[-1]
        current_hierarchy = hierarchy_list[len(path_list) - 2]

        env_dict = self.provider_env_dict[provider_id]
        env_dict = env_dict.drop(labels=['vm_name', 'vm_resource_id'], axis=1)
        env_dict.drop_duplicates(inplace=True)
        env_dict = env_dict[env_dict['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy)] == selected_path]

        data_ids = self.get_data_ids()[provider_id]

        cpu_id = data_ids[data_ids['data_name'] == 'vmware_host_cpu']['data_id'].iloc[0]
        memory_id = data_ids[data_ids['data_name'] == 'vmware_host_memory']['data_id'].iloc[0]
        network_id = data_ids[data_ids['data_name'] == 'vmware_host_network']['data_id'].iloc[0]
        disk_id = data_ids[data_ids['data_name'] == 'vmware_host_disk']['data_id'].iloc[0]

        cpu_data = pd.read_sql_query(
            "select avg(usage_avg), resource_id, time_bucket('1 hour',time) as bucket_time \
             from vmware.tb_metric_host_cpu_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=cpu_id, startdate=startdate, enddate=enddate), self.engine)
        memory_data = pd.read_sql_query(
            "select avg(usage_avg), resource_id, time_bucket('1 hour',time) as bucket_time \
             from vmware.tb_metric_host_memory_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=memory_id, startdate=startdate, enddate=enddate), self.engine)
        network_data = pd.read_sql_query(
            "select avg(usage_avg_kbps), resource_id, time_bucket('1 hour',time) as bucket_time \
             from vmware.tb_metric_host_network_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=network_id, startdate=startdate, enddate=enddate), self.engine)
        disk_data = pd.read_sql_query(
            "select avg(usage_avg), resource_id, time_bucket('1 hour',time) as bucket_time \
            from vmware.tb_metric_host_disk_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=disk_id, startdate=startdate, enddate=enddate), self.engine)

        cpu_merged = pd.merge(env_dict, cpu_data, left_on='host_resource_id', right_on='resource_id')
        memory_merged = pd.merge(env_dict, memory_data, left_on='host_resource_id', right_on='resource_id')
        network_merged = pd.merge(env_dict, network_data, left_on='host_resource_id', right_on='resource_id')
        disk_merged = pd.merge(env_dict, disk_data, left_on='host_resource_id', right_on='resource_id')

        ### import anomaly data
        cpu_anomaly_data = pd.read_sql_query(
            "select avg(score)*100 as anomaly_score, resource_id, time_bucket('1 hour',time) as bucket_time \
             from analytic_vmware.anomaly_host_cpu_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=cpu_id, startdate=startdate, enddate=enddate), self.engine)
        memory_anomaly_data = pd.read_sql_query(
            "select avg(score)*100 as anomaly_score, resource_id, time_bucket('1 hour',time) as bucket_time \
             from analytic_vmware.anomaly_host_memory_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=memory_id, startdate=startdate, enddate=enddate), self.engine)
        network_anomaly_data = pd.read_sql_query(
            "select avg(score)*100 as anomaly_score, resource_id, time_bucket('1 hour',time) as bucket_time \
             from analytic_vmware.anomaly_host_network_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=network_id, startdate=startdate, enddate=enddate), self.engine)
        disk_anomaly_data = pd.read_sql_query(
            "select avg(score)*100 as anomaly_score, resource_id, time_bucket('1 hour',time) as bucket_time \
            from analytic_vmware.anomaly_host_disk_{data_id} where time > '{startdate}' and time< '{enddate}' group by resource_id, bucket_time order by bucket_time".format(
                data_id=disk_id, startdate=startdate, enddate=enddate), self.engine)

        # cpu_anomaly_merged = pd.merge(env_dict, cpu_anomaly_data, left_on='host_resource_id', right_on='resource_id')
        # memory_anomaly_merged = pd.merge(env_dict, memory_anomaly_data, left_on='host_resource_id', right_on='resource_id')
        # network_anomaly_merged = pd.merge(env_dict, network_anomaly_data, left_on='host_resource_id', right_on='resource_id')
        # disk_anomaly_merged = pd.merge(env_dict, disk_anomaly_data, left_on='host_resource_id', right_on='resource_id')

        cpu_merged = pd.merge(cpu_merged,cpu_anomaly_data, left_on= ['resource_id','bucket_time'],right_on= ['resource_id','bucket_time'])
        memory_merged = pd.merge(memory_merged,memory_anomaly_data, left_on= ['resource_id','bucket_time'],right_on= ['resource_id','bucket_time'])
        network_merged = pd.merge(network_merged,network_anomaly_data, left_on= ['resource_id','bucket_time'],right_on= ['resource_id','bucket_time'])
        disk_merged = pd.merge(disk_merged,disk_anomaly_data, left_on= ['resource_id','bucket_time'],right_on= ['resource_id','bucket_time'])

        cpu_merged = \
        cpu_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy),'bucket_time']).mean()
        cpu_merged.reset_index(inplace=True)
        memory_merged = \
        memory_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy),'bucket_time']).mean()
        memory_merged.reset_index(inplace=True)
        network_merged = \
        network_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy),'bucket_time']).mean()
        network_merged.reset_index(inplace=True)
        disk_merged = \
        disk_merged.groupby(by=['{hierarchy}_resource_id'.format(hierarchy=current_hierarchy),'bucket_time']).mean()
        disk_merged.reset_index(inplace=True)


        host_url= "http://172.168.0.29:8089" ### event task api
        event_path ="/v1/log/vmware/{provider_id}/event".format(provider_id=provider_id)
        task_path ="/v1/log/vmware/{provider_id}/task".format(provider_id=provider_id)

        params= {'from':startdate, 'to':enddate}
        event_response= requests.get(url=host_url+ event_path, params=params)
        task_response= requests.get(url=host_url+ task_path, params=params)

        if event_response.raise_for_status():
            print('event response error')
        if task_response.raise_for_status():
            print('task response error')
        event_data = event_response.json()
        task_data = task_response.json()
        event_data = pd.DataFrame(event_data)
        task_data = pd.DataFrame(task_data)
        event_data['createdTime'] = pd.to_datetime(event_data['createdTime'], unit='ms')
        event_data.sort_values(by=['createdTime'], inplace=True)
        task_data['createdTime'] = pd.to_datetime(task_data['createdTime'], unit='ms')
        task_data.sort_values(by=['createdTime'], inplace=True)
        event_data['type']='event'
        task_data['type']='task'
        task_data['message']= 'entity: '+task_data['entityName']+', task: '+ task_data['descriptionId']
        event_data= event_data[['message','createdTime','type']]
        task_data = task_data[['message','createdTime','type']]
        log_data=pd.concat([event_data,task_data])
        log_data.sort_values(by=['createdTime'], inplace=True)
        log_data.index = log_data['createdTime']
        log_data['zip'] = list(zip((log_data['createdTime'].astype(np.int64)/1000000000).astype(int), log_data['message']))
        array=[]
        ### anything added to log dict goes here
        for row in log_data['zip']:
            data={'timestamp':row[0], 'message': row[1]}
            array.append(data)
        log_data['dict']=array


        log_data = log_data.groupby(log_data.index.to_period('H'))['dict'].apply(list)
        log_data = pd.DataFrame(log_data)
        log_data['bucket_time']= log_data.index
        log_data['bucket_time']=log_data['bucket_time'].astype(str)
        log_data['bucket_time'] = pd.to_datetime(log_data['bucket_time'])

        cpu_logs_merged=pd.merge(cpu_merged, log_data, on= 'bucket_time',how='left')
        memory_logs_merged=pd.merge(memory_merged, log_data, on= 'bucket_time',how='left')
        network_logs_merged=pd.merge(network_merged, log_data, on= 'bucket_time',how='left')
        disk_logs_merged=pd.merge(disk_merged, log_data, on= 'bucket_time',how='left')

        cpu_logs_merged['bucket_time'] = (
                    cpu_logs_merged['bucket_time'].astype(np.int64) / 1000000000).astype(int)

        memory_logs_merged['bucket_time'] = (
                    memory_logs_merged['bucket_time'].astype(np.int64) / 1000000000).astype(int)

        network_logs_merged['bucket_time'] = (
                    network_logs_merged['bucket_time'].astype(np.int64) / 1000000000).astype(int)

        disk_logs_merged['bucket_time'] = (
                    disk_logs_merged['bucket_time'].astype(np.int64) / 1000000000).astype(int)

        cpu_data = cpu_logs_merged[['bucket_time','avg','anomaly_score','dict']].values.tolist()
        memory_data = memory_logs_merged[['bucket_time','avg','anomaly_score','dict']].values.tolist()
        network_data = network_logs_merged[['bucket_time','avg','anomaly_score','dict']].values.tolist()
        disk_data = disk_logs_merged[['bucket_time','avg','anomaly_score','dict']].values.tolist()
        self.hierarchy_anomaly_metric_data['data']=list()
        self.hierarchy_anomaly_metric_data['data'].append({'type':'cpu', 'data':cpu_data})
        self.hierarchy_anomaly_metric_data['data'].append({'type':'memory', 'data':memory_data})
        self.hierarchy_anomaly_metric_data['data'].append({'type':'disk', 'data':disk_data})
        self.hierarchy_anomaly_metric_data['data'].append({'type':'network', 'data':network_data})
        self.hierarchy_anomaly_metric_data = json.dumps(self.hierarchy_anomaly_metric_data, ensure_ascii=False)

        return self.hierarchy_anomaly_metric_data


# self.hierarchy_anomaly_data.keys()
#  json.dumps(self.hierarchy_anomaly_data['score_by_metric'])


db_settings = {
    'user': 'admin',
    'password': 'admin!123',
    'host': '172.168.0.29',
    'port': 5432,
    'db': 'harmony'
}
#
# db = dbUtils(db_settings)
# db.provider_env_dict['openstack20200709080543_physical']
# db.provider_env_dict['openstack20200709080543_logical']
# db.provider_env_dict['vmware20200529235050']
# db.hierarchy_anomaly(path_list=['','vmware20200529235050'],startdate='2020-09-10', enddate='2020-10-10')
# db.hierarchy_anomaly_metric(path_list=['','vmware20200529235050'],startdate='2020-09-10', enddate='2020-10-10')
# host ='http://172.168.0.29:9998'
# path ='/anomaly/by-hierarchy/nav'
# url = host+path
# params= { 'provider_id':'vmware20200529235050', 'from':'2020-09-10', 'to':'2020-10-10' }
# r=requests.get(url=url,params=params)
# r

# db.provider_env_dict['vmware20200529235050']
# db.provider_env_dict['openstack20200709080543_physical']
# db.provider_env_dict['openstack20200709080543_logical']
# db.sunburst_chart_navigation('vmware20200529235050',db.provider_env_dict['vmware20200529235050'],0,'vmware20200529235050', startdate= '2020-09-21', enddate= '2020-10-10')
# db.get_data_ids()
# db.sunburst_chart_navigation('vmware','')

# db.provider_env_dict['vmware20200529235050']
# pd.read_sql_query("select time_bucket('1 hour',time) as bucket_time , resource_id, metrictype, sum(score)  as score from analytic_vmware.precision_host_network_ef3fd23f53dc404fb1277c4c04246a7c  where  model = 'xgboost' group by bucket_time, resource_id, metrictype order by bucket_time",db.engine)
# db.get_anomaly_status()
# pd.read_sql_query("select time_bucket('1 hour',time) as bucket_time , resource_id, metrictype, avg(value) as value, avg(score)  as score from analytic_openstack.anomaly_host_network_e733d4a0e8524e97a4fbe4ef580399ae  where model = 'arima' and time > '2020-09-12 01:32:32' and time < '2020-09-13 06:54:32' group by bucket_time, resource_id, metrictype order by bucket_time",db.engine)
# db.get_providers()
# # db.providers
# # db.get_data_ids()
# db.provider_env_dict
# db.get_anomaly_status()
# host_cpu_data=pd.read_sql_query("select time, resource_id as host_resource_id, usage_avg from vmware.tb_metric_host_cpu_e17d36e1ba954e7c97a5e9d46c259383 where time<now() - interval '7 hour' ",db.engine)
# pd.merge(vmware_env,host_cpu_data)
# index = pd.MultiIndex.from_tuples([('bird', 'falcon'),
#                                    ('bird', 'parrot'),
#                                    ('mammal', 'lion'),
#                                    ('mammal', 'monkey')],
#                                   names=['class', 'name'])
# columns = pd.MultiIndex.from_tuples([('speed', 'max'),
#                                      ('species', 'type')])
# df = pd.DataFrame([(389.0, 'fly'),
#                    ( 24.0, 'fly'),
#                    ( 80.5, 'run'),
#                    (np.nan, 'jump')],
#                   index=index,
#                   columns=columns)
# df.reset_index()
