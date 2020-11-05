from flask import Flask, request
from flask_restful import Resource, Api
from flask_restful import reqparse

#from app import app
from db_utils import dbUtils
app = Flask(__name__)
api = Api(app)

db_settings = {
    'user': 'admin',
    'password': 'admin!123',
    'host': '172.168.0.29',
    'port': 5432,
    'db': 'harmony'
}


class CreateUser():

    @app.route('/core/score/<prvdType>/<prvdId>/<tplgType>/<metricType>/<task>', methods=['GET'])
    def ano_select_data(prvdType, prvdId ,tplgType, metricType, task):
        resource_id = request.args.get('resource-id')
        model = request.args.get('model')
        startdate = request.args.get('from')
        enddate = request.args.get('to')
        sort = request.args.get('sort')
        aggr = request.args.get('aggregation')
        timebucket = request.args.get('time-bucket')
        column = request.args.get('column')
        measure= request.args.get('measure')
        try:
            db = dbUtils(db_settings)
            db.get_anomaly_status(prvdType=prvdType, prvdId=prvdId, tplgType=tplgType, metricType=metricType, task=task,
                                  resource_id=resource_id, model=model, startdate=startdate, enddate=enddate, sort=sort,
                                  aggr=aggr, timebucket=timebucket, column=column, measure=measure)
            return db.anomaly_status_data
        except:
            return "[]"


    @app.route('/anomaly/by-hierarchy/nav')
    def sunburst_chart_navigation():
        provider = request.args.get('provider')
        path = request.args.get('provider-id')
        startdate = request.args.get('from')
        enddate = request.args.get('to')
        try:
            if 'vmware' in provider:
                db = dbUtils(db_settings)
                path_list = path.split('/')
                hierarchy_number= len(path_list)-1
                provider_id = path_list[0]
                path = path_list[-1] ###해당경로의 미자막 패스
                env_dict= db.provider_env_dict[provider_id]
                db.sunburst_chart_navigation(path, env_dict,hierarchy_number, provider_id, startdate, enddate)
                return db.sunburst_data

            if 'openstack' in provider:
                db = dbUtils(db_settings)
                path_list = path.split('/')
                hierarchy_number= len(path_list)-1
                provider_id = path_list[0]
                path = path_list[-1] ###해당경로의 미자막 패스
                env_dict= db.provider_env_dict[provider_id+'_physical']
                db.sunburst_chart_navigation_openstack(path, env_dict,hierarchy_number, provider_id, startdate, enddate)
                return db.sunburst_data

        except:
            return "[]"

    @app.route('/anomaly/by-hierarchy')
    def hierarchy_anomaly():
        provider = request.args.get('provider')
        path = request.args.get('path')
        startdate = request.args.get('from')
        enddate = request.args.get('to')
        try:
            if 'vmware' in provider:
                db = dbUtils(db_settings)
                path_list= path.split('/')
                db.hierarchy_anomaly(path_list, startdate, enddate)
                return db.hierarchy_anomaly_data

            if 'openstack' in provider:
                db = dbUtils(db_settings)
                path_list = path.split('/')
                db.hierarchy_anomaly_openstack(path_list, startdate, enddate)
                return db.hierarchy_anomaly_data

        except:
            return "[]"

    @app.route('/anomaly/by-hierarchy/metric')
    def hierarchy_anomaly_metric():
        provider = request.args.get('provider')
        path = request.args.get('path')
        startdate = request.args.get('from')
        enddate= request.args.get('to')
        try:
            if 'vmware' in provider:
                db = dbUtils(db_settings)
                path_list= path.split('/')
                db.hierarchy_anomaly_metric(path_list, startdate, enddate)
                return db.hierarchy_anomaly_metric_data

            if 'openstack' in provider:
                db = dbUtils(db_settings)
                path_list = path.split('/')
                db.hierarchy_anomaly_metric_openstack(path_list, startdate, enddate)
                return db.hierarchy_anomaly_metric_data
        except:
            return "[]"

    @app.route('/anomaly/by-hierarchy/host')
    def hierarchy_anomaly_host():
        provider = request.args.get('provider')
        path = request.args.get('path')
        startdate = request.args.get('from')
        enddate = request.args.get('to')
        try:
            if 'vmware' in provider:
                db = dbUtils(db_settings)
                path_list = path.split('/')
                db.hierarchy_anomaly_host(path_list, startdate, enddate)
                return db.hierarchy_anomaly_host_data
            if 'openstack' in provider:
                db = dbUtils(db_settings)
                path_list = path.split('/')
                db.hierarchy_anomaly_host_openstack(path_list, startdate, enddate)
                return db.hierarchy_anomaly_host_data
        except:
            return "[]"


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True,port=5000)

