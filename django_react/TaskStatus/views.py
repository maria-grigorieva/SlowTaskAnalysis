from django.shortcuts import render
import slow_task_processing
import pandas as pd
import cx_Oracle
import configparser
import argparse
from collections import OrderedDict
import json
import numpy as np
from imblearn.under_sampling import TomekLinks, ClusterCentroids
from kmodes.kprototypes import KPrototypes
from kmodes.kmodes import KModes
from kmodes.util.dissim import matching_dissim
from django.http import JsonResponse

from rest_framework import generics
from rest_framework import views
from rest_framework.response import Response
from rest_framework.decorators import api_view, renderer_classes
from rest_framework.renderers import JSONRenderer


from .serializers import AllFailedJobs

config = configparser.ConfigParser()
config.read('../config.ini')
CONN_INFO = {
    'host': config['ORACLE']['host'],
    'port': config['ORACLE']['port'],
    'user': config['ORACLE']['user'],
    'psw': config['ORACLE']['pwd'],
    'service': config['ORACLE']['service'],
}
CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO)

# @api_view(['GET'])
# @renderer_classes([JSONRenderer])
# def index(request, jeditaskid):
#     config = configparser.ConfigParser()
#     config.read('../config.ini')
#     CONN_INFO = {
#         'host': config['ORACLE']['host'],
#         'port': config['ORACLE']['port'],
#         'user': config['ORACLE']['user'],
#         'psw': config['ORACLE']['pwd'],
#         'service': config['ORACLE']['service'],
#     }
#     CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO)
#     connection = get_db_connection(CONN_STR)
#     print(request)
#     statuses = statuses_duration(jobs_with_statuses(connection, jeditaskid)).to_dict('records')
#     results = AllFailedJobs(statuses, many=True).data
#     return Response(results)


def task_index(requset):
    """
    Renders a page asking to enter an ID
    """
    return render(requset, 'index-task.html')


def task_index_preselected(requset, jeditaskid):
    """
    Renders a page with a pre-entered ID, immediately starting the search
    """
    return render(requset, 'index-task-preselected.html', {'jeditaskid': jeditaskid})


def duration_index(requset):
    """
    Renders a page asking to enter the dates for tasks analysis
    """
    return render(requset, 'index-duration.html')

def request_db(request):
    """
    AJAX backend to process the ID and give back the result
    """
    jeditaskid = request.GET.get('jeditaskid', None)
    return JsonResponse(calculate_data(jeditaskid))


def calculate_data(jeditaskid):
    """
    Check the input, make a request to Oracle and process the result
    """
    result = {}

    try:
        jeditaskid = int(jeditaskid)
    except ValueError:
        result = {'error': 'id should be an integer'}
        print('Error! Function "calculate_data" got not an integer input.')

    if (not isinstance(jeditaskid, int)):
        result = {'error': 'id should be an integer'}
        print('Error! Function "calculate_data" got not an integer input.')
    else:
        try:
            connection = get_db_connection(CONN_STR)
            statuses = statuses_duration(jobs_with_statuses(connection, jeditaskid))

            min_time, max_time = task_time_range(connection, jeditaskid)
            task_sites = get_task_sites(connection, jeditaskid)
            efficiency = sites_efficiency(connection, min_time, max_time, task_sites)

            print("Initial size: {}".format(statuses.shape))

            statuses = pd.merge(statuses, efficiency,
                                how='left', on=['COMPUTINGSITE', 'DATE_TRUNCATED'])

            print("After merge: {}".format(statuses.shape))

            scouts = statuses[statuses['IS_SCOUT'] == 'SCOUT']
            not_scouts = statuses[statuses['IS_SCOUT'] == 'NOT_SCOUT']
            finished = not_scouts[not_scouts['FINAL_STATUS'] == 'finished']
            # closed = not_scouts[not_scouts['FINAL_STATUS'] == 'closed']
            failed = not_scouts[not_scouts['FINAL_STATUS'] == 'failed']

            # sampled_statuses = pd.merge(sampled_statuses, efficiency,
            #                         how='left', on=['COMPUTINGSITE', 'DATE_TRUNCATED'])
            pre_failed_statuses = pre_failed(failed)
            # sequences = sequences_of_statuses(sampled_statuses)
            # statuses = sampled_statuses.astype(str)
            pre_failed_statuses = pre_failed_statuses.astype(str)
            # _tmpl_sequences = {}
            # for seq in sequences:
            #     sequences[seq] = sequences[seq].astype(str)
            #     _tmpl_sequences[seq] = sequences[seq].to_dict('split')

            result = {'pre_failed': pre_failed_statuses.to_dict('split'),
                      # # 'sequences': _tmpl_sequences,
                      'finished': finished.astype(str).to_dict('split'),
                      'failed': failed.astype(str).to_dict('split'),
                      # 'closed': closed.astype(str).to_dict('split'),
                      'scouts': scouts.astype(str).to_dict('split'),
                      'jeditaskid': jeditaskid}
        except Exception as e:
            result = {'error': 'There was an error. ' + str(e)}
            print('Error! Function "calculate_data" got an error: ' + str(e))
    return result


def index(request, jeditaskid):
    result = calculate_data(jeditaskid)
    return render(request, 'index-duration.html', result)
#.sort_values(by=['DURATION'], ascending=False).head(10000)
#
# def slowest_tasks(request):
#     if request.method == 'POST':
#         # TODO:
#         # form
#         return render(request, 'list_of_tasks.html')
#     else:
#         connection = get_db_connection(CONN_STR)
#         get_slowest_user_tasks(connection, request['GET']['start_time'], request['GET']['end_time'])
#         return render(request, 'list_of_tasks.html')


def kmodes_samping(df):
    km = KModes(n_clusters=100, init='Huang', n_init=5, verbose=1, n_jobs=-1)
    #model = KPrototypes(n_clusters=100, init='Huang', n_init=5, verbose=1, n_jobs=1)
    data = df[['PANDAID', 'JOBSTATUS', 'COMPUTINGSITE',
               'FINAL_STATUS', 'IS_SCOUT','DURATION']].values
    clusters = km.fit_predict(data)
    centers = [row[0] for row in km.cluster_centroids_]
    return df[df['PANDAID'].isin(centers)]

def get_db_connection(conn_str):
    try:
        conn = cx_Oracle.connect(conn_str)
        print('Connected to Oracle!')
        return conn
    except Exception as e:
        print('Connection to Oracle failed')


def jobs_with_statuses(connection,
                               taskid):
    """
    Get failed jobs with all interim statuses
    :param conn_str:
    :param taskid:
    :return:
    """
    try:
        cursor = connection.cursor()
        # query = \
        #     "SELECT s.pandaid, s.modiftime_extended, " \
        #     "trunc(s.modiftime_extended, 'DDD') as date_truncated, "\
        #     "s.jobstatus, " \
        #     "s.computingsite, " \
        #     "s.modificationhost, " \
        #     "j.attemptnr, " \
        #     "j.jobstatus as final_status, " \
        #     "j.exeerrorcode, j.exeerrordiag, j.superrorcode, j.superrordiag,"\
        #     "j.ddmerrorcode, j.ddmerrordiag, j.taskbuffererrorcode, j.taskbuffererrordiag,"\
        #     "j.piloterrorcode, j.piloterrordiag, "\
        #     "(CASE WHEN INSTR(LOWER(specialhandling), 'sj') > 0 "\
        #     "THEN 'SCOUT' "\
        #     "ELSE 'NOT_SCOUT' END) as is_scout "\
        #     "FROM ATLAS_PANDA.JOBS_STATUSLOG s " \
        #     "INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED j ON(s.pandaid = j.pandaid) " \
        #     "WHERE jeditaskid = {} AND " \
        #     "j.jobstatus = 'failed'".format(taskid)
        query = \
            "SELECT s.pandaid, s.modiftime_extended, " \
            "trunc(s.modiftime_extended, 'DDD') as date_truncated, "\
            "s.jobstatus, " \
            "s.computingsite, " \
            "s.modificationhost, " \
            "j.attemptnr, " \
            "j.jobstatus as final_status, " \
            "j.exeerrorcode, j.exeerrordiag, j.superrorcode, j.superrordiag,"\
            "j.ddmerrorcode, j.ddmerrordiag, j.taskbuffererrorcode, j.taskbuffererrordiag,"\
            "j.piloterrorcode, j.piloterrordiag, "\
            "(CASE WHEN INSTR(LOWER(specialhandling), 'sj') > 0 "\
            "THEN 'SCOUT' "\
            "ELSE 'NOT_SCOUT' END) as is_scout "\
            "FROM ATLAS_PANDA.JOBS_STATUSLOG s " \
            "INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED j ON(s.pandaid = j.pandaid) " \
            "WHERE jeditaskid = {}".format(taskid)
        print(query)
        return pd.DataFrame([row for row in cursor.execute(query)],
                            columns=['PANDAID', 'MODIFTIME_EXTENDED', 'DATE_TRUNCATED',
                                     'JOBSTATUS',
                                     'COMPUTINGSITE', 'MODIFICATIONHOST',
                                     'ATTEMPTNR', 'FINAL_STATUS',
                                     'EXEERRORCODE',
                                     'EXEERRORDIAG',
                                     'SUPERRORCODE',
                                     'SUPERRORDIAG',
                                     'DDMERRORCODE',
                                     'DDMERRORDIAG',
                                     'TASKBUFFERERRORCODE',
                                     'TASKBUFFERERRORDIAG',
                                     'PILOTERRORCODE',
                                     'PILOTERRORDIAG',
                                     'IS_SCOUT'])
    except Exception as ex:
        print('exception occurred: {0}'.format(ex))
        return None


def task_time_range(connection, taskid):
    cursor = connection.cursor()
    query = \
        "SELECT MIN (starttime), MAX (endtime) " \
        "FROM ATLAS_PANDAARCH.JOBSARCHIVED WHERE jeditaskid = {}".format(taskid)
    cursor.execute(query)
    row = cursor.fetchone()
    return row[0], row[1]


def get_task_sites(connection, taskid):
    cursor = connection.cursor()
    query = "SELECT DISTINCT(computingsite) "\
            "FROM ATLAS_PANDAARCH.JOBSARCHIVED " \
            "WHERE jeditaskid = {} " \
            "GROUP BY computingsite".format(taskid)
    sites_list = [row[0] for row in cursor.execute(query)]
    return ','.join("'{0}'".format(w) for w in sites_list)


def sites_efficiency(connection, min_time, max_time, sites):
    print('Calculating computing site efficiency:...')
    cursor = connection.cursor()
    query = \
        "SELECT computingsite, "\
        "trunc(statechangetime,'DDD'), "\
        "jobstatus, "\
        "count(*) "\
        "FROM ATLAS_PANDAARCH.JOBSARCHIVED "\
        "WHERE statechangetime >= to_date('{}', 'yyyy-MM-dd HH24:mi:ss') "\
        "AND statechangetime < to_date('{}', 'yyyy-MM-dd HH24:mi:ss') "\
        "AND jobstatus IN ('finished','failed') "\
        "AND computingsite IN ({}) " \
        "GROUP BY computingsite, "\
        "trunc(statechangetime,'DDD'), "\
        "jobstatus".format(min_time, max_time, sites)
    result = pd.DataFrame([row for row in cursor.execute(query)],
                        columns=['COMPUTINGSITE',
                                 'STATECHANGETIME',
                                 'SITE_JOBSTATUS',
                                 'SITE_STATUS_COUNT'])

    print('Processing efficiency')

    frames = []
    groups = result.groupby('COMPUTINGSITE')
    for k,v in groups:
        finished = pd.DataFrame(v[v['SITE_JOBSTATUS'] == 'finished'][['STATECHANGETIME',
                                                                      'SITE_STATUS_COUNT']])
        finished.rename(columns={"SITE_STATUS_COUNT": "JOBS_FINISHED"}, inplace=True)
        failed = pd.DataFrame(v[v['SITE_JOBSTATUS'] == 'failed'][['STATECHANGETIME',
                                                     'SITE_STATUS_COUNT']])
        failed.rename(columns={"SITE_STATUS_COUNT": "JOBS_FAILED"}, inplace=True)
        result = finished.merge(failed, on='STATECHANGETIME', how='inner')
        result.rename(columns={"STATECHANGETIME": "DATE_TRUNCATED"}, inplace=True)
        result['SITE_EFFICIENCY'] = result['JOBS_FINISHED'] / (result['JOBS_FAILED'] +
                                                               result['JOBS_FINISHED'])
        result['COMPUTINGSITE'] = k
        frames.append(result)
    return pd.concat(frames)


def statuses_duration(df):
    """
    Calculates duration of each job status by modificationtime delta
    """
    frames = []
    for k, v in df.groupby(['PANDAID']):
        v.sort_values(by=['MODIFTIME_EXTENDED'], inplace=True)
        v['START_TS'] = pd.to_datetime(v['MODIFTIME_EXTENDED'])
        v['END_TS'] = v['START_TS'].shift(-1).fillna(v['START_TS'])
        v['DURATION'] = (v['END_TS'] - v['START_TS']).dt.total_seconds() / 60. / 60.
        status_sequence = v['JOBSTATUS'].values.tolist()
        sequence_set = list(OrderedDict.fromkeys(status_sequence))
        v['STATUS_LEVEL'] = v.apply(lambda x: get_seq_level(x['JOBSTATUS'], sequence_set), axis=1)
        v['SEQUENCE'] = str(sequence_set)
        errorcodes = ['EXE','SUP','PILOT','DDM','TASKBUFFER']
        codenames = []
        for column in errorcodes:
            for code in np.unique(v[f"{column}ERRORCODE"].values):
                if code != 0:
                    codenames.append(f"{column}:{code}")
        v['ERROR_CODE'] = ','.join(codenames)
        frames.append(v)
    return pd.concat(frames).sort_values(by=['PANDAID',
                                             'MODIFTIME_EXTENDED',
                                             'JOBSTATUS'
                                             ])


def pre_failed(df):
    """
    Return only jobs statuses previous to failed
    :param df:
    :return:
    """
    frames = []
    for k, v in df.groupby('PANDAID'):
        v.sort_values(by=['MODIFTIME_EXTENDED'], ascending=True, inplace=True)
        #v.drop(v[v['JOBSTATUS'] == 'failed'].index, inplace=True)
        v.rename(columns={"JOBSTATUS": "PRE-FAILED"}, inplace=True)
        #v['FINAL_STATUS'] = 'failed'
        frames.append(v.iloc[[-2]])
    return pd.concat(frames)


def get_slowest_job_statuses(df, limit=400):
    return df.sort_values(by=['DURATION'], ascending=False).head(limit)


def convert_to_csv(df, filename):
    df.to_csv(filename)


def get_slowest_user_tasks(connection, start_time, end_time):
    """
    2020-04-01 10:40:00
    :param start_time:
    :param end_time:
    :return:
    """
    cursor = connection.cursor()
    query = \
        "SELECT taskid," \
        "(TRUNC(endtime,'HH24') - " \
        "TRUNC(start_time,'HH24')) as duration " \
        "FROM ATLAS_DEFT.T_PRODUCTION_TASK " \
        "WHERE start_time >= to_date('{}','YYYY-MM-DD HH24:MI:SS') " \
        "AND start_time < to_date('{}', 'YYYY-MM-DD HH24:MI:SS') " \
        "AND status in ('done','finished') " \
        "AND prodsourcelabel = 'user' " \
        "AND start_time IS NOT NULL " \
        "AND endtime IS NOT NULL " \
        "ORDER BY duration desc".format(start_time, end_time)
    return pd.DataFrame([row for row in cursor.execute(query)],
                        columns=['taskid', 'duration']).sort_values(by='duration',
                                                                    ascending=False).head(100)


def get_seq_level(status, sequence_set):
    for i, x in enumerate(sequence_set):
        if status == x:
            return i


def sequences_of_statuses(df):
    sequences = {}
    groups = df.groupby('SEQUENCE')
    for k, v in groups:
        sequences[k] = v
    return sequences