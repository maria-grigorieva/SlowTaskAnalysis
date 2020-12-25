import pandas as pd
import cx_Oracle
import configparser
from collections import OrderedDict
import traceback
import json
import sys
import os
import numpy as np
from time import gmtime, strftime
from elasticsearch import Elasticsearch
from kmodes.kmodes import KModes
from django.http import JsonResponse
from TaskStatus.database.site import get_site_info


# Parse Oracle connection info from config.ini
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

# ElasticSearch connection
ES_CONN_INFO = {
    'host': config['ELASTICSEARCH']['host'],
    'port': config['ELASTICSEARCH']['port']
}
ES_CONN_STR = '{host}:{port}'.format(**ES_CONN_INFO)
if 'user' in config['ELASTICSEARCH']:
    ES_CONN_STR = '{user}:{psw}@{srv}'.format(user=config['ELASTICSEARCH']['user'],
                                              psw=config['ELASTICSEARCH']['pwd'],
                                              srv=ES_CONN_STR)
ES_FEATURES = ['WALLTIME_HOURS', 'ACTUALCORECOUNT_TOTAL', 'CORECOUNT_TOTAL',
               'CPU_UTILIZATION_FIXED', 'CPUTIME_HOURS', 'SITE_EFFICIENCY']
ES_FEATURES_RENAMED = ['SITE_WALLTIME_H', 'SITE_ACTUALCORECOUNT_TOTAL', 'SITE_CORECOUNT_TOTAL',
                       'SITE_CPU_UTILIZATION', 'SITE_CPUTIME_H', 'SITE_EFFICIENCY']

# Read sql scripts
SQL_DIR = 'TaskStatus/database/sql/'
SQL_SCRIPTS = {}

sql_files = os.listdir(SQL_DIR)
for file in sql_files:
    path = "{0}/{1}".format(SQL_DIR, file)
    name = file[:-4]
    with open(path, 'r') as infile:
        lines = [(" ".join(line.split())) for line
                 in infile.readlines() if not line.startswith('--')]
        SQL_SCRIPTS[name] = ' '.join(lines)


def request_db(request):
    """
    AJAX backend to process the ID and give back the result
    """
    result = {}
    try:
        request_type = request.GET.get('type', None)

        # Request information about jeditaskid
        if request_type == 'get-id-info':
            jeditaskid = request.GET.get('jeditaskid', None)
            result = get_taskid_information(jeditaskid)

        # Request the slowest tasks list
        elif request_type == 'get-slowest-tasks':
            connection = cx_Oracle.connect(CONN_STR)
            start = request.GET.get('start-time', None)
            end = request.GET.get('end-time', None)

            result = get_slowest_user_tasks(connection, start, end) \
                .astype(str).to_dict('split')

        # Request slowest task statuses for boxplot
        elif request_type == 'get-boxplot-information':
            connection = cx_Oracle.connect(CONN_STR)
            start = request.GET.get('start-time', None)
            end = request.GET.get('end-time', None)
            result = get_boxplot_information(connection, start, end) \
                .astype(str).to_dict('split')

        # Request tasks and their lag in statuses
        elif request_type == 'get_tasks_lag':
            connection = cx_Oracle.connect(CONN_STR)
            ids = request.GET.get('ids', None)          # String with ids, separated by comma
            result = get_tasks_lag(connection, ids) \
                .astype(str).to_dict('split')

        else:
            raise Exception('Wrong request type. Possible values: get-id-info, '
                            'get-slowest-tasks, get-barplot-information.')

    except Exception as e:
        print(get_time() + " Error processing the request: " + str(e))
        result['error'] = str(e)

    return JsonResponse(result)


def get_taskid_information(jeditaskid):
    """
    Get information about a particular task
    """
    result = {}

    # Check if id is an integer
    try:
        jeditaskid = int(jeditaskid)
    except ValueError:
        result = {'error': 'id should be an integer', 'jeditaskid': str(jeditaskid)}
        print(get_time() + ' Error! Function "get_taskid_information" got not an integer input.')
        return result

    # Make the request
    try:
        # Establish database connection
        connection = cx_Oracle.connect(CONN_STR)

        # Retrieve time ranges of the job
        min_time, max_time = task_time_range(connection, jeditaskid)

        # Check if there is a cached version of the result
        filename = 'json/' + str(jeditaskid) + '.json'
        if os.path.exists(filename):
            # If there is, check time ranges
            with open(filename, 'r') as infile:
                contents = json.load(infile)
                # If times are the same, return the cached result
                if str(min_time) == contents['min_time'] and str(max_time) == contents['max_time']:
                    result = contents

        # If no result cached or it differs from the database, calculate it
        if result == {}:
            # Retrieve statuses and durations
            jobs = jobs_with_statuses(connection, jeditaskid)
            statuses = statuses_duration(jobs)

            # Count unique jobs, finished and failed
            jobs_count = jobs['PANDAID'].nunique()
            jobs_status_table = jobs['JOBSTATUS'].value_counts()
            jobs_finished_count = 0 if 'finished' not in jobs_status_table.keys() else jobs_status_table['finished']
            jobs_failed_count = 0 if 'failed' not in jobs_status_table.keys() else jobs_status_table['failed']

            # Copy a slice of data to modify time values
            timings_long = statuses[['START_TS', 'END_TS', 'COMPUTINGSITE']].copy()

            # Zero out hours, minutes and seconds
            for col in ["START_TS", "END_TS"]:
                for row in range(len(timings_long.index)):
                    timings_long.at[row, col] = timings_long\
                        .at[row, col].replace(hour=0, minute=0, second=0, microsecond=0)

            # Remove duplicates to reduce load on the ES
            timings = timings_long.drop_duplicates().reset_index(drop=True)

            # Retrieve task sites and efficiency
            # task_sites = get_task_sites(connection, jeditaskid)
            efficiency = get_sites_efficiency_from_es(timings)

            # Put duplicates back
            timings_long = pd.merge(timings_long, efficiency, how='left', on=['START_TS', 'END_TS', 'COMPUTINGSITE'])

            # Merge jobs with the efficiency of the site it was processed on
            statuses[ES_FEATURES_RENAMED] = timings_long[ES_FEATURES_RENAMED]

            # Separate scout jobs from non-scouts
            scouts = statuses[statuses['IS_SCOUT'] == 'SCOUT']
            not_scouts = statuses[statuses['IS_SCOUT'] == 'NOT_SCOUT']

            # From non-scouts get finished and failed jobs
            finished = not_scouts[not_scouts['FINAL_STATUS'] == 'finished']
            # closed = not_scouts[not_scouts['FINAL_STATUS'] == 'closed']
            failed = not_scouts[not_scouts['FINAL_STATUS'] == 'failed']

            # List failed jobs with statuses prior to the failed ones
            pre_failed_statuses = pre_failed(failed)

            # Form the result
            result = {'pre_failed': pre_failed_statuses.astype(str).to_dict('split'),
                      # # 'sequences': _tmpl_sequences,
                      'finished': finished.astype(str).to_dict('split'),
                      'failed': failed.astype(str).to_dict('split'),
                      'all_jobs': statuses.astype(str).to_dict('split'),
                      'scouts': scouts.astype(str).to_dict('split'),
                      'jobs_count': str(jobs_count),
                      'finished_count': str(jobs_finished_count),
                      'failed_count': str(jobs_failed_count),
                      'min_time': str(min_time),
                      'max_time': str(max_time),
                      'jeditaskid': str(jeditaskid)}

            # Cache the result
            filename = 'json/' + str(jeditaskid) + '.json'
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(filename, 'w') as outfile:
                json.dump(result, outfile)

            # Remove older files
            list_of_files = os.listdir(dirname)
            full_path = ["{0}/{1}".format(dirname, x) for x in list_of_files]
            while len(next(os.walk(dirname))[2]) > 500:
                oldest_file = min(full_path, key=os.path.getctime)
                full_path.remove(oldest_file)
                os.remove(oldest_file)

    # If connection to Oracle failed
    except cx_Oracle.DatabaseError as e:
        result = {'error': 'Error connecting to the database. ' + str(e)}

    # Other exceptions
    except Exception as e:
        result = {'error': 'There was an error. ' + str(e)}
        print(get_time() + " [" + str(jeditaskid) + '] Error! Function "get_taskid_information" got an error.')
        traceback.print_exc()

    result['jeditaskid'] = jeditaskid
    return result


def get_db_connection(conn_str):
    """
    Connect to Oracle.

    :param conn_str: Connection configuration
    :return: cx_Oracle object or an error
    """
    try:
        conn = cx_Oracle.connect(conn_str)
        print(get_time() + 'Connected to Oracle!')
        return conn
    except Exception as e:
        print(get_time() + 'Connection to Oracle failed. ' + str(e))
        return None


def jobs_with_statuses(connection, taskid):
    """
    Get job list of a given task.

    :param connection: cx_Oracle object
    :param taskid: ID of the task
    :return: DataFrame with jobs or None if error
    """
    try:
        cursor = connection.cursor()
        query = SQL_SCRIPTS['jobs_with_statuses'].format(taskid=taskid)

        return pd.DataFrame(cursor.execute(query),
                            columns=['PANDAID', 'MODIFTIME_EXTENDED', 'DATE_TRUNCATED',
                                     'JOBSTATUS', 'COMPUTINGSITE', 'MODIFICATIONHOST',
                                     'ATTEMPTNR', 'FINAL_STATUS',
                                     'EXEERRORCODE', 'EXEERRORDIAG',
                                     'SUPERRORCODE', 'SUPERRORDIAG',
                                     'DDMERRORCODE', 'DDMERRORDIAG',
                                     'TASKBUFFERERRORCODE', 'TASKBUFFERERRORDIAG',
                                     'PILOTERRORCODE', 'PILOTERRORDIAG',
                                     'IS_SCOUT'])
    except Exception as ex:
        print(get_time() + " [" + str(taskid) + '] Exception in jobs_with_statuses: ' + str(ex))
        return None


def task_time_range(connection, taskid):
    """
    Get the time range of a given task.

    :param connection: cx_Oracle object
    :param taskid: ID of the task
    :return: start time and end time
    """
    cursor = connection.cursor()
    query = SQL_SCRIPTS['task_time_range'].format(taskid)
    cursor.execute(query)
    row = cursor.fetchone()
    return row[0], row[1]


def get_task_sites(connection, taskid):
    """
    Get computing sites where the task was executed.

    :param connection: cx_Oracle object
    :param taskid: ID of the task
    :return: sites list
    """
    cursor = connection.cursor()
    query = SQL_SCRIPTS['task_sites'].format(taskid)
    sites_list = [row[0] for row in cursor.execute(query)]
    return sites_list  # ','.join("'{0}'".format(w) for w in sites_list)


def get_sites_efficiency(connection, min_time, max_time):
    """
    Calculate the efficiency of sites.

    :param connection: cx_Oracle object
    :param min_time: start time
    :param max_time: end time
    :return: DataFrame with quantities of finished and failed jobs
        for each site for every day in the time span
    """
    # Retrieve a list with site names and quantities of finished and failed jobs
    # for each date of the time span
    cursor = connection.cursor()
    query = SQL_SCRIPTS['efficiency'].format(date_from=min_time, date_to=max_time)

    result = pd.DataFrame([row for row in cursor.execute(query)],
                          columns=['tstamp_day',
                                   'queue',
                                   'walltime_hours',
                                   'actualcorecount_total',
                                   'corecount_total',
                                   'cpu_utilization_fixed',
                                   'cpu_utilization',
                                   'cputime_hours',
                                   'finished_jobs',
                                   'failed_jobs',
                                   'site_efficiency'])

    # Retrieve site names and locations from CRIC
    site_names = []
    site_locations = []

    for queue in result['queue']:
        info = get_site_info(queue)
        site_names.append(info['COMPUTINGSITE'])
        site_locations.append(info['SITE_LOCATION'])

    # Add them to the resulting array
    result['site_name'] = site_names
    result['site_location'] = site_locations

    return result


def get_sites_efficiency_from_es(timings):
    """
    Calculates efficiency from ES cache.

    :param timings: Array containing site name, start and end time
    :return: Efficiency values as a DataFrame
    """
    # Establish connection
    es = Elasticsearch([ES_CONN_STR])

    # Prepare an empty DataFrame
    result_pd = pd.DataFrame(index=[0], columns=['START_TS', 'END_TS', 'COMPUTINGSITE'] + ES_FEATURES)

    # Calculate efficiency for every site
    for index, row in timings.iterrows():
        site = row.to_dict()

        # Specify the query
        query = {
            # If start and end times are equal - extract exact values via _source array
            "_source": False if site["START_TS"] != site["END_TS"] else [x.lower() for x in ES_FEATURES],

            # Filtering options: time and site name
            "query": {
                "bool": {
                    "filter": [
                        {
                            # If start and end date are different - search through a time span: >=START and <=END
                            "range": {
                                "tstamp_day": {
                                    "gte": site["START_TS"].to_pydatetime().strftime("%Y-%m-%d"),
                                    "lte": site["END_TS"].to_pydatetime().strftime("%Y-%m-%d")
                                }
                            }
                        } if site["START_TS"] != site["END_TS"] else {
                            # If start and end are equal, search a particular day
                            "match": {
                                "tstamp_day": site["START_TS"].to_pydatetime().strftime("%Y-%m-%d")
                            }
                        },
                        {
                            # Specify the computing site
                            "term": {
                                "site_name.keyword": site["COMPUTINGSITE"]
                            }
                        }
                    ]
                }
            },

            # If dates are different - get an aggregated result from the ES.
            # Some fields are summarized, from some of them a maximum value is taken.
            "aggs": {} if site["START_TS"] == site["END_TS"] else {
                "finished_jobs":   {"sum": {"field": "finished_jobs"}},
                "failed_jobs":     {"sum": {"field": "failed_jobs"}},
                "walltime_hours":  {"sum": {"field": "walltime_hours"}},
                "corecount_total": {"max": {"field": "corecount_total"}},
                "cputime_hours":   {"sum": {"field": "cputime_hours"}},
                "actualcorecount_total":  {"max": {"field": "actualcorecount_total"}},
                "cpu_utilization_fixed":  {"max": {"field": "cpu_utilization_fixed"}}
            }
        }

        # Make a search
        search = es.search(index='queues-metrics-v1-*', body=query)

        # Init resulting array for this site
        result = {
            "START_TS": site["START_TS"],
            "END_TS": site["END_TS"],
            "COMPUTINGSITE": site["COMPUTINGSITE"]
        }

        # For every feature in the predefined ES_FEATURES array:
        for feature in ES_FEATURES:
            # If start and end dates are different:
            if site["START_TS"] != site["END_TS"]:
                # If feature is site_efficiency:
                if feature == "SITE_EFFICIENCY":
                    # Calculate finished and failed
                    finished_jobs = search['aggregations']['finished_jobs']['value']
                    failed_jobs = search['aggregations']['failed_jobs']['value']
                    # Divide by formula or return 0
                    result[feature] = 0 if finished_jobs + failed_jobs <= 0 else \
                        truncate(finished_jobs / (finished_jobs + failed_jobs), 4)
                else:
                    # For other features get value from aggregations
                    result[feature] = search['aggregations'][feature.lower()]['value']
            # Otherwise, check if ES actually found something
            elif search['hits']['total']['value'] != 0:
                # If so, get value from _source array
                result[feature] = search['hits']['hits'][0]['_source'][feature.lower()]
            else:
                # If no data is found in the ES, fill the result with zeroes
                result[feature] = 0

        result_pd = pd.concat([result_pd, pd.DataFrame(result, index=[0],
                                                       columns=['START_TS', 'END_TS',
                                                                'COMPUTINGSITE'] + ES_FEATURES)])

    # Rename columns to add 'site_' prefix
    result_pd.columns = ['START_TS', 'END_TS', 'COMPUTINGSITE'] + ES_FEATURES_RENAMED

    # Reset index
    return result_pd.iloc[1:].reset_index(drop=True)


def statuses_duration(df):
    """
    Calculates duration of each job status by modificationtime delta
    """
    if df.empty:
        raise Exception('The request result was empty.')

    frames = []
    for k, v in df.groupby(['PANDAID']):
        v = v.loc[:]
        v.sort_values(by=['MODIFTIME_EXTENDED'], inplace=True)
        v['START_TS'] = pd.to_datetime(v['MODIFTIME_EXTENDED'])
        v['END_TS'] = v['START_TS'].shift(-1).fillna(v['START_TS'])
        v['DURATION'] = (v['END_TS'] - v['START_TS']).dt.total_seconds() / 60. / 60.
        status_sequence = v['JOBSTATUS'].values.tolist()
        sequence_set = list(OrderedDict.fromkeys(status_sequence))
        v['STATUS_LEVEL'] = v.apply(lambda x: get_seq_level(x['JOBSTATUS'], sequence_set), axis=1)
        v['SEQUENCE'] = str(sequence_set)
        errorcodes = ['EXE', 'SUP', 'PILOT', 'DDM', 'TASKBUFFER']
        codenames = []
        for column in errorcodes:
            for code in np.unique(v[f"{column}ERRORCODE"].values):
                if code != 0:
                    codenames.append(f"{column}:{code}")
        v['ERROR_CODE'] = ', '.join(codenames)
        frames.append(v)

    return pd.DataFrame() if not frames else \
        pd.concat(frames).sort_values(by=['PANDAID',
                                          'MODIFTIME_EXTENDED',
                                          'JOBSTATUS'
                                          ])


def pre_failed(df):
    """
    Return only jobs statuses previous to failed.

    :param df:
    :return:
    """
    frames = []
    for k, v in df.groupby('PANDAID'):
        v = v.loc[:]
        v.sort_values(by=['MODIFTIME_EXTENDED'], ascending=True, inplace=True)
        # v.drop(v[v['JOBSTATUS'] == 'failed'].index, inplace=True)
        v.rename(columns={"JOBSTATUS": "PRE-FAILED"}, inplace=True)
        # v['FINAL_STATUS'] = 'failed'
        frames.append(v.iloc[[-2]])
    return pd.DataFrame() if not frames else pd.concat(frames)


def get_slowest_job_statuses(df, limit=400):
    return df.sort_values(by=['DURATION'], ascending=False).head(limit)


def get_slowest_user_tasks(connection, start_time, end_time):
    """
    Get the list of the slowest user tasks in a time span.

    :param connection: cx_Oracle object
    :param start_time: start time
    :param end_time: end time
    :return: DataFrame with top 50 longest tasks in a time span
    """
    cursor = connection.cursor()
    query = SQL_SCRIPTS['slowest_user_tasks'].format(start_time, end_time)
    return pd.DataFrame([row for row in cursor.execute(query)],
                        columns=['taskid', 'duration', 'status'])


def get_boxplot_information(connection, start_time, end_time):
    """
    Get the list of all tasks and their statuses and durations in a time span.

    :param connection: cx_Oracle object
    :param start_time: start time
    :param end_time: end time
    :return: DataFrame with duration and status columns
    """
    cursor = connection.cursor()
    query = SQL_SCRIPTS['boxplot_information'].format(start_time, end_time)
    return pd.DataFrame([row for row in cursor.execute(query)],
                        columns=['jeditaskid', 'duration', 'status'])


def get_tasks_lag(connection, ids):
    """
    Get the list of tasks and history of their statuses

    :param connection: cx_Oracle object
    :param ids: task ids
    :return: DataFrame with duration and status columns
    """
    cursor = connection.cursor()
    query = SQL_SCRIPTS['jeditasks_status_lag'].format(ids)
    return pd.DataFrame([row for row in cursor.execute(query)],
                        columns=['jeditaskid', 'modificationtime', 'status', 'lag', 'delay'])


def get_seq_level(status, sequence_set):
    for i, x in enumerate(sequence_set):
        if status == x:
            return i


def get_time():
    """
    Returns current time for logs.

    :return: String with this format: "[01/Sep/2020 01:22:35]"
    """
    return strftime("[%d/%b/%Y %H:%M:%S]", gmtime())


def truncate(n, decimals=0):
    """
    Limit quantity of decimal numbers of 'n' to 'decimals'
    :param n: The number
    :param decimals: Quantity of decimal numbers
    :return: Truncated number
    """
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier


"""
    Not used functions
"""


def sequences_of_statuses(df):
    sequences = {}
    groups = df.groupby('SEQUENCE')
    for k, v in groups:
        sequences[k] = v
    return sequences


def kmodes_samping(df):
    km = KModes(n_clusters=100, init='Huang', n_init=5, verbose=1, n_jobs=-1)
    # model = KPrototypes(n_clusters=100, init='Huang', n_init=5, verbose=1, n_jobs=1)
    data = df[['PANDAID', 'JOBSTATUS', 'COMPUTINGSITE',
               'FINAL_STATUS', 'IS_SCOUT', 'DURATION']].values
    clusters = km.fit_predict(data)
    centers = [row[0] for row in km.cluster_centroids_]
    return df[df['PANDAID'].isin(centers)]
