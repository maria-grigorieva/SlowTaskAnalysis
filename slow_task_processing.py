import re
import pandas as pd
import cx_Oracle
import configparser
import argparse
from collections import OrderedDict
import sys

# required libraries
# libclntsh.dylib
# libnnz19.dylib
# libclntshcore.dylib.19.1


def get_db_connection(conn_str):
    try:
        conn = cx_Oracle.connect(conn_str)
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
        query = \
            "SELECT s.pandaid, s.modiftime_extended, " \
            "s.jobstatus, " \
            "s.computingsite, " \
            "s.modificationhost, " \
            "j.attemptnr, " \
            "j.jobstatus as final_status " \
        "FROM ATLAS_PANDA.JOBS_STATUSLOG s " \
        "INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED j ON(s.pandaid = j.pandaid) "\
        "WHERE jeditaskid = {} AND " \
            "j.jobstatus = 'failed'" .format(taskid)
        print(query)
        return pd.DataFrame([row for row in cursor.execute(query)],
                            columns=['PANDAID','MODIFTIME_EXTENDED','JOBSTATUS',
                                     'COMPUTINGSITE','MODIFICATIONHOST',
                                     'ATTEMPTNR','FINAL_STATUS'])
    except Exception as ex:
        print('exception occurred: {0}'.format(ex))
        return None


def statuses_duration(df):
   """
   Calculates duration of each job status by modificationtime delta
   """
   frames = []
   for k,v in df.groupby(['PANDAID']):
       v.sort_values(by=['MODIFTIME_EXTENDED'], inplace=True)
       v['START_TS'] = pd.to_datetime(v['MODIFTIME_EXTENDED'])
       v['END_TS'] = v['START_TS'].shift(-1).fillna(v['START_TS'])
       v['DURATION'] = (v['END_TS'] - v['START_TS']).dt.total_seconds()/60./60.
       status_sequence = v['JOBSTATUS'].values.tolist()
       sequence_set = list(OrderedDict.fromkeys(status_sequence))
       v['STATUS_LEVEL'] = v.apply(lambda x: get_seq_level(x['JOBSTATUS'], sequence_set), axis=1)
       v['SEQUENCE'] = str(sequence_set)
       frames.append(v)
   return pd.concat(frames).sort_values(by=['PANDAID',
                                            'MODIFTIME_EXTENDED',
                                            'JOBSTATUS'])


def pre_failed(df):
    """
    Return only jobs statuses previous to failed
    :param df:
    :return:
    """
    frames = []
    for k,v in df.groupby('PANDAID'):
        v.sort_values(by=['MODIFTIME_EXTENDED'], ascending=True, inplace=True)
        v.drop(v[v['JOBSTATUS'] == 'failed'].index, inplace=True)
        v.rename(columns={"JOBSTATUS": "PRE-FAILED"}, inplace=True)
        v['FINAL_STATUS'] = 'failed'
        frames.append(v.iloc[[-1]])
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
                        columns=['taskid','duration']).sort_values(by='duration',
                                                                   ascending=False).head(100)


def get_seq_level(status, sequence_set):
    for i,x in enumerate(sequence_set):
        if status == x:
            return i


def sequences_of_statuses(jeditaskid, df):
    groups = df.groupby('SEQUENCE')
    for k,v in groups:
        v.to_csv(f"{jeditaskid}_sequence_{k}.csv")


def main(jeditaskid, limit=400):
    CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO)
    connection = get_db_connection(CONN_STR)
    # get_slowest_user_tasks(connection, '2020-05-01 00:00:00',
    #                        '2020-05-30 00:00:00').to_csv('slowest_tasks_may2020.csv')
    statuses = statuses_duration(jobs_with_statuses(connection, jeditaskid))
    pre_failed_statuses = pre_failed(statuses)
    slowest_statuses = get_slowest_job_statuses(statuses, limit)
    convert_to_csv(pre_failed_statuses, f"{jeditaskid}-pre-failed.csv")
    convert_to_csv(slowest_statuses, f"{jeditaskid}-slowest-{limit}.csv")
    convert_to_csv(statuses, f"{jeditaskid}_all.csv")
    sequences_of_statuses(jeditaskid, statuses)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('jeditaskid', type=int)
    parser.add_argument('limit', type=int)
    # list_of_choices = ["stream", "file"]
    # parser.add_argument('output_mode', type=str, choices=list_of_choices)
    # parser.add_argument('fname', type=str)
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('config.ini')
    CONN_INFO = {
        'host': config['ORACLE']['host'],
        'port': config['ORACLE']['port'],
        'user': config['ORACLE']['user'],
        'psw': config['ORACLE']['pwd'],
        'service': config['ORACLE']['service'],
    }
    main(args.jeditaskid, args.limit)