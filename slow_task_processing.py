import re
import pandas as pd
import cx_Oracle
import configparser
import argparse
import sys

# required libraries
# libclntsh.dylib
# libnnz19.dylib
# libclntshcore.dylib.19.1


def jobs_with_statuses(conn_str,
                           taskid):
    """
    Get failed jobs with all interim statuses
    :param conn_str:
    :param taskid:
    :return:
    """
    try:
        connection = cx_Oracle.connect(conn_str)
        print(connection)
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
        v.sort_values(by=['MODIFTIME_EXTENDED'], inplace=True)
        frames.append(v.iloc[[-2]])
    return pd.concat(frames)


def convert_to_csv(df, filename):
    df.to_csv(filename)


def main(jeditaskid, output_mode='s', fname=None):
    CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO)
    if output_mode == 'stream':
        return pre_failed(statuses_duration(jobs_with_statuses(CONN_STR, jeditaskid)))
    elif output_mode == 'file':
        convert_to_csv(pre_failed(statuses_duration(jobs_with_statuses(CONN_STR, jeditaskid))),fname)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('jeditaskid', type=int)
    list_of_choices = ["stream", "file"]
    parser.add_argument('output_mode', type=str, choices=list_of_choices)
    parser.add_argument('fname', type=str)
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
    main(args.jeditaskid, args.output_mode, args.fname)