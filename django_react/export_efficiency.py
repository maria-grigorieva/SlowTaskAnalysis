import TaskStatus.database.database
import cx_Oracle
import argparse
import datetime


def export_sites_efficiency():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--from', dest='from_date', type=lambda s: datetime.datetime.strptime(s, '%d-%m-%Y'),
        default=datetime.date.today() - datetime.timedelta(days=1))
    parser.add_argument(
        '--to', dest='to_date', type=lambda s: datetime.datetime.strptime(s, '%d-%m-%Y'),
        default=datetime.date.today())
    parser.add_argument(
        '--output', dest='output_path', type=str,
        default=f"efficiency-{datetime.date.today()}-airflow.csv")
    args = parser.parse_args()

    connection = cx_Oracle.connect(TaskStatus.database.database.CONN_STR)
    data = TaskStatus.database.database.get_sites_efficiency(connection,
                                                             args.from_date,
                                                             args.to_date)

    if data is None or len(data) == 0:
        print(f"No data to store")
        return

    output_path = args.output_path
    print(f"Writing to {output_path}")

    data.to_csv(output_path, index=False)
    print('Done')


if __name__ == '__main__':
    export_sites_efficiency()
