import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator, SSHHook

hook = SSHHook(remote_host='server04', username='ubuntu', port=22)

with DAG(
    "generate_daily_summary_table",
    description="1일 수집 데이터들에 대해서 분석용 테이블을 구성",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024,8,2, tz="Asia/Seoul"),
    catchup=False
) as dag:
    command = """
    /home/ubuntu/.pyenv/versions/py3_11_9/bin/python /home/ubuntu/work/save_hdfs_day_csv.py
    """

    ssh_task = SSHOperator(
        task_id="ssh_task",
        command=command,
        ssh_hook=hook,
        cmd_timeout=100
    )
    empty = EmptyOperator(task_id="empty_task")

    ssh_task >> empty
