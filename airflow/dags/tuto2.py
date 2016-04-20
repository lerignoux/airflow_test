from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators import BashOperator, PythonOperator, SimpleHttpOperator, DummyOperator
from datetime import datetime, timedelta
import requests
import json

import pdb


default_args = {
    'owner': 'surycat',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'group': 'day_shift',
    'email': 'mail@server.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'X-Surycat-Token': 'API_DEBUG_TOKEN',
    'sms_recipient': '',
    'mail_recipient': 'mail@server.com',
    'api': 'http://nginx',
    'failure_recipient': 'mail@server.com',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}   

dag = DAG('test_workflow', default_args=default_args)


def set_group(*args, **kwargs):
    if datetime.now().hour > 18 or datetime.now().hour < 8:
        Variable.set('group', 'night_shift')
    else:
        Variable.set('group', 'day_shift')


def set_call(*args, **context):
    group = Variable.get('group')
    if group == 'night_shift':
        context['task_instance'].xcom_push(key='recipient', value='0011223344')
    else:
        context['task_instance'].xcom_push(key='recipient', value='0011223344')


def set_mail(*args, **context):
    group = Variable.get('group')
    if group == 'night_shift':
        context['task_instance'].xcom_push(key='recipient', value='mail@server.com')
    else:
        context['task_instance'].xcom_push(key='recipient', value='mail@server.com')


def set_sms(*args, **context):
    group = Variable.get('group')
    if group == 'night_shift':
        context['task_instance'].xcom_push('recipient', '0011223344')
        context['task_instance'].xcom_push('message', 'night airflow message')
    else:
        context['task_instance'].xcom_push('recipient', '0011223344')
        context['task_instance'].xcom_push('message', 'day airflow message')


def send_sms(*args, **context):
    recipient = context['task_instance'].xcom_pull(task_ids='set_sms', key='recipient')
    message = context['task_instance'].xcom_pull(task_ids='set_sms', key='message')
    data = json.dumps({'receiver': recipient, 'message': message})

    url = 'http://nginx/infobip/api/v1/sms'
    headers = {
        'Content-Type': 'application/json',
        'X-Surycat-Token': default_args['X-Surycat-Token']
    }
    r = requests.post(url, data=data, headers=headers)
    print(r.raw)
    return r.status_code


def notify_failure(*args, **kwargs):
    recipient = default_args['failure_recipient']
    message = 'some tasks failed in workflow, check UI'
    data = json.dumps({'to_addr': recipient, 'subject': 'workflow failure', 'body': message})

    url = 'smtp/api/v1/email'
    headers = {
        'Content-Type': 'application/json',
        'X-Surycat-Token': default_args['X-Surycat-Token']
    }
    r = requests.post(url, data=data, headers=headers)


def sum_up_task(*args, **context):

    sms_result = context['task_instance'].xcom_pull(task_ids='send_sms')
    mail_result = context['task_instance'].xcom_pull(task_ids='send_mail')
    call_result = context['task_instance'].xcom_pull(task_ids='send_call')
    # pdb.set_trace()
    print('success')

# t1, t2 and t3 are examples of tasks created by instatiating operators
p0 = PythonOperator(
    task_id='set_group',
    python_callable=set_group,
    dag=dag)

p1 = PythonOperator(
    task_id='set_call',
    provide_context=True,
    python_callable=set_call,
    dag=dag)

p2 = PythonOperator(
    task_id='set_mail',
    provide_context=True,
    python_callable=set_mail,
    dag=dag)

p3 = PythonOperator(
    task_id='set_sms',
    provide_context=True,
    python_callable=set_sms,
    dag=dag)

c1 = SimpleHttpOperator(
    task_id='send_call',
    http_conn_id='http_default',
    endpoint='telephony/api/v1/call',
    headers={
        'X-Surycat-Token': default_args['X-Surycat-Token'],
        'Content-Type': 'application/json'
    },
    data={},
    dag=dag,
    response_check=lambda x: True if x.status_code == 200 else False)

c2 = SimpleHttpOperator(
    task_id='send_mail',
    http_conn_id='http_default',
    endpoint='smtp/api/v1/email',
    headers={
        'X-Surycat-Token': default_args['X-Surycat-Token'],
        'Content-Type': 'application/json'
    },
    data=json.dumps({"to_addr": default_args['mail_recipient'],
                     "subject": "testing emailing", "body": "test email airflow"}),
    dag=dag)

c3 = PythonOperator(
    task_id='send_sms',
    provide_context=True,
    python_callable=send_sms,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

join = DummyOperator(
    task_id='join',
    trigger_rule='all_done',
    dag=dag
)

sum_up = PythonOperator(
    task_id='sum_up',
    provide_context=True,
    python_callable=sum_up_task,
    dag=dag,
    execution_timeout=timedelta(seconds=60),
    on_failure_callback=notify_failure,
    )

p1.set_upstream(p0)
p2.set_upstream(p0)
p3.set_upstream(p0)
c1.set_upstream(p1)
c2.set_upstream(p2)
c3.set_upstream(p3)
c3.set_downstream(join)
c2.set_downstream(join)
sum_up.set_upstream(join)
