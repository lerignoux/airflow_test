# airflow test

A repo to test [airflow library](http://nerds.airbnb.com/airflow/)

## Testing:

in the container : 
Check dag script:
```bash
list_dags
```

Test tasks:
```bash
airflow list_tasks test_workflow
airflow test test_workflow {task_id} 2016-00-01
```

un workflow
```bash
airflow backfill test_workflow -s 2016-04-19
```

you have to change the date if you want to rerun everything, otherwise only failed tasks will be retried
