from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator

from datetime import timedelta
from airflow.utils.dates import days_ago

# Follows steps from https://medwiki.stanford.edu/display/scci/DICOM+Image+Acquisition+Pipeline

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'eloh',
    'depends_on_past': False,
    'email': ['eloh@stanford.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

inserts = []

## ask Ryan if I need to account for batches.  batches maxes at 2 digit number.
def get_new_acc_numbers ():
    oracle_hook = OracleHook(oracle_conn_id='tris_rim')
    records = oracle_hook.get_records(sql="select p.accession_number from dcm_qa h, dcm_pipeline p "
                                      + "where h.customer in ('Total Joint') and "
                                      + "p.accession_number=h.accession_number and state in (2,23) "
                                      + "and trunc(p.UPDATED_ON)>trunc(sysdate - 7)")
    #with open('/opt/airflow/logs/test.txt', 'w') as f:  # 'a' means to append
    #    for record in records:
    #        f.write("'" + f'{record[0]}' +"'\n")
    for record in records:
        inserts.append("insert into ajrr values ('"+record[0]+"') ON CONFLICT DO NOTHING")

dcm_qa_updates = []
# saw this at https://deveshpoojari.medium.com/elementals-of-airflow-part-2-acbe648c8844
# might be worth trying-  postgres copy function
# copy (select * from {{ params.schema_table }} where date between '{{ task_instance.xcom_pull(
# task_ids = "date_range", key = "date_from")}}' and '{{ task_instance.xcom_pull(key = "date_till", task_ids = "date_range") }}'
# where revenue > 7) to 'postgres_data.csv' delimiter ',' csv header;

def get_srs_updates ():
    postgres_hook = PostgresHook(postgres_conn_id='cloud-cdw-postgres')
    records = postgres_hook.get_records(sql="SELECT s.accession_number, count(DISTINCT r.series_instance_uid) srs "
                                            +"FROM dicom_study s, dicom_series r, ajrr m "+
                                            "WHERE s.study_instance_uid=r.study_instance_uid AND "+
                                            "s.accession_number=m.accession_number AND "+
                                            "date_trunc('day',s.last_updated) > date_trunc('day',(CURRENT_DATE - 7)) "
                                            "GROUP BY s.accession_number")
    #with open('/opt/airflow/logs/test.txt', 'w') as f:  # 'a' means to append
    #    for record in records:
    #        f.write("'" + f'{record[0]}' +"'\n")
    for record in records:
        dcm_qa_updates.append("update dcm_qa set gcp_srs=" + str(record[1]) +
                                " where customer='Total Joint' and accession_number='" + str(record[0]) + "'")
"""
alternative function that reads series updates and accession numbers from a file
def update_gcp_srs() :
    with open('/opt/airflow/dags/scripts/cloud_sql.out', 'r') as f:
        content = f.readlines()
    #with open('/opt/airflow/logs/test.txt', 'w') as f:  # used for testing
    for x in content:
        tmp = x.replace(" ","").replace("\n","")
        tuple = tmp.split("|")
        if len(tuple)==2:
            #f.write(f'{tuple[0]}' + ", " + f'{tuple[1]}'  +"\n")
            dcm_qa_updates.append("update dcm_qa set gcp_srs=" + tuple[1] +
                          " where customer='Total Joint' and accession_number='" + tuple[0] + "'")
"""
def download_success() :
    oracle_hook = OracleHook(oracle_conn_id='tris_rim')
    records = oracle_hook.get_records(
        sql="select distinct accession_number from dcm_qa "
           + "where customer='Total Joint' and qa_pass='Y' "
           + " and trunc(qa_date)>trunc(sysdate-7)")

    with open('/opt/airflow/logs/queue.txt', 'w') as f:  # 'a' means to append
        for record in records:
            f.write(f'{record[0]}' + '\n')

with DAG(
    dag_id = 'Ajrr_dicom_pipeline',
    default_args=default_args,
    description='AJRR DICOM pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    #creates view of AJRR procedure dicoms -- includes latest dicom related to hip/knee
    # within 1 year prior to procedure as well as all dicoms after procedure
    create_view = OracleOperator(
        task_id='create_view',
        oracle_conn_id='tris_rim',
        sql='/sql/1_create_ajrr_dicom_view.sql',
        autocommit ='True'
    )
    # insert list of accession numbers into dcm_qa
    insert_acc_num = OracleOperator(
        task_id='insert_acc_num',
        oracle_conn_id='tris_rim',
        sql= '/sql/2_insert_acc_num.sql',
        autocommit ='True'
    )

    #update the failed accessions for retry
    retry_accession = OracleOperator(
        task_id='retry_accession',
        oracle_conn_id='tris_rim',
        sql= '/sql/3_retry_accession.sql',
        autocommit ='True',
    )
    # insert missing accession numbers
    insert_missing = OracleOperator(
        task_id='insert_missing',
        oracle_conn_id='tris_rim',
        sql= '/sql/4_insert_missing_acc_num.sql',
        autocommit ='True',
    )
    get_new_acc_num_task = PythonOperator(
        task_id='get_new_acc_numbers',
        python_callable=get_new_acc_numbers,
    )
    gcp_insert = PostgresOperator(
        task_id='gcp_insert',
        postgres_conn_id='cloud-cdw-postgres',
        sql=inserts,
        autocommit ='True',
    )
    get_srs_updates = PythonOperator(
        task_id='get_srs_updates',
        python_callable=get_srs_updates,
    )
    """
    gcp_insert = CloudSQLExecuteQueryOperator(
        task_id='gcp_insert',
    #    #connection id
        gcp_conn_id='cloud-cdw',
    #    #url of the connection
        gcp_cloudsql_conn_id='cloud-cdw',
        #gcp_cloudsql_conn_id='gcpcloudsql://eloh:<pwd>@cloud-sql-proxy:5433/deid?',
        sql=inserts,
        autocommit ='True'
    )
    #get gcp_srs count per accession_number, writes to /opt/airflow/dags/scripts/cloud_sql.out
    gcp_query = BashOperator(
        task_id='gcp_query',
        do_xcom_push=True,
        # "scripts" folder is under "/opt/airflow/dags, space at end of cmd is important"
        bash_command="/opt/airflow/dags/scripts/cloud_sql.sh ",
    )

    #read gcp query gcp_srs series count
    update_gcp_srs_task = PythonOperator(
        task_id='update_gcp_srs_task',
        python_callable=update_gcp_srs,
    )
"""
    #update counts on the dcm_qa table by running output from above query on dcm_qa table in tris_rim
    dcm_qa_update_counts = OracleOperator(
        task_id = "dcm_qa_update_counts",
        oracle_conn_id='tris_rim',
        sql=dcm_qa_updates,
        autocommit = True,
    )
    #update dcm_qa table by comparing series count
    dcm_qa_update_pass = OracleOperator(
        task_id = "dcm_qa_update_pass",
        oracle_conn_id='tris_rim',
        sql="/sql/6_update_dcm_qa_pass_series_count.sql",
        autocommit = True
    )
    download_success_task = PythonOperator(
        task_id='download_success_task',
        python_callable=download_success,
    )


create_view >> insert_acc_num >> retry_accession >> insert_missing >> get_new_acc_num_task >> gcp_insert >> get_srs_updates >>dcm_qa_update_counts >> dcm_qa_update_pass >> download_success_task
