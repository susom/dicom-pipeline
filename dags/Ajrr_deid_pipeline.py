import os, time
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator

from datetime import timedelta
from airflow.utils.dates import days_ago

# [START howto_operator_gce_args_common]
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT', 'som-rit-phi-pacs-prod')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'som-rit-phi-pacs-prod')
GCE_ZONE = os.environ.get('GCE_ZONE', 'us-west1-a')
GCE_INSTANCE = os.environ.get('GCE_INSTANCE', 'deid-worker')
# [END howto_operator_gce_args_common]

# refer to https://medwiki.stanford.edu/display/scci/DICOM+DEID for more details

def get_batch_id():
    postgres_hook = PostgresHook(postgres_conn_id='cloud-cdw-postgres')
    batch_id = postgres_hook.get_first(sql="select max(study_batch_id) from study_batch")
    return int(batch_id[0]) + 1

def wait_for_run_complete(batch_id):
    postgres_hook = PostgresHook(postgres_conn_id='cloud-cdw-postgres')
    status_id = postgres_hook.get_first(sql=
                                        "select status_id from study_batch where study_batch_id=%s",
                                        parameters=[batch_id])
    total_sleep_time=0
    while (int(status_id[0]) !=3 and int(status_id[0]) !=4 and total_sleep_time < 120):
        total_sleep_time += 20
        time.sleep(20)
        status_id = postgres_hook.get_first(sql=
                                            "select status_id from study_batch where study_batch_id=%s",
                                            parameters=[batch_id])

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

## ask Ryan if I need to account for batches.  batches maxes at 2 digit number.

with DAG(
        dag_id = 'Ajrr_deid_pipeline',
        default_args=default_args,
        description='AJRR Deid Pipeline',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['example'],
) as dag:
    #step 4 Copy the content of the validation/batch (cat accessions.txt | pbcopy) and add them to the
    # root of deid-server as queue.txt -- this is done as last step of dicom pipeline

    #steps 5, 6 & 8, reuse same study for AJRR

    #7 In DEID make a new row in STUDY_BATCH
    batch_id = PythonOperator(
        task_id="batch_id",
        python_callable=get_batch_id,
        op_kwargs={'batch_id': 181},
        execution_timeout=timedelta(minutes=5)
    )

    insert_study_batch = PostgresOperator(
        task_id='insert_study_batch',
        postgres_conn_id="cloud-cdw-postgres",
        sql='/sql/insert_study_batch_deid.sql',
        params={"batchid":batch_id},
        autocommit ='True'
    )
#    insert_study_batch = CloudSQLExecuteQueryOperator(
#        task_id='insert_study_batch',
#        #connection id
#        gcp_conn_id='cloud-cdw',
#        #url of the connection
#        gcp_cloudsql_conn_id='cloud-cdw',
#        #gcp_cloudsql_conn_id='gcpcloudsql://eloh:<pwd>@cloud-sql-proxy:5433/deid?',
#        sql='/sql/insert_study_batch_deid.sql',
#        params={"batchid":batch_id},
#        autocommit ='True'
#    )

    #9 In Server.java, change the studybatchid argument to the correct studybatch (eg. 72L)
    # not necessary -- starr-radio Server.java code modified to take local.properties, queue file and
    # batchId as command line options

    #10 From gcp - compute engine - instance groups, edit the deid-worker instance group to add workers.
    #som-rit-phi-pacs-prod VM instance deid-server
    #POST api call =  https://compute.googleapis.com/compute/v1/projects/GCP_PROJECT_ID/zones/GCE_ZONE
    # /instanceGroupManagers/GCE_INSTANCE/resize?size=1
    gcp_start_deid_worker= BashOperator(
        task_id = 'gcp_start_deid_worker',
        bash_command = 'gcloud compute instance-groups managed resize deid-worker --size=1 ' +
                       '--zone={{ params.GCE_ZONE }} ',
        params = {'GCE_ZONE' : GCE_ZONE}
    )
    gcp_wait_for_stable= BashOperator(
        task_id = 'gcp_wait_for_stable',
        bash_command = 'gcloud compute instance-groups managed wait-until --stable deid-worker ' +
                       '--zone={{ params.GCE_ZONE }} ',
        params = {'GCE_ZONE' : GCE_ZONE}
    )

    #10.5 Once workers have started, run queueBatch.
    # If successful, will see a message in IDEA like 'Studies from file
    # queue.txt have been queued for study batch <study_batch_id>'.
    deid_server_template = \
        """
        cd /opt/airflow/dags/jars;java -Dlogback.configurationFile=/opt/airflow/dags/jars/logback.xml \
                       -cp /opt/airflow/dags/jars/starr-radio-server-1.2-SNAPSHOT.jar \
                       com.github.susom.cloudcdw.server.container.Server queue \
                       queueFile=/opt/airflow/logs/queue.txt batchId={{ params.batch_id }} \
                       propertiesFile=/opt/airflow/dags/jars/local.properties > /opt/airflow/logs/deid_server.log 2>&1 
        """
    deid_server= BashOperator(
        task_id = 'deid_server',
        bash_command = deid_server_template,
        params={'batch_id':batch_id},
        execution_timeout=timedelta(minutes=5)
    )

    #11. In DEID, go back to STUDY_BATCH and change the status to 2 (ready)

    update_study_batch_status = PostgresOperator(
        task_id='update_study_batch_status',
        postgres_conn_id="cloud-cdw-postgres",
        sql='/sql/update_study_batch_status.sql',
        params={"batchid":batch_id},
        autocommit ='True'
    )

    # 12 You can see the progress by refreshing the STUDY_BATCH_ACCESSION_TABLE in DEID.
    # When the job is done, the status_id in STUDY_BATCH will go to 3 or 4
    # (will not go to 4 if high-profile MRNs were excluded).
    # select status_id, count(*) from study_batch_accession where study_batch_id = <study_batch_id> group by  status_id;

    # these are all xrays so should be fast, but revisit execution timeout if needed
    wait_for_status = PythonOperator(
        task_id="wait_for_status",
        python_callable=wait_for_run_complete,
        op_kwargs={'batch_id': 181},
        execution_timeout=timedelta(minutes=5)
    )

    #12.5 Turn off deid-workers in compute
    gcp_stop_deid_worker= BashOperator(
        task_id = 'gcp_stop_deid_worker',
        bash_command = 'gcloud compute instance-groups managed resize deid-worker --size=0 ' +
                       '--zone={{ params.GCE_ZONE }} ',
        params = {'GCE_ZONE' : GCE_ZONE}
    )

batch_id >> insert_study_batch >> gcp_start_deid_worker >> gcp_wait_for_stable >> deid_server >> \
update_study_batch_status >> wait_for_status >> gcp_stop_deid_worker
