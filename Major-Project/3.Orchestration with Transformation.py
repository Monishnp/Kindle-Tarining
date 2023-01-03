from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


dag= DAG(dag_id= 'My_Transformation_for bq', start_date = datetime.today(), catchup=False, schedule_interval='@once')


cloudsql_to_bq = BeamRunPythonPipelineOperator(
    task_id="cloudsql_to_bq",
    runner="DataflowRunner",
    py_file="gs://us-central1-mainpro-a06ea0ba-bucket/dags/sql  to bq _moni.py",
    pipeline_options={'tempLocation' : 'gs://majorpro/temp/', 'stagingLocation': 'gs://majorpro/temp/'},
    py_options=[],
    py_requirements=['apache-beam[gcp]==2.42.0','cloud-sql-python-connector[pytds]==0.6.1','pyodbc==4.0.34','SQLAlchemy==1.4.41','pymssql==2.2.5','sqlalchemy-pytds==0.3.4','pylint==2.15.4'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(job_name='mainproject-job', project_id='my-project-001-365110', 
    location="us-central1"),
    dag=dag)



query1 = '''
            SELECT InvoiceNo, StockCode, Description, Quantity, PARSE_DATETIME('%m/%d/%Y %H:%M', InvoiceDate) AS InvoiceDate, UnitPrice, CustomerID,Country FROM `my-project-001-365110.Major_Project.sql_to_bigquery`
         '''

Loading = BigQueryOperator(task_id='Loading', 
                            destination_dataset_table= "my-project-001-365110.Major_Project.Main_data",
                            sql=query1,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            dag=dag)
                            

query2 = '''
            delete from `my-project-001-365110.Major_Project.Main_data`
            where Quantity  < 0 or CustomerID = 0;
         '''
      

filter1 = BigQueryOperator(task_id='filter1', 
                            sql=query2,
                            use_legacy_sql=False,
                            dag=dag)

query3 = '''
            delete from `my-project-001-365110.Major_Project.Main_data`
            where  EXTRACT(year FROM InvoiceDate)  = 2011 and EXTRACT(month FROM InvoiceDate) = 12;
         '''

filter2 = BigQueryOperator(task_id='filter2', 
                            sql=query3,
                            use_legacy_sql=False,
                            dag=dag)

query4 = '''
            SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID,Country,Quantity * UnitPrice as ItemTotal
            FROM `my-project-001-365110.Major_Project.Main_data`;
         '''

ONLINE_RETAIL = BigQueryOperator(task_id='ONLINE_RETAIL', 
                            destination_dataset_table= "my-project-001-365110.Major_Project.ONLINE_RETAIL",
                            sql=query4,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            dag=dag
                            )

query5 = '''
            select CustomerID, sum(ItemTotal) as TotalSales,count(Quantity) as OrderCount, avg(ItemTotal) as AvgOrderValue 
            from `my-project-001-365110.Major_Project.ONLINE_RETAIL`
            group by CustomerID;
         '''

CUSTOMARY_SUMMARY = BigQueryOperator(task_id='CUSTOMARY_SUMMARY', 
                            destination_dataset_table= "my-project-001-365110.Major_Project.CUSTOMARY_SUMMARY",
                            sql=query5,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            dag=dag)

query6 = '''
            select Country, sum(ItemTotal) as TotalSales, sum(ItemTotal)*100/(select sum(ItemTotal) 
            from `my-project-001-365110.Major_Project.ONLINE_RETAIL`) as PercentofCountrySales from `my-project-001-365110.Major_Project.ONLINE_RETAIL` 
            group by Country;
         '''

SALES_SUMMARY = BigQueryOperator(task_id='SALES_SUMMARY', 
                            destination_dataset_table= "my-project-001-365110.Major_Project.SALES_SUMMARY",
                            sql=query6,
                            use_legacy_sql=False,
                            create_disposition="CREATE_IF_NEEDED",
                            write_disposition="WRITE_TRUNCATE",
                            dag=dag)

Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> cloudsql_to_bq >> Loading >> filter1 >> filter2 >> ONLINE_RETAIL >> [CUSTOMARY_SUMMARY, SALES_SUMMARY] >> End