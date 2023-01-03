import logging
import re, json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

PROJECT_ID = "my-project-001-365110"

table_schema1 = parse_table_schema_from_json(json.dumps(json.load(open("/home/monishofficial066/Task2/employee_schema.json"))))
table_schema2 = parse_table_schema_from_json(json.dumps(json.load(open("/home/monishofficial066/Task2/employee_personal_info_schema.json"))))

def run(argv=None):

    q1 = """
            select Emp_ID,CONCAT(First_Name," ",Last_Name) as Name, Gender, E_Mail as Email,
                   Date_of_Birth, Date_of_Joining, Age_in_Company__Years_ as Age_in_Company,
                   Salary, 
                   FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                   FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time from `my-project-001-365110.moni2.monitabl2`
    
         """

    q2 = """
            select
                generate_uuid() as ID, Emp_ID, Father_s_Name as Father_Name, Mother_s_Name as Mother_Name,
                date_diff(CURRENT_DATE, COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', Date_of_Birth ), SAFE.PARSE_DATE('%m/%d/%Y', Date_of_Birth )), year) as Age_in_Yrs, 
                Weight_in_Kgs_ as Weight_in_Kgs, Phone_No_ as Phone_No,
                State, Zip, Region, 
                FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time from `my-project-001-365110.moni2.monitabl2`
         """

    p = beam.Pipeline(options=PipelineOptions())

    (p
     | 'Query from BigQuery' >> beam.io.ReadFromBiqQuery(query=q1, use_standard_sql=True)
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery('my-project-001-365110.miniprojectT2.employee',
                                                schema = table_schema1,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    (p
     | 'Query from BigQuery1' >> beam.io.ReadFromBiqQuery(query=q2, use_standard_sql=True)
     | 'Write to BigQuery1' >> beam.io.WriteToBigQuery('my-project-001-365110.miniprojectT2.employee_personal_info',
                                                schema = table_schema2,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()