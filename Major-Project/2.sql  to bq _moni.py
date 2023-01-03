import argparse
import logging
import re
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import google.cloud
from google.cloud.sql.connector import Connector
import sqlalchemy


class ReadSQLTable(beam.DoFn):
    """
    parDo class to read the cloudSQL table.

    """        
    
    def process(self, element):     
        conne = google.cloud.sql.connector.Connector()
        conn = conne.connect(
            hostaddr,
            host,
            user=username,
            password=password,
            db=dbname
        )
        # Connect to database
        # Execute a query
        cursor = conn.cursor()
        cursor.execute("SELECT * from Sales")
        result = []
        # Fetch the results
        for row in cursor.fetchall() :
            result.append(dict(zip(('InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country'), row)))

        return result

def run(argv=None, save_main_session = True):

    parser = argparse.ArgumentParser()
    parser.add_argument('--hostaddr',dest='hostaddr',
    default='my-project-001-365110:us-central1:majorpro',help='Host Address')
    parser.add_argument(
        '--host',
        dest='host',
        default='pytds',
        help='Host')
    parser.add_argument(
        '--username',
        dest='username',
        default='sqlserver',
        help='CloudSQL User')
    parser.add_argument(
        '--password',
        dest='password',
        default='Sandip26@nc',
        help='Host')
    parser.add_argument(
        '--dbname',
        dest='dbname',
        default='MajorProject',
        help='Database name')

    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(SetupOptions).requirements_file = '/home/airflow/gcs/dag/requirements.txt'
    global host , hostaddr, dbname, username, password
    host , hostaddr, dbname, username, password =known_args.host,known_args.hostaddr,known_args.dbname,known_args.username,known_args.password
    
    
    
    p = beam.Pipeline(options=pipeline_options)

    init = p | 'Begin pipeline with initiator' >> beam.Create(['All tables initializer'])
    (init
     # Read the file. This is the source of the pipeline. All further
     # processing starts with lines read from the file. We use the input
     # argument from the command line. We also skip the first line which is a
     # header row.
      
     | 'Read from a DB' >>  beam.ParDo(ReadSQLTable())

     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
    | 'Write to BigQuery' >> beam.io.WriteToBigQuery('my-project-001-365110.Major_Project.sqlserver_to_bigquery',
                                            schema='InvoiceNo:STRING,StockCode:STRING,Description:STRING,Quantity:INTEGER,'
'InvoiceDate:STRING,UnitPrice:FLOAT,CustomerID:INTEGER,Country:STRING',
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()        