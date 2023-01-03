import apache_beam as beam
import logging
import json
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = 'my-project-001-365110'
SCHEMA = parse_table_schema_from_json(json.dumps(json.load(open('schema-auditcol.json'))))


def run(argv=None):
    query = """ SELECT
                    *,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()) AS modified_Time
                FROM `my-project-001-365110.moni2.monitabl2`"""


    p = beam.Pipeline(options = PipelineOptions())
    (p
     |'Query from Bigquery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
     |'Write to BigQuery' >> beam.io.WriteToBigQuery('{0}:my-project-001-365110.moni2.monitabaudit'.format(PROJECT_ID),
                                            schema=SCHEMA,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
