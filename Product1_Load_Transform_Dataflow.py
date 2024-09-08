import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

# Load the JSON schema for BigQuery
table_schema = parse_table_schema_from_json('{"fields": [{"name": "string_field0", "type": "STRING", "mode": "NULLABLE"}]}')

# Define the UDF to process each line of the file
def process_newline_delimited(element):
    # Return each line as a dictionary
    return {'string_field0': element}

def run():
    # Define pipeline options
    options = PipelineOptions()

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from GCS' >> beam.io.ReadFromText('gs://watch-bucket/Product1/*.csv')  # Use wildcard pattern
            | 'Process Each Line' >> beam.Map(process_newline_delimited)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table='practice-1-433516:sandbox.Product1',
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()