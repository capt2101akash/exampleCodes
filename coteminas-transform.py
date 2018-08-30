from __future__ import absolute_import

import argparse
import logging
import re
from datetime import datetime
import apache_beam as beam

from past.builtins import unicode
from apache_beam import window 
from apache_beam.io import ReadFromText

from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


schema = 'Store:INTEGER,Week:INTEGER,Year:INTEGER,Total_Price:FLOAT'
def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='coteminas_sales.stores',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      default='gs://loganalysis/output',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      '--runner=DirectRunner',
      '--project=springmldemoproject',
      '--staging_location=gs://loganalysis/staging',
      '--temp_location=gs://loganalysis/temp',
      '--job_name=bigquery-job',
  ])

  def sum_l(l):                       
    s0, s1 = 0, 0                                        
    for i in range(len(l)):
        #print(l[i])                              
        s0 += l[i][0]                                                   
        # s1 += l[i][1]                
    return s0
  
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:
    rows = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(known_args.input))
    sumFuel = (
      rows
        # | 'Split Commas' >> beam.Map(lambda x: x.strip().split(','))
        # | 'grouping weeks' >> beam.Map(lambda row: row["Store"])
        | 'Extract Week' >> beam.Map(lambda row: (row["Store"], datetime.strptime(row["Date"], '%Y-%m-%d').isocalendar()[1], datetime.strptime(row["Date"],'%Y-%m-%d').year, row['Fuel_Price']))
        | 'Prepare keys' >> beam.Map(lambda x: (x[:-1], map(float, x[-1:])))
        | 'GroupAndSum' >> beam.GroupByKey() 
        | 'sum' >> beam.Map(lambda x: [x[0], sum_l([e for e in x[1]])])
        # | 'write to temp output' >> WriteToText(known_args.output)
    )
    def format_result(store_count):
      (store, count) = store_count
      return {'Store':store[0], 'Week':store[1], 'Year':store[2], 'Total_Price': count}

    output = sumFuel | 'Format' >> beam.Map(format_result)

    output | WriteToText(known_args.output)
    output | 'WriteToBigQuery' >> (beam.io.WriteToBigQuery('springmldemoproject:result.sales', schema=schema ))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()