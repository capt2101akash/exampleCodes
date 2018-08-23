from __future__ import absolute_import

import argparse
import logging
import re
import apache_beam as beam

from past.builtins import unicode
from apache_beam import window 
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://loganalysis/error_log.txt',
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
      '--job_name=log-job',
  ])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:
    lines = p | ReadFromText(known_args.input)
    counts = (
        lines
        | 'window' >> beam.WindowInto(window.GlobalWindows())
        | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)', x))
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    def format_result(time_count):
      (time, count) = time_count
      return '%s: %s' % (time, count)

    output = counts | 'Format' >> beam.Map(format_result)

    output | WriteToText(known_args.output)
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()