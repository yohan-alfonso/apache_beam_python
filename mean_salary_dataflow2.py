import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam.dataframe as df
import apache_beam as beam

from apache_beam.io import ReadFromText, WriteToText
import apache_beam.runners.interactive.interactive_beam as ib


class split_and_lowercase(beam.DoFn):
    def process(self, df):
        # Split the element into individual words
        words = df.split(',')
        # Convert each word to lowercase
        lowercased_words = [word.lower() for word in words]
        # Return the lowercase words as a list
        return [lowercased_words]
    
class filter_mean_salary(beam.DoFn):
    def process(self, column):
        column_value = column[3]
        if column_value != '0.0':
            return [column]

class segregate_dataset(beam.DoFn):
    def process(self, df):
        select_columns = (df[1],df[2],df[3],df[4]+' '+df[7])
        yield select_columns
        
def try_float(value):
    try:
        return float(value)
    except ValueError:
        return None
    
  
   
#-----------------------------------------------------------------------------------        
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-apache-quickstart_dataflow-python-388422/Data/clean_df.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    transformed_data = (
        lines 
            # Split each line by commas
            # Split each line into words and convert to lowercase
        | "SplitAndLowercase" >> beam.ParDo(split_and_lowercase())
            # filter column mean salary diferent from the string '0.0'
        | "filter column" >> beam.ParDo(filter_mean_salary()) 
            # Segregate data that is considered useful for analisis and concatenate period and jobtype for simplicity
        | "segregate data" >>  beam.ParDo(segregate_dataset())
            # Transform data into dictionary
        | 'TransformData' >> beam.Map(lambda columns: {'initial_sal': try_float(columns[0]), 'final_sal': try_float(columns[1]), 'mean_sal': try_float(columns[2]),'job_type': columns[3]})
        
        )

    output = counts | 'Format' >> beam.Map(transformed_data)
    
    output | 'Write' >> WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
   

