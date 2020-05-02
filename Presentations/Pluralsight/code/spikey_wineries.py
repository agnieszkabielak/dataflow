#apache beam package typically aliased as beam
import apache_beam as beam
import sys

def find_wineries(line, term):
   if  term in line:
      yield line

#instantiate new Beam pipeline in the main function by calling beam.Pipeline
if __name__ == '__main__':
   p = beam.Pipeline(argv=sys.argv)

#specify where the input file is available
#and where the output of the pipeline should be stored
   input_prefix = 'spikey_winery_list.csv'
   output_prefix = 'output/result'
   
#this job will perform grep operation. Thesearch term is California
#We're going to find all wineries that are based in California
   searchTerm = 'California'

#with the pipeline instantieted, we can use the pipe operator in Python to apply
#all of our transforms.
#We can associate string or a message with every transform that tells you wgat exactly that transform is about.
#We first read in all of the winery data from our input file using beam.io.ReadFromText.
#Every line in the input.csv file becomes an entity in the PCollection that is then passed on to the beam.FlatMap operation.
#A FlatMap is typically used to process a single entity and the output of a flatmap canbe one or more entities for each input entity.
#We want to apply find_wineries method to all of our input entities.
#If the search term is present in a particular input record, then we return that line otherwise, we do nothing.
#Once we grepped all of the wineries that are present in California, we write the results out to file using beam.io.WriteToText.
   (p
      | 'GetWineries' >> beam.io.ReadFromText(input_prefix) 
      | 'GrepWineriesInCalifornia' >> beam.FlatMap(lambda line: find_wineries(line, searchTerm) )
      | 'WriteToFile' >> beam.io.WriteToText(output_prefix)
   )

#This pipeline can now be executed by calling p.run.wait_until_finish
   p.run().wait_until_finish()
