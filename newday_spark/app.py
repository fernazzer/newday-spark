import sys
import traceback
import os

from pyspark.sql import SparkSession
from loader import FileLoader
from extractor import CSVFileSource
from transformer import Transformer




spark = SparkSession.builder\
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print(f'Spark application name: {spark.sparkContext.appName}')

data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data')
print(f'Data is in this directory: {data_dir}')

output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)),'outputs')
print(f'Outputs will be saved to this directory: {output_dir}')

file_options =  {
                    'format':'csv',
                    'header':False,
                    'delimiter':'::'
                }

file_locations = {
            'movies':os.path.join(data_dir,'movies.dat'),
            'ratings':os.path.join(data_dir,'ratings.dat'),
            'users':os.path.join(data_dir,'users.dat')
        }

try:
       
    fileExtractor = CSVFileSource(spark=spark,file_locations=file_locations,file_options=file_options)
    source_dfs = fileExtractor.extract_data()

    transformer = Transformer(dataFrame=source_dfs)
    results = transformer.transform()

    #output the original three data frames
    for df_key in source_dfs.keys():
        loader = FileLoader(dataFrame=source_dfs[df_key],target_path=os.path.join(output_dir,f'{df_key}.csv'))
        loader.output_to_file()

    #and the results
    output = 1
    for result in results:
        loader = FileLoader(dataFrame=result,target_path=os.path.join(output_dir,f'result_{output}.csv'))
        loader.output_to_file()
        output = output + 1

    
except Exception as e:
    print(traceback.format_exc())
    sc.stop()
    spark.stop()







