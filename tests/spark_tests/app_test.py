import os
import json
import shutil
from spark_tests.spark_base import SparkBase
from newday_spark.extractor import CSVFileSource
from newday_spark.loader import FileLoader
from transformers.stats_transformer import StatsTransformer
from newday_spark.transformer import Transformer
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col,mean,max,min

class TransformTests(SparkBase):


    def __init__(self, *args,**kwargs) -> None:
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'data')
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)),'outputs')
        self.file_options = {
            'format':'csv',
            'header':False,
            'delimiter':'::'
        }
        self.file_locations = {
            'movies':os.path.join(self.data_dir,'movies.dat'),
            'ratings':os.path.join(self.data_dir,'ratings.dat'),
            'users':os.path.join(self.data_dir,'users.dat')
        }
        super(TransformTests,self).__init__(*args,**kwargs)
        


    def test_new_csv_file_source(self):
        fs = CSVFileSource(self.spark,self.file_locations,self.file_options)
        self.assertTrue('movies' in self.file_locations)
        self.assertTrue('ratings' in self.file_locations)
        self.assertTrue(fs.file_locations['movies'].__contains__('data/movies.dat'))



    def test_load_movies_data_df(self):
        fs = CSVFileSource(self.spark,self.file_locations,self.file_options)
        
        res = fs.extract_data()

        self.assertIsInstance(res,dict)

        self.assertTrue(res['movies'].count() > 0)
        self.assertTrue(res['ratings'].count() > 0)
        self.assertTrue(res['users'].count() > 0)
        

    def test_load_data_check_columns(self):
        fs = CSVFileSource(self.spark,self.file_locations,self.file_options)

        res = fs.extract_data()

        self.assertTrue('Title' in res['movies'].columns)
        self.assertTrue('Gender' in res['users'].columns)
        self.assertTrue('Rating' in res['ratings'].columns)

    def test_apply_stats_transformer(self):
        data = [("Java", 20000), ("Python", 100000), ("Scala", 3000),("Python", 110),("Python", 40000),("C#",23434)]
        schema = """language STRING,number INTEGER """
        df = self.spark.createDataFrame(data=data,schema=schema)


        analytics_var = 'number'

        resdf = df.groupBy('language')\
                            .agg(
                                min(analytics_var).alias(f'min_{analytics_var}'),
                                max(analytics_var).alias(f'max_{analytics_var}'),
                                mean(analytics_var).alias(f'mean_{analytics_var}')
                            )
        
        
        self.assertTrue('min_number' in resdf.columns)
        self.assertTrue('max_number' in resdf.columns)
        self.assertTrue('mean_number' in resdf.columns)

    def test_apply_rank_transformer(self):
        data = [
            ('A','Java', 2), 
            ('A','Python', 1), 
            ('A','C#', 3), 
            ('B','Scala', 2),
            ('B','Python', 1),
            ('B','C#', 3),
            ('B','Java', 4),
            ('C','Python', 4),
            ('C','C#',2)
        ]
        schema = """person STRING,language STRING, Score INTEGER """
        df = self.spark.createDataFrame(data=data,schema=schema)
        window = Window.partitionBy(df['person']).orderBy(df['Score'].desc())
        df = df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
        self.assertTrue('rank' in df.columns)
        



    def test_load_data_transform(self):
        fs = CSVFileSource(self.spark,self.file_locations,self.file_options)
        
        res = fs.extract_data()

        transformer = Transformer(res)
        resdf = transformer.transform()

        
        self.assertTrue(len(resdf) > 1)
        self.assertTrue('min_Rating' in resdf[0].columns)
        self.assertTrue('max_Rating' in resdf[0].columns)
        self.assertTrue('mean_Rating' in resdf[0].columns)


    def test_integration(self):
        if os.path.exists(os.path.join(self.output_dir,'output_extract_0.csv')):
            shutil.rmtree(os.path.join(self.output_dir,'output_extract_0.csv'))

        if os.path.exists(os.path.join(self.output_dir,'output_extract_1.csv')):
            shutil.rmtree(os.path.join(self.output_dir,'output_extract_1.csv'))
        
        fs = CSVFileSource(self.spark,self.file_locations,self.file_options)
        
        res = fs.extract_data()

        transformer = Transformer(res)
        resdf = transformer.transform()

        for df in resdf:
            df.show()

        counter = 0
        for df in resdf:
            loader = FileLoader(df,os.path.join(self.output_dir,f'output_extract_{str(counter)}.csv'))
            loader.output_to_file()
            counter = counter + 1

        self.assertTrue(os.path.exists(os.path.join(self.output_dir,'output_extract_0.csv')))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir,'output_extract_1.csv')))


