from pyspark.sql  import SparkSession

class CSVFileSource:

    def __init__(self, spark:SparkSession, file_locations:dict,file_options:dict):
        self.spark = spark
        self.file_locations = file_locations
        self.file_options = file_options

    def extract_data(self) -> dict:
        # MovieID::Title::Genres
        movies_df = self.spark.read.format(self.file_options['format'])\
                        .option("header", self.file_options['header'])\
                        .option("delimiter", self.file_options['delimiter'])\
                        .load(self.file_locations['movies'])\
                        .withColumnRenamed('_c0','MovieID')\
                        .withColumnRenamed('_c1','Title')\
                        .withColumnRenamed('_c2','Genres')
        
        # UserID::MovieID::Rating::Timestamp
        ratings_df = self.spark.read.format(self.file_options['format'])\
                        .option("header", self.file_options['header'])\
                        .option("delimiter", self.file_options['delimiter'])\
                        .load(self.file_locations['ratings'])\
                        .withColumnRenamed('_c0','UserID')\
                        .withColumnRenamed('_c1','MovieID')\
                        .withColumnRenamed('_c2','Rating')\
                        .withColumnRenamed('_c3','Timestamp')
        
        # UserID::Gender::Age::Occupation::Zip-code
        users_df = self.spark.read.format(self.file_options['format'])\
                        .option("header", self.file_options['header'])\
                        .option("delimiter", self.file_options['delimiter'])\
                        .load(self.file_locations['users'])\
                        .withColumnRenamed('_c0','UserID')\
                        .withColumnRenamed('_c1','Gender')\
                        .withColumnRenamed('_c2','Age')\
                        .withColumnRenamed('_c3','Occupation')\
                        .withColumnRenamed('_c4','Zip')\
        
        return { 'movies':movies_df, 'ratings':ratings_df, 'users':users_df }