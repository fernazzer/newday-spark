from transformers.base_transform import BaseTransform
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

class StatsTransformer(BaseTransform):


    def __init__(self, dataFrame:dict,*args,**kwargs):
        '''
        Returns the Max, Min & Average for a provided partition key
        '''
        super().__init__(dataFrame,*args,**kwargs)
        
        self.groupByKey = self.kwargs.get("groupByKey")
        self.analytics_var = self.kwargs.get("analytics_var")

        


    def transform(self):
        movies_df = self.dataFrame['movies']
        ratings_df = self.dataFrame['ratings']
        users_df = self.dataFrame['users']

        table_to_transform = movies_df\
                                .join(ratings_df,
                                      movies_df.MovieID == ratings_df.MovieID,
                                      'inner')\
                                .select(movies_df.MovieID,movies_df.Title,ratings_df.Rating)
        
        return table_to_transform.groupBy(self.groupByKey)\
                            .agg(
                                F.min(self.analytics_var).alias(f'min_{self.analytics_var}'),
                                F.max(self.analytics_var).alias(f'max_{self.analytics_var}'),
                                F.mean(self.analytics_var).alias(f'mean_{self.analytics_var}')
                            )