from transformers.base_transform import BaseTransform
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col,first

class Top3RankTransformer(BaseTransform):

    def __init__(self, dataFrame: dict,*args,**kwargs):
        super().__init__(dataFrame,*args,**kwargs)

        self.groupByKey = self.kwargs.get("groupByKey")
        self.analytics_var = self.kwargs.get("analytics_var")
        self.category = self.kwargs.get("category")
        


    def transform(self):
        '''
        For each user show First, second and third fav movies
        '''
        movies_df = self.dataFrame['movies']
        ratings_df = self.dataFrame['ratings']
        users_df = self.dataFrame['users']

        table_to_transform = users_df\
                                .join(ratings_df,
                                    users_df.UserID == ratings_df.UserID,
                                    'inner')\
                                .join(movies_df,
                                      movies_df.MovieID == ratings_df.MovieID,
                                      'inner')\
                                .select(users_df.UserID,movies_df.Title,ratings_df.Rating)

        window = Window.partitionBy(table_to_transform[self.groupByKey]).orderBy(table_to_transform[self.analytics_var].desc(),table_to_transform[self.category])
        result =  table_to_transform.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
        return result.groupBy(self.groupByKey)\
                        .pivot('rank')\
                        .agg(first(self.category))\
                        .withColumnRenamed('1','Rank 1')\
                        .withColumnRenamed('2','Rank 2')\
                        .withColumnRenamed('3','Rank 3')
        
        