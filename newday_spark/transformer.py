from transformers.stats_transformer import StatsTransformer
from transformers.user_favs_transformer import Top3RankTransformer
from pyspark.sql.dataframe import DataFrame
from typing import List


class Transformer:

    def __init__(self,dataFrame:dict) -> None:
        self.dataFrame = dataFrame
        self.tranformers = [
            StatsTransformer(self.dataFrame,**{'groupByKey':'Title','analytics_var':'Rating'}),
            Top3RankTransformer(self.dataFrame,**{'groupByKey':'UserID','analytics_var':'Rating','category':'Title'})                         
        ]

    def transform(self) -> List[DataFrame]:
        '''
        apply all transforms
        '''
        results = []
        for tranformer in self.tranformers:
            results.append(tranformer.transform()) 

        return results