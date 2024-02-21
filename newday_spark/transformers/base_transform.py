import abc
from pyspark.sql.dataframe import DataFrame

class BaseTransform(metaclass=abc.ABCMeta):

    def __init__(self,dataFrame:dict,*args,**kwargs):
        self.dataFrame = dataFrame
        self.args = args
        self.kwargs = kwargs
        
        
        
    @abc.abstractclassmethod
    def transform(self):
        pass