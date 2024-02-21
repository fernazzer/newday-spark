from pyspark.sql.dataframe import DataFrame

class FileLoader:

    def __init__(self,dataFrame:DataFrame,target_path:str) -> None:
        self.dataFrame = dataFrame
        self.target_path = target_path

    def output_to_file(self) -> dict:
        self.dataFrame.write\
            .format('csv')\
            .option('header', 'true')\
            .save(self.target_path,mode='overwrite')