import time
import pandas

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



class DataFrameManipulator(metaclass=SingletonMeta): #pylint: disable=too-few-public-methods   
    '''
    Class of DataFrameManipulator

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of DataFrameManipulator object.
    '''


    @staticmethod
    def splitDataFrameBasedOn(df, checkCol):
        '''
        Split the dataframe into several grouped dataframe based on the label.

        Parameters:
        df (DataFrame): Original/Master DataFrame.
        checkCol (str): Column label.

        Returns:
        Object: A collection of the DataFrames.
        '''
        dfCollection = {}
        start = time.time()
        try:
            labels = df[checkCol].unique().tolist()
            for l in labels:
                dfCollection[l] = pandas.DataFrame(df[df[checkCol]==l])
        except Exception as e:
            raise Exception(f"splitDataFrameBasedOn() - {e}") from e
        end = time.time()
        print("The time of execution of splitDataFrameBasedOn() is :", end-start)
        return dfCollection

