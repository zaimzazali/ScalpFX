import time
import pandas

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



#pylint: disable=too-few-public-methods
class XlsxReader(metaclass=SingletonMeta):
    '''
    Class of XlsxReader

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of XlsxReader object.
    '''


    @staticmethod
    def readXlsxFile(filePath, sheetName, useCols):
        '''
        Reads the data from the sheet based on 'sheetName' of the xlsx file.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.
        sheetName (str): Name of the Sheet to be read.
        useCols (str): String of Column Index in xlsx format (eg: 'a,b,f:h') to be parsed.

        Returns:
        DataFrame: DataFrame of the selected sheet of the xlsx file.
        '''
        start = time.time()
        try:
            df = pandas.read_excel(io=filePath, sheet_name=sheetName, usecols=useCols)
        except Exception as e:
            raise Exception(f"readXlsxFile() - {e}") from e
        end = time.time()
        print("The time of execution of readXlsxFile() is :", end-start)
        return df

