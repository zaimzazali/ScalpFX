import time
import pandas
import json
import os

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



#pylint: disable=too-few-public-methods
class FileReader(metaclass=SingletonMeta):
    '''
    Class of FileReader

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of FileReader object.
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


    @staticmethod
    def readJSONfile(filePath):
        '''
        Reads the data from the JSON file based on the given path.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.

        Returns:
        JSON Object: Object (key-value) of the selected file to read.
        '''
        max_retry = 3
        retry_count = 0

        def tryReadJSON(filePath, retry_count, e):
            if retry_count == max_retry:
                raise Exception(f"readJSONfile() - {e}")

            try:
                jsonFile = open(filePath, 'r', encoding ='utf8')
                data = json.load(jsonFile)
                jsonFile.close()
                return data
            except (Exception, json.JSONDecodeError) as e:
                retry_count += 1
                return tryReadJSON(filePath, retry_count, e)
        
        start = time.time()
        data = tryReadJSON(filePath, retry_count, None)
        end = time.time()
        print("The time of execution of readJSONfile() is :", end-start)
        return data


    @staticmethod
    def readJSONLikeFile(filePath):
        '''
        Reads the data from a JSON-like file based on the given path.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.

        Returns:
        JSON Object: Object (key-value) of the selected file to read.
        '''
        max_retry = 3
        retry_count = 0

        def tryReadJSON(filePath, retry_count, e):
            if retry_count == max_retry:
                raise Exception(f"readJSONLikeFile() - {e}")

            try:
                jsonLikeFile = open(filePath, 'r', encoding ='utf8')
                data = json.load(jsonLikeFile)
                jsonLikeFile.close()
                return data
            except (Exception, json.JSONDecodeError) as e:
                retry_count += 1
                return tryReadJSON(filePath, retry_count, e)
        
        start = time.time()
        data = tryReadJSON(filePath, retry_count, None)
        end = time.time()
        print("The time of execution of readJSONLikeFile() is :", end-start)
        return data


    @staticmethod
    def isFileExists(filePath):
        '''
        Checks whether the file exists.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.

        Returns:
        Bool: Returns True if the file exists, otherwise False.
        '''
        if os.path.exists(filePath):
            return True
        return False


    @staticmethod
    def isFileEmpty(filePath):
        '''
        Checks whether the file is empty.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.

        Returns:
        Bool: Returns True if the file empty, otherwise False.
        '''
        if os.stat(filePath).st_size == 0:
            return True
        return False

