import time
import json
import os

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



#pylint: disable=too-few-public-methods
class FileWriter(metaclass=SingletonMeta):
    '''
    Class of FileWriter

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of FileWriter object.
    '''


    @staticmethod
    def writeJSONfile(filePath, data):
        '''
        Writes the data to the JSON file based on the given path.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.
        data (object): Data to be written in the said file.

        Returns:
        None: No return value.
        '''
        start = time.time()
        try:
            a_file = open(filePath, 'w', encoding ='utf8')
            json.dump(data, a_file)
            a_file.close()
        except Exception as e:
            raise Exception(f"writeJSONfile() - {e}") from e
        end = time.time()
        print("The time of execution of writeJSONfile() is :", end-start)


    @staticmethod
    def writeJSONLikeFile(filePath, data):
        '''
        Writes the data to the JSON-like file based on the given path.

        Parameters:
        filePath (str): Location of the file with its filename and extension type.
        data (object): Data to be written in the said file.

        Returns:
        None: No return value.
        '''
        start = time.time()
        try:
            a_file = open(filePath, 'w', encoding ='utf8')
            json.dump(data, a_file)
            a_file.close()
        except Exception as e:
            raise Exception(f"writeJSONLikeFile() - {e}") from e
        end = time.time()
        print("The time of execution of writeJSONLikeFile() is :", end-start)


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