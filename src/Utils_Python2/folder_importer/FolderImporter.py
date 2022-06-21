import sys
import os
from pathlib import Path

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



class FolderImporter(metaclass=SingletonMeta):
    '''
    Class of FolderImporter

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of FolderImporter object.
    '''

    def __init__(self):
        self.importAllFolders()
        print('All Folder Packages have been imported!')


    def getAllFolderName(self, mainDir):
        '''
        List out all folder names in the given directory.

        Parameters:
        mainDir (str): Location of the main directory.

        Returns:
        list: Returns the list of folder names within the given directory.
        '''
        try:
            return os.listdir(mainDir)
        except Exception as e:
            raise Exception(f"getAllFolderName() - {e}") from e


    def importAllFolders(self, mainDir=None):
        '''
        Append all folders available in the given directory into Path.

        Parameters:
        mainDir (str): Location of the main directory.

        Returns:
        None: No return value.
        '''
        try:
            if mainDir is None:
                mainDir = f"{Path(__file__).parents[1]}"

            for folderName in os.listdir(mainDir):
                tmpPath = f"{Path(__file__).parents[1]}/{folderName}"
                if os.path.isdir(tmpPath) and not folderName.startswith('.'):
                    if tmpPath not in sys.path:
                        sys.path.append(tmpPath)

        except Exception as e:
            raise Exception(f"importAllFolders() - {e}") from e

