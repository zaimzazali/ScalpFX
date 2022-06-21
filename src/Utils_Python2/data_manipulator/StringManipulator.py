import random
import string

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



class StringManipulator(metaclass=SingletonMeta): #pylint: disable=too-few-public-methods   
    '''
    Class of StringManipulator

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of StringManipulator object.
    '''


    @staticmethod
    def getAlphaNumericRandomString(stringLength):
        '''
        Returns a random alphanumeric string based on the given string length.

        Parameters:
        stringLength (int): Determine the length of the random string.

        Returns:
        String: A randomly geneated string (AlphaNumeric).
        '''
        
        result_str = ''
        try:
            source = string.ascii_letters + string.digits
            result_str = ''.join((random.choice(source) for i in range(stringLength)))
        except Exception as e:
            raise Exception(f"getAlphaNumericRandomString() - {e}") from e
        return result_str

