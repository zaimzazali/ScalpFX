import smtplib, email

# IMPORTANT
import sys
from pathlib import Path
sys.path.append(f"{Path(__file__).parents[1]}/design_pattern/")
from SingletonMeta import SingletonMeta



class EmailService(metaclass=SingletonMeta):
    '''
    Class of EmailService

    Parameters:
    None: Not Applicable.

    Returns:
    Instance: Returns an Instance of EmailService object.
    '''
    
    @staticmethod
    def sendEmailWithAttachments(params):
        try:
            smtpObj = smtplib.SMTP(params['server'])
            smtpObj.sendmail(   params['sender'], 
                                params['reciever'], 
                                params['message'])
            print("Successfully sent email")
        except Exception:
            print("Error: unable to send email")