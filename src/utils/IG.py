from Singleton import Singleton
from trading_ig.rest import IGService

class IG(metaclass=Singleton):
    config_object = None

    def __init__(self):
        print('IG - Instantiated')
        from configparser import ConfigParser
        self.config_object = ConfigParser()
        
    def getLoginConfig(self, loginType):
        self.config_object.read('../credentials/trading_ig_config.ini')
        # Create a Config file (trading_ig_config.ini) of your credential in the following format:-
        # 
        # [demo]
        # username = <USERNAME>
        # password = <PASSWORD>
        # api_key = <API_KEY>
        # acc_type = DEMO
        # acc_number = <123ABC>
        #
        # [live]
        # username = <USERNAME>
        # password = <PASSWORD>
        # api_key = <API_KEY>
        # acc_type = LIVE
        # acc_number = <123ABC>
        #

        if loginType == 'live':
            return self.config_object['live']
        elif loginType == 'demo':
            return self.config_object['demo']
        else:
            return None

    def getIgService(self, config):
        return IGService(config['username'], config['password'], config['api_key'], config['acc_type'])

    def getIgAccountDetails(self, igService):
        return igService.create_session()
