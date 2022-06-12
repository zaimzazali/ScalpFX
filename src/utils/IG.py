from src.utils.Singleton import Singleton
from trading_ig.rest import IGService

class IG(metaclass=Singleton):
    config_object = None

    def __init__(self):
        print('IG - Instantiated')
        from configparser import ConfigParser
        self.config_object = ConfigParser()
        
    def getLoginConfig(self, loginType, filePath):
        self.config_object.read(filePath)
        # Create a Config file (trading_ig_config.ini) in the credentials folder with the following format:-
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

    def getHistoricalPricesByDuration(self, igService, params):
        return igService.fetch_historical_prices_by_epic_and_date_range(params['TARGET_EPIC'], 
                                                                        params['RESOLUTION'], 
                                                                        params['start_date'], 
                                                                        params['end_date'])

    def getHistoricalPricesByDataPoints(self, igService, params):
        return igService.fetch_historical_prices_by_epic_and_num_points(params['TARGET_EPIC'], 
                                                                        params['RESOLUTION'], 
                                                                        params['DATA_POINTS'])

    def getOpenPositions(self, igService):
        return igService.fetch_open_positions()

    def isAllowedToOpenDeal(self, igService, params):
        openPositions = igService.fetch_open_positions()

        if (params['deal_direction'] == 'BUY' and 
            len(openPositions[openPositions['direction'] == 'BUY']) < params['LIMIT_BUY_COUNT']):
            return True
        elif (params['deal_direction'] == 'SELL' and 
            len(openPositions[openPositions['direction'] == 'SELL']) < params['LIMIT_SELL_COUNT']):
            return True

        return False

    def openDeal(self, igService, params):
        igService.create_open_position(
            currency_code           =params['currency_code'],
            direction               =params['direction'],
            epic                    =params['epic'],
            order_type              =params['order_type'],
            expiry                  =params['expiry'],
            force_open              =params['force_open'],
            guaranteed_stop         =params['guaranteed_stop'],
            size                    =params['size'], 
            level                   =params['level'],
            limit_distance          =params['limit_distance'],
            limit_level             =params['limit_level'],
            quote_id                =params['quote_id'],
            stop_level              =params['stop_level'],
            stop_distance           =params['stop_distance'],
            trailing_stop           =params['trailing_stop'],
            trailing_stop_increment =params['trailing_stop_increment'])