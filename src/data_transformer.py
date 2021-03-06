from round2 import round2
import ta as ta
import json
from datetime import datetime



SUPER_FAST = 9
FAST = 12
SLOW = 26
NORMALISED_DATA_PATH = '/ScalpFX/src/data/unnormalized_parameters.json'



def calcFeaturesEngineering(df, verbose=False):
    working_df = df.copy()

    def diffTwoFloats(x,y):
        return round2(x-y, 5)

    def signIndicator(x):
        if x > 0:
            return -1
        elif x < 0:
            return 1
        else:
            return 0

    working_df['O-H'] = working_df.apply(lambda x: abs(diffTwoFloats(x['open'], x['high'])), axis=1)
    working_df['O-L'] = working_df.apply(lambda x: diffTwoFloats(x['open'], x['low']), axis=1)

    working_df['C-H'] = working_df.apply(lambda x: abs(diffTwoFloats(x['close'], x['high'])), axis=1)
    working_df['C-L'] = working_df.apply(lambda x: diffTwoFloats(x['close'], x['low']), axis=1)

    working_df['H-L'] = working_df.apply(lambda x: diffTwoFloats(x['high'], x['low']), axis=1)

    working_df['O-C-Value'] = working_df.apply(lambda x: abs(diffTwoFloats(x['open'], x['close'])), axis=1)
    working_df['O-C-Sign'] = working_df.apply(lambda x: signIndicator(x['open'] - x['close']), axis=1)

    if verbose:
        print(working_df)

    return working_df


def calcTA(df, verbose=False):
    working_df = df.copy()

    # Momentum
    working_df['Mom_AO'] = ta.momentum.AwesomeOscillatorIndicator(working_df['high'], working_df['low'], window1=FAST, window2=SLOW).awesome_oscillator()
    Mom_PPO = ta.momentum.PercentagePriceOscillator(working_df['close'], window_slow=SLOW, window_fast=FAST, window_sign=SUPER_FAST)
    working_df['Mom_PPO_ppo'] = Mom_PPO.ppo()
    working_df['Mom_PPO_ppo_hist'] = Mom_PPO.ppo_hist()
    working_df['Mom_PPO_ppo_signal'] = Mom_PPO.ppo_signal()
    Mom_PVO = ta.momentum.PercentageVolumeOscillator(working_df['volume'], window_slow=SLOW, window_fast=FAST, window_sign=SUPER_FAST)
    working_df['Mom_PVO_pvo'] = Mom_PVO.pvo()
    working_df['Mom_PVO_pvo_hist'] = Mom_PVO.pvo_hist()
    working_df['Mom_PVO_pvo_signal'] = Mom_PVO.pvo_signal()
    working_df['Mom_ROC'] = ta.momentum.ROCIndicator(working_df['close'], window=FAST).roc()
    working_df['Mom_RSI'] = ta.momentum.RSIIndicator(working_df['close'], window=FAST).rsi()
    Mom_SRSI = ta.momentum.StochRSIIndicator(working_df['close'], window=FAST, smooth1=3, smooth2=3)
    working_df['Mom_SRSI_stochrsi'] = Mom_SRSI.stochrsi()
    working_df['Mom_SRSI_stochrsi_d'] = Mom_SRSI.stochrsi_d()
    working_df['Mom_SRSI_stochrsi_k'] = Mom_SRSI.stochrsi_k()
    Mom_SO = ta.momentum.StochasticOscillator(working_df['close'], working_df['high'], working_df['low'], window=FAST, smooth_window=3)
    working_df['Mom_SO_stoch'] = Mom_SO.stoch()
    working_df['Mom_SO_stoch_signal'] = Mom_SO.stoch_signal()
    working_df['Mom_TSI'] = ta.momentum.TSIIndicator(working_df['close'], window_slow=SLOW, window_fast=FAST).tsi()
    working_df['Mom_UO'] = ta.momentum.UltimateOscillator(working_df['high'], working_df['low'], working_df['close'], window1=SUPER_FAST, window2=FAST, window3=SLOW).ultimate_oscillator()

    # Volume
    working_df['Vol_ADI'] = ta.volume.AccDistIndexIndicator(working_df['high'], working_df['low'], working_df['close'], working_df['volume']).acc_dist_index()
    working_df['Vol_CMF'] = ta.volume.ChaikinMoneyFlowIndicator(working_df['high'], working_df['low'], working_df['close'], working_df['volume'], window=FAST).chaikin_money_flow()
    Vol_EoM = ta.volume.EaseOfMovementIndicator(working_df['high'], working_df['low'], working_df['volume'], window=FAST)
    working_df['Vol_EoM_ease_of_movement'] = Vol_EoM.ease_of_movement()
    working_df['Vol_EoM_sma_ease_of_movement'] = Vol_EoM.sma_ease_of_movement()
    working_df['Vol_FI'] = ta.volume.ForceIndexIndicator(working_df['close'], working_df['volume'], window=FAST).force_index()
    working_df['Vol_MFI'] = ta.volume.MFIIndicator(working_df['high'], working_df['low'], working_df['close'], working_df['volume'], window=FAST).money_flow_index()
    working_df['Vol_NVI'] = ta.volume.NegativeVolumeIndexIndicator(working_df['close'], working_df['volume']).negative_volume_index()
    working_df['Vol_OBV'] = ta.volume.OnBalanceVolumeIndicator(working_df['close'], working_df['volume']).on_balance_volume()
    working_df['Vol_VPT'] = ta.volume.VolumePriceTrendIndicator(working_df['close'], working_df['volume']).volume_price_trend()
    working_df['Vol_VWAP'] = ta.volume.VolumeWeightedAveragePrice(working_df['high'], working_df['low'], working_df['close'], working_df['volume'], window=FAST).volume_weighted_average_price() # Good for day-trading

    # Volatility
    working_df['Vola_ATR'] = ta.volatility.AverageTrueRange(working_df['high'], working_df['low'], working_df['close'], window=FAST).average_true_range()
    Vola_BB = ta.volatility.BollingerBands(working_df['close'], window=FAST)
    working_df['Vola_BB_bollinger_hband'] = Vola_BB.bollinger_hband()
    working_df['Vola_BB_bollinger_hband_indicator'] = Vola_BB.bollinger_hband_indicator()
    working_df['Vola_BB_bollinger_lband'] = Vola_BB.bollinger_lband()
    working_df['Vola_BB_bollinger_lband_indicator'] = Vola_BB.bollinger_lband_indicator()
    working_df['Vola_BB_bollinger_mavg'] = Vola_BB.bollinger_mavg()
    working_df['Vola_BB_bollinger_pband'] = Vola_BB.bollinger_pband()
    working_df['Vola_BB_bollinger_wband'] = Vola_BB.bollinger_wband()
    Vola_DC = ta.volatility.DonchianChannel(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Vola_DC_donchian_channel_hband'] = Vola_DC.donchian_channel_hband()
    working_df['Vola_DC_donchian_channel_lband'] = Vola_DC.donchian_channel_lband()
    working_df['Vola_DC_donchian_channel_mband'] = Vola_DC.donchian_channel_mband()
    working_df['Vola_DC_donchian_channel_pband'] = Vola_DC.donchian_channel_pband()
    working_df['Vola_DC_donchian_channel_wband'] = Vola_DC.donchian_channel_wband()
    Vola_KC = ta.volatility.KeltnerChannel(working_df['high'], working_df['low'], working_df['close'], window=FAST, window_atr=FAST, original_version=False)
    working_df['Vola_KC_keltner_channel_hband'] = Vola_KC.keltner_channel_hband()
    working_df['Vola_KC_keltner_channel_hband_indicator'] = Vola_KC.keltner_channel_hband_indicator()
    working_df['Vola_KC_keltner_channel_lband'] = Vola_KC.keltner_channel_lband()
    working_df['Vola_KC_keltner_channel_lband_indicator'] = Vola_KC.keltner_channel_lband_indicator()
    working_df['Vola_KC_keltner_channel_mband'] = Vola_KC.keltner_channel_mband()
    working_df['Vola_KC_keltner_channel_pband'] = Vola_KC.keltner_channel_pband()
    working_df['Vola_KC_keltner_channel_wband'] = Vola_KC.keltner_channel_wband()

    # Trend
    Trend_ADX = ta.trend.ADXIndicator(working_df['high'], working_df['low'], working_df['close'], window=FAST, fillna=True)
    working_df['Trend_ADX_adx'] = Trend_ADX.adx()
    working_df['Trend_ADX_adx_neg'] = Trend_ADX.adx_neg()
    working_df['Trend_ADX_adx_pos'] = Trend_ADX.adx_pos()
    Trend_AI = ta.trend.AroonIndicator(working_df['close'], window=FAST)
    working_df['Trend_AI_aroon_down'] = Trend_AI.aroon_down()
    working_df['Trend_AI_aroon_indicator'] = Trend_AI.aroon_indicator()
    working_df['Trend_AI_aroon_up'] = Trend_AI.aroon_up()
    working_df['Trend_CCI'] = ta.trend.CCIIndicator(working_df['high'], working_df['low'], working_df['close'], window=FAST).cci()
    working_df['Trend_DPO'] = ta.trend.DPOIndicator(working_df['close'], window=FAST).dpo()
    working_df['Trend_EMA_12'] = ta.trend.EMAIndicator(working_df['close'], window=FAST).ema_indicator()
    working_df['Trend_EMA_50'] = ta.trend.EMAIndicator(working_df['close'], window=50).ema_indicator()
    working_df['Trend_EMA_100'] = ta.trend.EMAIndicator(working_df['close'], window=100).ema_indicator()
    Trend_Ichi = ta.trend.IchimokuIndicator(working_df['high'], working_df['low'], window1=SUPER_FAST, window2=FAST, window3=SLOW)
    working_df['Trend_Ichi_ichimoku_a'] = Trend_Ichi.ichimoku_a()
    working_df['Trend_Ichi_ichimoku_b'] = Trend_Ichi.ichimoku_b()
    working_df['Trend_Ichi_ichimoku_base_line'] = Trend_Ichi.ichimoku_base_line()
    working_df['Trend_Ichi_ichimoku_conversion_line'] = Trend_Ichi.ichimoku_conversion_line()
    Trend_MACD = ta.trend.MACD(working_df['close'], window_fast=FAST, window_slow=SLOW, window_sign=SUPER_FAST)
    working_df['Trend_MACD_macd'] = Trend_MACD.macd()
    working_df['Trend_MACD_macd_diff'] = Trend_MACD.macd_diff()
    working_df['Trend_MACD_macd_signal'] = Trend_MACD.macd_signal()
    working_df['Trend_MI'] = ta.trend.MassIndex(working_df['high'], working_df['low'], window_fast=FAST, window_slow=SLOW).mass_index()
    working_df['Trend_SMA_12'] = ta.trend.SMAIndicator(working_df['close'], window=FAST).sma_indicator()
    working_df['Trend_SMA_50'] = ta.trend.SMAIndicator(working_df['close'], window=50).sma_indicator()
    working_df['Trend_SMA_100'] = ta.trend.SMAIndicator(working_df['close'], window=100).sma_indicator()
    working_df['Trend_STC'] = ta.trend.STCIndicator(working_df['close'], window_fast=FAST, window_slow=SLOW).stc()
    working_df['Trend_TRIX'] = ta.trend.TRIXIndicator(working_df['close'], window=FAST).trix()
    Trend_VI = ta.trend.VortexIndicator(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Trend_VI_vortex_indicator_diff'] = Trend_VI.vortex_indicator_diff()
    working_df['Trend_VI_vortex_indicator_neg'] = Trend_VI.vortex_indicator_neg()
    working_df['Trend_VI_vortex_indicator_pos'] = Trend_VI.vortex_indicator_pos()
    working_df['Trend_WMA'] = ta.trend.WMAIndicator(working_df['close'], window=FAST).wma()

    # print(working_df)
    # print(working_df.shape[0])
    working_df = working_df.dropna()
    # print(working_df.shape[0])

    if verbose:
        print(working_df)

    return working_df


def normalised(df, mode='train'):
    working_df = df.copy()

    # Doing the Min-Max Normalization
    # normX = (X - minX) / (maxX - minX)

    unNormalizeParams = {}

    if mode == 'train':
        for column in df.columns:
            working_df[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
            try:
                # Numbers
                unNormalizeParams[column] = {'min':float(df[column].min()), 'max':float(df[column].max())}
            except Exception as e:
                # Datetime
                unNormalizeParams[column] = {'min':df[column].min().strftime("%Y-%m-%d %H:%M:%S"), 'max':df[column].max().strftime("%Y-%m-%d %H:%M:%S")}
    elif mode == 'predict':
        f = open(NORMALISED_DATA_PATH)
        data = json.load(f)
        f.close()
        for column in df.columns:
            if column == 'datetime':
                working_df[column] = ( 
                                        (df[column] - datetime.strptime(data[column]['min'], "%Y-%m-%d %H:%M:%S")) /
                                        (datetime.strptime(data[column]['max'], "%Y-%m-%d %H:%M:%S") - datetime.strptime(data[column]['min'], "%Y-%m-%d %H:%M:%S"))
                                     )
            else:
                working_df[column] = (df[column] - data[column]['min']) / (data[column]['max'] - data[column]['min'])
    else:
        pass

    if len(unNormalizeParams) > 0:
        with open(NORMALISED_DATA_PATH, 'w') as outfile:
            json.dump(unNormalizeParams, outfile)

    return working_df


def unnormalised(val):
    f = open(NORMALISED_DATA_PATH)
    data = json.load(f)
    f.close()

    # X = normX * (maxX - minX) + minX
    unNormalisedVal = val * (data['close']['max'] - data['close']['min']) + data['close']['min']
    return round2(unNormalisedVal, 5)
