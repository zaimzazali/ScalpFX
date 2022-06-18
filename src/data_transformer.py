from round2 import round2
import ta as ta
import json
from datetime import datetime



SUPER_FAST = 9
FAST = 12
SLOW = 26
NORMALISED_DATA_PATH = '/models/unnormalized_parameters.json'



def calcFeaturesEngineering(df):
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

    return working_df


def calcTA(df):
    working_df = df.copy()

    # Momentum
    working_df['Mom_AO'] = ta.momentum.AwesomeOscillatorIndicator(working_df['high'], working_df['low'], window1=FAST, window2=SLOW)
    working_df['Mom_PPO'] = ta.momentum.PercentagePriceOscillator(working_df['close'], window_slow=SLOW, window_fast=FAST, window_sign=SUPER_FAST)
    working_df['Mom_PVO'] = ta.momentum.PercentageVolumeOscillator(working_df['volume'], window_slow=SLOW, window_fast=FAST, window_sign=SUPER_FAST)
    working_df['Mom_ROC'] = ta.momentum.ROCIndicator(working_df['close'], window=FAST)
    working_df['Mom_RSI'] = ta.momentum.RSIIndicator(working_df['close'], window=FAST)
    working_df['Mom_SRSI'] = ta.momentum.StochRSIIndicator(working_df['close'], window=FAST, smooth1=3, smooth2=3)
    working_df['Mom_SO'] = ta.momentum.StochasticOscillator(working_df['close'], working_df['high'], working_df['low'], window=FAST, smooth_window=3)
    working_df['Mom_TSI'] = ta.momentum.TSIIndicator(working_df['close'], window_slow=SLOW, window_fast=FAST)
    working_df['Mom_UO'] = ta.momentum.UltimateOscillator(working_df['high'], working_df['low'], working_df['close'], window1=SUPER_FAST, window2=FAST, window3=SLOW)

    # Volume
    working_df['Vol_ADI'] = ta.volume.AccDistIndexIndicator(working_df['high'], working_df['low'], working_df['close'], working_df['volume'])
    working_df['Vol_CMF'] = ta.volume.ChaikinMoneyFlowIndicator(working_df['high'], working_df['low'], working_df['close'], working_df['volume'], window=FAST)
    working_df['Vol_EoM'] = ta.volume.EaseOfMovementIndicator(working_df['high'], working_df['low'], working_df['volume'], window=FAST)
    working_df['Vol_FI'] = ta.volume.ForceIndexIndicator(working_df['close'], working_df['volume'], window=FAST)
    working_df['Vol_MFI'] = ta.volume.MFIIndicator(working_df['high'], working_df['low'], working_df['close'], working_df['volume'], window=FAST)
    working_df['Vol_NVI'] = ta.volume.NegativeVolumeIndexIndicator(working_df['close'], working_df['volume'])
    working_df['Vol_OBV'] = ta.volume.OnBalanceVolumeIndicator(working_df['close'], working_df['volume'])
    working_df['Vol_VPT'] = ta.volume.VolumePriceTrendIndicator(working_df['close'], working_df['volume'])
    working_df['Vol_VWAP'] = ta.volume.VolumeWeightedAveragePrice(working_df['high'], working_df['low'], working_df['close'], working_df['volume'], window=FAST) # Good for day-trading

    # Volatility
    working_df['Vola_ATR'] = ta.volatility.AverageTrueRange(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Vola_BB'] = ta.volatility.BollingerBands(working_df['close'], window=FAST)
    working_df['Vola_DC'] = ta.volatility.DonchianChannel(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Vola_KC'] = ta.volatility.KeltnerChannel(working_df['high'], working_df['low'], working_df['close'], window=FAST, window_atr=FAST, original_version=False)

    # Trend
    working_df['Trend_ADX'] = ta.trend.ADXIndicator(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Trend_AI'] = ta.trend.AroonIndicator(working_df['close'], window=FAST)
    working_df['Trend_CCI'] = ta.trend.CCIIndicator(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Trend_DPO'] = ta.trend.DPOIndicator(working_df['close'], window=FAST)
    working_df['Trend_EMA_12'] = ta.trend.EMAIndicator(working_df['close'], window=FAST)
    working_df['Trend_EMA_50'] = ta.trend.EMAIndicator(working_df['close'], window=50)
    working_df['Trend_EMA_100'] = ta.trend.EMAIndicator(working_df['close'], window=100)
    working_df['Trend_Ichi'] = ta.trend.IchimokuIndicator(working_df['high'], working_df['low'], window1=SUPER_FAST, window2=FAST, window3=SLOW)
    working_df['Trend_MACD'] = ta.trend.MACD(working_df['close'], window_fast=FAST, window_slow=SLOW, window_sign=SUPER_FAST)
    working_df['Trend_MI'] = ta.trend.MassIndex(working_df['high'], working_df['low'], window_fast=FAST, window_slow=SLOW)
    working_df['Trend_PSAR'] = ta.trend.PSARIndicator(working_df['high'], working_df['low'], working_df['close'])
    working_df['Trend_SMA_12'] = ta.trend.EMAIndicator(working_df['close'], window=FAST)
    working_df['Trend_SMA_50'] = ta.trend.EMAIndicator(working_df['close'], window=50)
    working_df['Trend_SMA_100'] = ta.trend.EMAIndicator(working_df['close'], window=100)
    working_df['Trend_STC'] = ta.trend.STCIndicator(working_df['close'], window_fast=FAST, window_slow=SLOW)
    working_df['Trend_TRIX'] = ta.trend.TRIXIndicator(working_df['close'], window=FAST)
    working_df['Trend_VI'] = ta.trend.VortexIndicator(working_df['high'], working_df['low'], working_df['close'], window=FAST)
    working_df['Trend_WMA'] = ta.trend.WMAIndicator(working_df['close'], window=FAST)

    return working_df


def normalised(df, mode):
    working_df = df.copy()

    # Doing the Min-Max Normalization
    # normX = (X - minX) / (maxX - minX)

    unNormalizeParams = {}

    if mode == 'train':
        for column in df.columns:
            working_df[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
            unNormalizeParams[column] = {'min':df[column].min(), 'max':df[column].max()}
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
