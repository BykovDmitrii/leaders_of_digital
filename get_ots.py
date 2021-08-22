import pandas as pd
# !pip install fastparquet
import numpy as np
# import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA

import datetime
from datetime import timedelta
from datetime import datetime
import os
import re

# data_path = "C:\\Downloads\\gallery_data\\RowData"
# timeseries_path = "C:\\Downloads\\gallery_hack"
'''
data_path = "gallery_data/RowData" # CHANGE FOR YOUR CASE
timeseries_path = "." # path with my csv timeseries, CHANGE FOR YOUR CASE

filename_pl = os.path.join(data_path, "..", "player_details.csv")
dfp = pd.read_csv(filename_pl, sep=";", index_col=False)
'''
# for example, "257" -> "NVS036APL"
def player_id_to_num(player_id):
    return dfp[dfp.PlayerId == player_id].PlayerNumber.iloc[0]

# format: player_text_identifier, [dt1, dt2_incl]
# player is player text identifier
# dt1 is start day in format of datetime
# dt2 is end day INCLUDING in format of datetime
def get_ots(player_num, dt1, dt2_incl, dfp, timeseries_path, train_len=30*6, order=(30, 0, 0)):

    player = dfp[dfp.PlayerNumber == player_num].PlayerId.iloc[0]
    dt2 = dt2_incl + timedelta(days=1)
    # using: [dt1, dt2)

    n_days = (dt2-dt1).days

    dt1_wd = datetime.weekday(dt1)
    dt2_wd = datetime.weekday(dt2)
    dt2_wd_incl = datetime.weekday(dt2_incl)

    period = 24*7
    dfs = pd.read_csv(os.path.join(timeseries_path, f"player_{player}_ts.csv"), index_col=0, squeeze=True, parse_dates=True)

    # fill missing values in periodical index grid
    dfs = dfs.asfreq('h', method='pad')

    result = seasonal_decompose(dfs, model='multiplicative', period=period)

    trend = result.trend
    seasonal = result.seasonal
    na_index = trend.loc[pd.isna(trend)].index

    trend = trend.drop(index=na_index)
    seasonal = seasonal.drop(index=na_index)

    seasonal_period = np.array(seasonal)[:period]
    seasonal_clean = np.tile(seasonal_period, -(-len(trend.index)//len(seasonal_period)))[:len(trend.index)]
    seasonal_clean = pd.Series(seasonal_clean, index=trend.index)

    trend_d = trend.asfreq('d')

    split_point = trend.index[trend.index < dt1][-1]
    split_point -= timedelta(hours=split_point.hour)

    trend_train = trend_d[trend_d.index < split_point]
#     trend_test = trend_d[trend_d.index >= split_point]
    trend_train_trim = trend_train[-train_len:]
    print(f"used data from {trend_train_trim.index[0]} to {trend_train_trim.index[-1]} for training")

    tt_border = split_point

    model = ARIMA(trend_train_trim, order=order)
    model_fit = model.fit()

    n_steps = n_days

    dt2_wd_got = datetime.weekday(split_point + timedelta(days=n_days))

    n_steps_extra = (dt2_wd - dt2_wd_got) % 7
    n_steps = n_days + n_steps_extra

    assert(datetime.weekday(split_point + timedelta(days=n_steps)) == dt2_wd)
    output = model_fit.forecast(steps=n_steps+1)

    trend_predict = np.concatenate((trend_train, output))

    index_extend = pd.date_range(start=trend_train.index[0], end=trend_train.index[0]+timedelta(days=1)*(len(trend_predict)-1), freq='d')
    trend_predict_resample = pd.Series(trend_predict, index=index_extend)
    trend_predict_resample = trend_predict_resample.asfreq('h', method='pad')

    seasonal_period = seasonal.iloc[:period]
    seasonal_clean = np.tile(seasonal_period, -(-len(trend_predict_resample.index)//len(seasonal_period)))[:len(trend_predict_resample.index)]
    seasonal_clean = pd.Series(seasonal_clean, index=trend_predict_resample.index)

    predict_full = trend_predict_resample*seasonal_clean
    tt_border_2 = tt_border + timedelta(days=n_steps_extra)
    predict_ret = predict_full[(predict_full.index >= tt_border_2) & (predict_full.index < tt_border_2 + (dt2-dt1))]
    assert(dt1_wd == datetime.weekday(predict_ret.index[0]))
    assert(dt2_wd_incl == datetime.weekday(predict_ret.index[-1]))
    assert(len(predict_ret) == 24*n_days)
    assert((dt1 - tt_border_2).total_seconds() % (3600*24*7) == 0)
    predict_ret_shift = pd.Series(np.array(predict_ret), index=predict_ret.index + (dt1 - tt_border_2))
    return predict_ret_shift

if __name__ == "__main__":
    # example of input data

    # player_num = player_id_to_num(257) # another way to set player: by id in folder name
    player_num = "NVS036APL"
    t1 = datetime.now()
    # from 6th to 23th september INCLUDING 23th
    ots = get_ots(player_num, datetime(2021, 9, 6), datetime(2021, 9, 23))
    t2 = datetime.now()
    print("time:", (t2-t1).total_seconds(), "s")
    print(ots)
    # ots.plot()
    # plt.show()
