import pandas as pd
# !pip install fastparquet
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA

import datetime
from datetime import timedelta
from datetime import datetime
import os
import re
from glob import glob

data_path = "C:\\Downloads\\gallery_data\\RowData"
ts_subdir = "C:\\Downloads\\gallery_hack"

# data_path = "gallery_data/RowData"
# ts_subdir = "."

filename_pl = os.path.join(data_path, "..", "player_details.csv")
dfp = pd.read_csv(filename_pl, sep=";", index_col=False)

player_num = "NVS036APL"
player = dfp[dfp.PlayerNumber == player_num].PlayerId.iloc[0]

# filename_inv = os.path.join(data_path, "..", "inventory.xlsx")
# filename_adm = os.path.join(data_path, "..", "admetrix_data.xlsx")
# dfi = pd.read_excel(filename_inv)
# dfa = pd.read_excel(filename_adm)

# init
month_now_str = '2021-9'
year_now, month_now = list(map(int, month_now_str.split('-')))
players_months = dict()
for s in glob(os.path.join(data_path, "crowd", "player=*")):
    months = []
    player = int(s[s.find("player=")+len("player="):])
    if player == 40:
        continue
    print(player)
    for ss in glob(os.path.join(data_path, "crowd", f"player={player}", "month=*")):
        r = re.match(r".*month=(.*)\.parquet", ss)
        if r is not None:
            month_str = r.groups()[0]
            year, month = list(map(int, month_str.split('-')))
            months.append((year, month))
    months = sorted(months)
    print(player, months)
    players_months[player] = months

# n_months = 8
# generate time series for multiple months
for player in players_months.keys():
    t1 = datetime.now()
    dfc_sers = []
    print(player)
    # player = 257
    player_num = dfp[dfp.PlayerId == player].PlayerNumber.iloc[0]
    player_ = dfp[dfp.PlayerNumber == player_num].PlayerId.iloc[0]
    assert(player_ == player)
    player = player_

    out_filename = os.path.join(ts_subdir, f"player_{player}_ts.csv")
    if os.path.isfile(out_filename):
        print(f"skipping player {player}")
        continue

    months = players_months[player]
    months_sel = months
    # months_sel = sorted([m for m in months if m < (year_now, month_now)])

    # n_months_slice = -n_months if n_months is not None else None
    # months_sel = months_sel[n_months_slice:]
    for month in months_sel:
        print(month)
        filename_crowd = os.path.join(data_path, "crowd", f"player={player}", f"month={month[0]}-{month[1]}.parquet")
    #     print(filename_crowd, os.path.isfile(filename_crowd))
        assert(os.path.isfile(filename_crowd))
        dfc = pd.read_parquet(filename_crowd)

        dfc["AddedOnTick"] = dfc["AddedOnTick"] // 1000
        dfc["AddedOnHour"] = dfc["AddedOnTick"] - (dfc["AddedOnTick"] % 3600)
        dfc["AddedOnHour"] = pd.to_datetime(dfc["AddedOnHour"], unit='s')
        dfc["AddedOnTick"] = pd.to_datetime(dfc["AddedOnTick"], unit='s')

        dfc_g = dfc[["AddedOnHour", "Mac"]].groupby("AddedOnHour").count()
        dfc_ser = pd.Series(dfc_g.Mac, index=dfc_g.index)
        dfc_sers.append(dfc_ser)
        del dfc
    t2 = datetime.now()
    print((t2-t1).total_seconds())
    for d in dfc_sers:
        print("\t", d.index[0], d.index[-1])
    dfc_ser_full = pd.concat(dfc_sers)
    dfs = dfc_ser_full.asfreq('h').dropna()
    print("done concat:\t\t", len(dfs), dfs.index[0], dfs.index[-1])
    dfs.to_csv(out_filename)
