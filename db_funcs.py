import pandas as pd
import numpy as np
import os

# exp - expected
# res - result

db_filename = "campaign.db"
# изначальные наполнения для столбца дней (список datetime-ов) и столбца изначального плана (на все дни в dt_list)
def db_init(db_filename, dt_list, ots_exp_list):

    ots_res_list = [-1] * len(dt_list) # незаполненные ots_res равны -1, не получили еще по этим дням реальный ots
    df = pd.DataFrame(data=list(zip(dt_list, ots_exp_list, ots_res_list)), columns=["day", "ots_exp", "ots_res"])
    df.to_csv(db_filename, index=False)

    return df

# dt is day (datetime)
# (dt, ots_res) - value to append in ots_res column (напротив дня dt)
def db_update(db_filename, dt, ots_res):
    df = pd.read_csv(db_filename, index_col=False)

    df["day"] = pd.to_datetime(df["day"])
    df.loc[df.day == dt, "ots_res"] = ots_res
    new_plan_tail = planner(dt, ots_res) # not working now, change this to your func
                                         # it returns ots_exp values for days from (dt+1) to end
    # new_plan_tail = [666,777] # затычка, но длина не обяз такая будет в реале
    new_plan = np.concatenate((np.array(df.ots_exp[:-len(new_plan_tail)]), new_plan_tail))
    df.ots_exp = new_plan

    df.to_csv(db_filename, index=False)
    return df

if __name__ == "__main__":
    # example of usage
    df = db_init(db_filename,
                 [datetime(2021,9,1),
                  datetime(2021,9,2),
                  datetime(2021,9,3),
                  datetime(2021,9,4)],
                [100, 200, 300, 400])
    print(df)

    df = db_update(db_filename, datetime(2021,9,2), 51515)
    print(df)

    df = db_update(db_filename, datetime(2021,9,3), 12121)
    print(df)

    df = db_update(db_filename, datetime(2021,9,4), 6666)
    print(df)
