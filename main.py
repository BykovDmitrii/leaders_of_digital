import pandas as pd
import numpy as np
import os
from datetime import datetime
from datetime import date, timedelta

def hour_range(start_datetime, end_datetime):
    current_datetime = start_datetime
    while current_datetime < end_datetime:
        yield current_datetime
        current_datetime += timedelta(hours=1)


#возвращает для заданного множества билбордов их слоты в формте
# {bil_name: {datetime: ots}}
def predictor_ots(otses, bilboard_names, start_date, end_date, available_hours):
    predictor_forecast = { bil_name: {i: otses[bil_name][i.hour]
                                      for i in hour_range(start_date, end_date)
                                      if i.hour in available_hours}
                           for bil_name in bilboard_names}
    return predictor_forecast

#возвращает для заданного множества билбордов их слоты в формте
# {bil_name: {datetime: frequency}}
def get_free_frequences(free_bilboards, bilboards_names, start_date, end_date):
    free_frequences = {
             bil_name : {
                 datetime(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]), i):
                 free_bilboards[(free_bilboards['ID экрана'] == bil_name) & (free_bilboards['Дата']==date)][i].values[0]
                         for date in free_bilboards[free_bilboards['ID экрана'].isin(bilboards_names)]["Дата"]
                         for i in range(24)
                        if datetime(*[int(d) for d in date.split('-')]) >= start_date and\
                            datetime(*[int(d) for d in date.split('-')]) <= end_date

             }
             for bil_name in bilboards_names
    }
    return free_frequences




#-------------------------------------------------------------------------------
# Flask

import os
from flask import Flask, render_template, request, redirect, url_for, abort, \
    send_from_directory

from get_ots import get_ots
from scheduler import get_plan
import json



app = Flask(__name__, template_folder='.')
global_vars = {}
@app.route('/')
def main_index():
    return render_template('main_index.html')

@app.route('/set')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def set_params():
    global global_vars
    print("Form:", request.form)
    global_vars['row_data_path'] = request.form.get("parquete")
    global_vars['time-series-path'] = request.form.get("time-series")
    global_vars['bilboard_info_file'] = request.form.get('bilboard_info')
    global_vars['inventory'] = request.form.get('inventory')

    return redirect('/set')

@app.route('/set', methods=['POST'])
def upload_files():
    uploaded_file = request.files['file']
    json_data = json.load(uploaded_file)
    val_dict = dict(
                    bilboards=json_data['bilboards'],
                    ots=int(json_data['ots']),
                    start_date=json_data['time period'][0],
                    end_date=json_data['time period'][1],
                    start_time=json_data['time'][0],
                    end_time=json_data['time'][1]
                    )


    plan = compute_function(val_dict, global_vars)
    plan_json = json.dumps({billboard: {str(ts.isoformat()): v for ts, v in plan[billboard].items()} for billboard in plan})

    return render_template('returned.html', **val_dict, jsonfile=plan_json)


@app.route('/uploads/<filename>')
def upload(filename):
    return send_from_directory(app.config['UPLOAD_PATH'], filename)


# основная вычислительная функция, где происходит вызов планировщика и
# предиктора
def compute_function(user_params, file_params):

    bilboards_names = user_params['bilboards']
    start_date = datetime(*[int(t) for t in user_params['start_date'].split('-')])
    end_date = datetime(*[int(t) for t in user_params['end_date'].split('-')])


    start_time, end_time = user_params['start_time'], user_params['end_time']

    available_hours = [a for a in range(int(start_time.split(':')[0]),
                                      int(end_time.split(':')[0]))]

    print("File params:", file_params['bilboard_info_file'])
    dfp = pd.read_csv(
                        file_params['bilboard_info_file'],
                        sep=";", index_col=False
                    )

    inventory = pd.read_excel(file_params['inventory'])

    otses = {bilboard_name: get_ots(
                    bilboard_name, start_date,
                    end_date, dfp, file_params['time-series-path']
                    )

            for bilboard_name  in bilboards_names}


    limits = predictor_ots(otses, user_params['bilboards'], start_date,
                            end_date, available_hours)

    eots = get_free_frequences(inventory, bilboards_names, start_date, end_date)

    plan = get_plan(user_params['ots'], eots, limits)

    return plan

if __name__ == "__main__":
    gloabal_vars = {}
    app.run(debug=False, use_reloader=False)
