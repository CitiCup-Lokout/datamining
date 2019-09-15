import os, shutil
import datetime
import time
import math
import sklearn
import pandas as pd
import numpy as np
from threading import Timer

def write_log(s, verbose=False):
    if verbose:
        print(s)
    with open('predictor.log', 'a') as f:
        print(s, file=f)


def parse_datetime(row, column, fmt='%Y-%m-%d %H:%M:%S'):
    dt = row[column]
    if isinstance(row[column], str):
        dt = time.mktime(time.strptime(row[column].strip(), fmt))
    return dt


def parse_int(row, column):
    return int(row[column])


def parse_float(row, column):
    return float(row[column])


def time_transform(row, column, translate, scale):
    return (row[column] - translate) / scale


def time_inv_transform(row, column, translate, scale):
    return row[column] * scale + translate


def cast_dataframe(df, casts):
    for col, parser in casts:
        df[col] = df.apply(parser, axis=1, args=(col,))

def random_walk(X, Y, steps, stride=1, n=1, noise=0.5, keep_origin=False):
    X = X.squeeze()
    derivatives = [Y]
    dX = np.diff(X)
    for i in range(n):
        dY = np.diff(derivatives[-1])
        if i == 0:
            derivatives.append(dY/dX)
        else:
            derivatives.append(dY)
    Y_ = np.random.choice(derivatives.pop(), size=steps)
    Y_ *= np.random.normal(loc=1.0, scale=noise, size=steps)
    for i in range(n):
        last_Y = derivatives.pop()
        Y_ *= stride
        Y_[0] += last_Y[-1]
        Y_ = np.cumsum(Y_)
    X_ = np.arange(steps) * stride + X[-1]
    if keep_origin:
        X = np.hstack((X, X_))
        Y = np.hstack((Y, Y_))
        return X, Y
    else:
        return X_, Y_
    
def predict_worker():
    # if os.path.exists('p.json'):
    #     now = datetime.datetime.now().timetuple()
    #     dstname = time.strftime("%Y-%m-%d-%H-%M-%S.json")
    #     shutil.copyfile('p.json', dstname)

    if not os.path.exists('../P'):
        os.mkdir('../P')

    up_list = pd.read_json('a.json')

    for filename in os.listdir('../A'):
        basename, extname = os.path.splitext(filename)
        if basename.startswith('.'): continue
        if extname.lower() != '.csv': continue

        uid = int(basename)
        up_info = up_list.loc[up_list['uid'] == uid].iloc[0]

        try:
            df = pd.read_csv(os.path.join('../A', filename))

            casts = (
                ('Time', parse_datetime), ('PlayNum', parse_float)
                , ('FanNum', parse_float), ('ChargeNum', parse_float)
            )
            cast_dataframe(df, casts)
            df.sort_values(by='Time', inplace=True, ascending=True)

            today = datetime.date.today()
            month_ago = today - datetime.timedelta(days=30)
            timestamp = time.mktime(month_ago.timetuple())

            df = df.loc[df['Time']>timestamp]
            df['Time'] = df.apply(time_transform, axis=1, args=('Time', timestamp, 60*60))

            fields = ['FanNum', 'PlayNum']
            prediction = None
            for field in fields:
                if field == 'PlayNum':
                    diff = df['PlayNum'].diff()
                    subdf = df.loc[diff.abs() > 1e-8]
                else:
                    subdf = df
                subdf = subdf.dropna(subset=[field])

                X = subdf['Time'].to_numpy()
                Y = subdf[field].to_numpy()

                steps = 24//3 * 7*2
                X_, Y_ = random_walk(X, Y, steps, n=1, stride=3)
                X_ = X_[:, np.newaxis]
                Y_ = Y_[:, np.newaxis]

                if prediction is None:
                    prediction = X_

                prediction = np.hstack((prediction, Y_))
            
            pred_df = pd.DataFrame(data=prediction, columns=['Time', 'FanNum', 'PlayNum'])
            casts = (('FanNum', parse_int), ('PlayNum', parse_int))
            cast_dataframe(pred_df, casts)
            pred_df['Time'] = pred_df.apply(time_inv_transform, axis=1, args=('Time', timestamp, 60*60))

            def predict_channel_value(row, info):
                try:
                    K = (info['ChargesMonthly']/info['ViewsMonthly']) ** 0.5 * 100
                    N = row['FanNum'] * (row['FanNum']/info['AvgView'] * 2 * (info['AvgScore']/info['AvgView'])) ** 0.5
                    X = info['IncomeYearly']
                    channelValue = (X/2) * 3.49 + K * N * math.log(N) / 2
                except:
                    channelValue = float('Nan')
                return channelValue
            pred_df['ChannelValue'] = pred_df.apply(predict_channel_value, axis=1, args=(up_info,))

            s = pred_df.to_json(orient='records')
            path = os.path.join('../P', basename+'.json')
            with open(path, 'w', encoding='utf-8') as f:
                f.write(s)
            now = datetime.datetime.now().timetuple()
            write_log("Finished predicting %s at %s" % (filename, time.strftime("%Y-%m-%d %H:%M:%S", now)), verbose=True)
        except Exception as e:
            write_log("Failed to predict %s" % filename, verbose=True)

g_current_times = 0
g_timeout_interval = 5


def on_timeout():
    global g_current_times, g_timeout_interval
    g_current_times -= 1
    if g_current_times <= 0:
        predict_worker()
        g_current_times = 12
    write_log("Next execution %s mins later" %
              g_timeout_interval*g_current_times)
    timer = Timer(g_timeout_interval * 60, on_timeout)
    timer.start()


if __name__ == "__main__":
    # Run immediately first
    on_timeout()
