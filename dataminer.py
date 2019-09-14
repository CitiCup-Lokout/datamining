import os, shutil
import math
import time
import datetime
import numpy as np
import pandas as pd
import requests
import json
from threading import Timer

def write_log(s, verbose=False):
    if verbose: print(s)
    with open('dataminer.log', 'a') as f:
        print(s, file=f)

def parse_datetime(row, column, fmt='%Y-%m-%d %H:%M:%S'):
    if isinstance(row[column], str):
        row[column] = time.mktime(time.strptime(row[column].strip(), fmt))
    return row

def parse_int(row, column):
    row[column] = int(row[column])
    return row

def parse_float(row, column):
    row[column] = float(row[column])
    return row

def fixMergedVideoData(json_buf):
    replacements = (
        ('Aid', 'AVNum'), ('Name', 'Topic'), ('Time', 'UploadTime')
        , ('Danmaku', 'DMNum'), ('DMnum', 'DMNum'), ('reply', 'Comment')
        , ('favorite', 'Save'), ('coin', 'Coin'), ('like', 'Like')
    )
    for origin, to in replacements:
        json_buf = json_buf.replace('"%s":' % origin, '"%s":' % to)
    return json_buf


def read_historical_json(path):
    with open(path, 'r', encoding="utf-8") as f:
        json_buf = f.read()
        json_buf = fixMergedVideoData(json_buf)
        lines = [line.strip(' \n\r\t[],') for line in json_buf.split('\n')]
        json_buf = ','.join([line for line in lines if len(line) != 0])
        json_buf = json_buf.strip(' \t\n\r,')
        json_buf = '[' + json_buf + ']'
        # with open('test.json', 'w') as o:
        #     o.write(json_buf)
        df = pd.read_json(json_buf, orient='records', encoding='utf-8')

        casts = (
            ('AVNum', parse_int), ('UploadTime', parse_datetime)
            , ('DMNum', parse_float), ('Comment', parse_float)
            , ('Save', parse_float), ('Coin', parse_float)
            , ('Like', parse_float)
        )
        for col, parser in casts:
            df = df.apply(parser, axis=1, args=(col,))
        return df

class MiningManager:
    def __init__(self, df):
        self.df = df
        self.iter = 0

    def report(self):
        self.iter += 1
        if self.iter % 150 == 0:         
            write_log("Current progress %d/%d" % (self.iter, self.df.shape[0]))

def mining_worker():
    df = pd.read_json('../Apic/a.json', orient='records', encoding='utf-8')
    df.drop('Time', axis=1, inplace=True)

    mm = MiningManager(df)

    if os.path.exists('a.json'):
        now = datetime.datetime.now().timetuple()
        dstname = time.strftime("%Y-%m-%d-%H-%M-%S.json")
        shutil.copyfile('a.json', dstname)

    df = df.apply(compute_index, axis=1, args=(mm,))
    s = df.to_json(orient='records')
    with open('a.json', 'w', encoding='utf-8') as f:
        f.write(s)
    # with open('debug.log', 'a', encoding='utf-8') as f:
    #     print(df.dtypes, file=f)
    #     print(df.columns, file=f)
    #     print(df.head().to_string(), file=f)
    now = datetime.datetime.now().timetuple()
    write_log("Finished mining at %s" % time.strftime("%Y-%m-%d %H:%M:%S", now))

def compute_index(info, mm):
    uid = info['uid']
    write_log("Computing uid: %d" % uid)

    mm.report()

    try:
        #######################################################################

        csv_path = os.path.join('../A', str(uid)+'.csv')

        df = pd.read_csv(csv_path)
        df = df.apply(parse_datetime, axis=1, args=('Time',))
        df.sort_values('Time', axis=0, inplace=True, ascending=True)

        today = datetime.date.today()
        first_day = datetime.date(year=today.year, month=today.month, day=1)
        timestamp = time.mktime(first_day.timetuple())
        this_month = df.loc[df['Time'] > timestamp, :]
        if not this_month.empty:
            first_day_row = this_month.iloc[0, :]
            this_day_row = this_month.iloc[-1, :]

            info['ViewsFirstDayInMonth'] = first_day_row['PlayNum']
            info['ViewsMonthly'] = this_day_row['PlayNum'] - first_day_row['PlayNum']
            info['ChargesMonthly'] = this_day_row['ChargeNum']
            info['ChargeNum'] = info['ChargesMonthly']

        today = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)
        timestamp = time.mktime(week_ago.timetuple())
        this_week = df.loc[df['Time'] > timestamp, :]
        if not this_week.empty:
            week_ago_row = this_week.iloc[0, :]
            this_day_row = this_week.iloc[-1, :]

            info['ViewsWeekAgo'] = week_ago_row['PlayNum']
            info['ViewsNow'] = this_day_row['PlayNum']
            info['ViewsWeekly'] = info['ViewsNow'] - info['ViewsWeekAgo']
            info['PlayNum'] = info['ViewsNow']

            info['FansWeekAgo'] = week_ago_row['FanNum']
            info['FansNow'] = this_day_row['FanNum']
            info['FanIncWeekly'] = info['FansNow'] - info['FansWeekAgo']
            info['FanNum'] = info['FansNow']

        #######################################################################

        json_path = os.path.join('../HistoricalRecords', str(uid)+'.json')
        df = read_historical_json(json_path)
        df.sort_values('UploadTime', axis=0, inplace=True, ascending=True)

        today = datetime.date.today()
        month_ago = today - datetime.timedelta(days=30)
        timestamp = time.mktime(month_ago.timetuple())
        info['RecentSince'] = timestamp
        info['RecentCount'] = df.loc[df['UploadTime'] >= timestamp, :].shape[0]

        # Last 10 videos
        df = df.tail(n=10)
        df['Score'] = df.apply(lambda v: v['Like'] + 3 *
                                v['Coin'] + 5*v['Save'], axis=1)
        if 'View' not in df.columns: df['View'] = 0
        info['AvgView'] = df['View'].mean()
        info['AvgScore'] = df['Score'].mean()
        info['AvgQuality'] = info['AvgView'] + info['AvgScore']
        info['AvgDuration'] = df['Duration'].mean()
        info['TotalCount'] = df.shape[0]

        first_day = datetime.datetime.fromtimestamp(df['UploadTime'].min())
        last_day = datetime.datetime.fromtimestamp(df['UploadTime'].max())
        days_escaped = (last_day-first_day).days
        info['Frequency'] = days_escaped / info['TotalCount']

        #######################################################################
        '''
        ((50% * 88% * 5/1000 * B * S * 16 + 85% * 12% * 15/1000 * K * B * S * 16)/2）* 3.49 +  K * N * ln(N)/2
        s=30-7天发布视频数
        B=平均视频播放量
        K= （两次根号下月充电/播放比）*10
        N=粉丝数*根号下[(平均视频播放量/粉丝量）*2*（平均赞赏/平均播放比)]

        (50% * 88% * 5/1000 * B * S * 16 + 85% * 12% * 15/1000 * K * B * S * 16 )
        这个是频道每年收入

        平均视频收入是
        (50% * 88% * 5/1000 * B * S * 16 + 85% * 12% * 15/1000 * K * B * S * 16) / (s*16）
        '''

        try:
            L4, L3, O4, N4, P4 = info['ViewsNow'], info['ViewsWeekAgo'], info['AvgView'], info['AvgQuality'], info['RecentCount']
            info['WorkIndex'] = ((N4**1.1 + (L4-L3)/10) * (N4-O4) / O4 * P4 / 30) ** 0.65 / 10
        except:
            info['WorkIndex'] = float('Nan')

        K3, K4 = info['FansWeekAgo'], info['FansNow']
        try:
            info['FanIncPercentage'] = (K4 - K3) / K3
        except:
            info['FanIncPercentage'] = float('Nan')
        try:
            info['FanIncIndex'] = ((K4 - K3) * info['FanIncPercentage']) ** 0.75 * (1 if K4 > K3 else -1)
        except:
            info['FanIncIndex'] = float('Nan')

        try:
            S3, R4, K4, L4, L3 = info['WorkIndex'], info['FanIncIndex'], info['FansNow'], info['ViewsNow'], info['ViewsWeekAgo']
            info['SummaryIndex'] = (S3+R4)*(K4/1000+(L4-L3)/10000) ** 0.7 / 1000
        except:
            info['SummaryIndex'] = float('Nan')

        try:
            B = info['AvgView']
            S = info['RecentCount']
            K = info['ChargesMonthly']/info['ViewsMonthly'] * 10
            N = info['FanNum'] * (info['AvgView']/info['FanNum'] * 2 * (info['AvgScore']/info['AvgView'])) ** 0.5
            # info['IncomeYearly'] = info['AvgView']*0.003*12 * info['RecentCount']/1000 \
            #     + info['SummaryIndex']*400/3000 \
            #     + info['FanNum']**0.85 * info['RecentCount']**0.7*36 / 300000
            info['IncomeYearly'] = (0.5 * 0.88 * 5/1000 * B * S * 16 + 0.85 * 0.12 * 15/1000 * K * B * S * 16)
        except:
            info['IncomeYearly'] = float('Nan')

        try:
            info['IncomePerVideo'] = info['IncomeYearly'] / (info['RecentCount']*16)
        except:
            info['IncomePerVideo'] = 0.0

        try:
            X = info['IncomeYearly']
            K = info['ChargesMonthly']/info['ViewsMonthly'] * 10
            N = info['FanNum'] * (info['AvgView']/info['FanNum'] * 2 * (info['AvgScore']/info['AvgView'])) ** 0.5
            # info['ChannelValue'] = X + X / (1.1**1) + X/(1.1**2) + X/(1.1**3) + X/(1.1**4)
            info['ChannelValue'] = (X/2) * 3.49 + K * N * math.log(N) / 2
        except:
            info['ChannelValue'] = 0.0

        #######################################################################

        try:
            url = 'https://api.bilibili.com/x/space/acc/info?mid=%s' % str(uid)
            info['Face'] = requests.get(url).json()['data']['face']
        except:
            info['Face'] = 'Not Found'
    except:
        write_log("Failed to compute: %d" % uid)
        pass

    return info

g_current_times = 0
g_timeout_interval = 5

def on_timeout():
    global g_current_times, g_timeout_interval
    g_current_times -= 1
    if g_current_times <= 0:
        mining_worker()
        g_current_times = 12
    write_log("Next execution %s mins later" % g_timeout_interval*g_current_times)
    timer = Timer(g_timeout_interval * 60, on_timeout)
    timer.start()

if __name__ == "__main__":
    # Run immediately first
    on_timeout()
