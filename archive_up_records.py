import sys, os
import time, datetime
import pandas as pd

from threading import Timer

g_uprecords_dir = "../Apic"
g_uprecords_out = "../A"

def write_log(s, verbose=True):
    if verbose: print(s)
    with open('archive.log', 'a') as f:
        print(s, file=f)

def parse_datetime(row, column, fmt='%Y-%m-%d %H:%M:%S'):
    dt = row[column]
    if isinstance(row[column], str):
        dt = time.mktime(time.strptime(row[column].strip(), fmt))
    return dt

def archive_worker():
    agg_df = None

    today = datetime.date.today()
    first_day = datetime.date(year=today.year, month=today.month, day=1)
    month = today.month
    for filename in os.listdir(g_uprecords_dir):
        basename, extname = os.path.splitext(filename)
        if extname.lower() != ".csv": continue

        try:
            dt = time.strptime(basename, "%m-%d %H")
        except ValueError:
            continue

        if dt.tm_mon < month: continue

        try:
            df = pd.read_csv(os.path.join(g_uprecords_dir, filename))
            df["uid"] = df.apply(lambda row: int(str(row["uid"]).strip()), axis=1)

            if agg_df is None:
                agg_df = df
            else:
                agg_df = pd.concat((agg_df, df), axis=0)
        except Exception as e:
            write_log('Failed to process %s' % filename)
            write_log(e)
            continue

        write_log('Successfully process %s' % filename)

    if not os.path.exists(g_uprecords_out):
        os.mkdir(g_uprecords_out)

    if agg_df is not None:
        for uid, section in agg_df.groupby("uid"):
            file_path = os.path.join(g_uprecords_out, str(uid)+'.csv')
            if os.path.exists(file_path):
                write_log("Loading alread existing file: %s" % file_path)

                df = pd.read_csv(file_path)
                timestamp = time.mktime(first_day.timetuple())
                mask = df.apply(parse_datetime, axis=1, args=('Time',))
                df = df.loc[mask < timestamp]
                df = pd.concat((df, section), axis=0)
                df.drop_duplicates(subset="Time")
            else:
                df = section
            write_log("Saving file: %s" % file_path)
            df.to_csv(file_path, index=False)

g_invoke_first = False

def on_timeout():
    global g_invoke_first
    if g_invoke_first:
        archive_worker()
    g_invoke_first = True

    now = datetime.datetime.now()
    start_hour = 3
    if now.timetuple().tm_hour > start_hour:
        tomorrow = now + datetime.timedelta(days=1)
        next_time = tomorrow.replace(hour=start_hour, minute=30, second=0)
    else:
        next_time = now.replace(hour=start_hour, minute=0, second=0)
    interval = (next_time-now).seconds
    write_log("Next execution %d seconds later" % interval)

    timer = Timer(interval, on_timeout)
    timer.start()


if __name__ == "__main__":
    # Run immediately first
    if len(sys.argv) >= 2 and sys.argv[1] == '--now':
        g_invoke_first = True

    on_timeout()

