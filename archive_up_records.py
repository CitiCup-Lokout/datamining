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

def archive_worker():
    agg_df = None
    month = 10
    for filename in os.listdir(g_uprecords_dir):
        basename, extname = os.path.splitext(filename)
        if extname.lower() != ".csv": continue

        try:
            datetime = time.strptime(basename, "%m-%d %H")
        except ValueError:
            continue

        if datetime.tm_mon < month: continue

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
                df = pd.concat((df, section), axis=0)
                df.drop_duplicates(subset="Time")
            else:
                df = section
            write_log("Saving file: %s" % file_path)
            df.to_csv(file_path, index=False)

"""
g_current_times = 0
g_timeout_interval = 5

def on_timeout():
    global g_current_times, g_timeout_interval
    g_current_times -= 1
    if g_current_times <= 0:
        archive_worker()
        g_current_times = 12
    write_log("Next execution %s mins later" % g_timeout_interval*g_current_times)
    timer = Timer(g_timeout_interval * 60, on_timeout)
    timer.start()
"""

if __name__ == "__main__":
    # Run immediately first
    if len(sys.argv) >= 2 and sys.argv[1] == '--now':
        archive_worker()
    now = datetime.datetime.now()
    start_hour = 4
    if now.timetuple().tm_hour > start_hour:
        tomorrow = now + datetime.timedelta(days=1)
        next_time = tomorrow.replace(hour=start_hour, minute=0, second=0)
    else:
        next_time = now.replace(hour=start_hour, minute=0, second=0)
    interval = (next_time-now).seconds
    write_log("Next execution %d seconds later" % interval)

    timer = Timer(interval, archive_worker)
    timer.start()

