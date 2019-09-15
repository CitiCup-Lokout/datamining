import os
import time
import pandas as pd

from threading import Timer

g_uprecords_dir = "../Apic"
g_uprecords_out = "../A"

def write_log(s, verbose=False):
    if verbose: print(s)
    with open('archive.log', 'a') as f:
        print(s, file=f)

def archive_worker():
    agg_df = None
    for filename in os.listdir(g_uprecords_dir):
        basename, extname = os.path.splitext(filename)
        if extname.lower() != ".csv": continue

        try:
            datetime = time.strptime(basename, "%m-%d %H")
        except ValueError:
            continue

        try:
            df = pd.read_csv(os.path.join(g_uprecords_dir, filename))
            if agg_df is None:
                agg_df = df
            else:
                agg_df = pd.concat((agg_df, df), axis=0)
        except:
            write_log('Failed to process %s' % filename)
            continue

    if not os.path.exists(g_uprecords_out):
        os.mkdir(g_uprecords_out)

    if agg_df is not None:
        for uid, section in agg_df.groupby("uid"):
            file_path = os.path.join(g_uprecords_out, str(uid)+'.csv')
            write_log("Saving file: %s" % file_path)
            section.to_csv(file_path, index=False)

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


if __name__ == "__main__":
    # Run immediately first
    on_timeout()

