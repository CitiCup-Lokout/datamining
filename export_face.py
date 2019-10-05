import sys, os, shutil
import math
import time
import datetime
import numpy as np
import pandas as pd
import requests
import json

if __name__ == '__main__':
    if len(sys.argv) < 2:
        exit()

    df = pd.read_json(sys.argv[1], orient='records', encoding='utf-8')
    df = df.loc[:, ['uid', 'Face']]
    s = df.to_json(orient='records')
    with open('face.json', 'w', encoding='utf-8') as f:
        f.write(s)
    print("Finished exporting.")
    