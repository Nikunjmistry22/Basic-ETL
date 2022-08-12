import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=nikunj22")
cur = conn.cursor()


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files

song_files = get_files('data/song_data')
filepath = song_files[0]
df = pd.read_json(filepath, lines=True)
print(df.head())

#Song Data
song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
print(song_data)

cur.execute(song_table_insert, song_data)
conn.commit()