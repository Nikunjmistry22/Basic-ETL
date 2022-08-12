import psycopg2


conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=nikunj22")
cur = conn.cursor()

def song_detail():
    cur.execute('select count(*) from songs ;')
    ans=cur.fetchall()
    for i in ans:
        print(i)
    cur.close()
if __name__== "__main__":
    song_detail()