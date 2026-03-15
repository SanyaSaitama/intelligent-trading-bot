import sqlite3

conn = sqlite3.connect('moex_quotes.db')
c = conn.cursor()
tables = ['exchanges','markets','securities','security_metadata','quotes','orderbook_snapshots','orderbook_levels']
for t in tables:
    try:
        c.execute(f'SELECT count(*) FROM {t}')
        print(t, c.fetchone()[0])
    except Exception as e:
        print(t, 'ERROR', e)
conn.close()
