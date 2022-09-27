import sqlite3
import pandas as pd


class DbInterface:
    def __init__(self, db_filepath='db.sqlite3'):
        self.db = db_filepath
        self.conn = None
        self.cur = None
        # Initialize the monitor SQLite database
        self.open_db()
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS job (
                id INTEGER PRIMARY KEY,
                job_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT DEFAULT 'created',
                deleted BOOLEAN DEFAULT 0
            )'''
                         )
        self.close_db()

    def open_db(self):
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()

    def close_db(self):
        self.conn.commit()
        self.conn.close()

    def add_job(self, job_id='', kind=''):
        self.open_db()
        self.cur.execute(f'''
            INSERT INTO job (
                job_id,
                kind
            ) VALUES(
                :job_id,
                :kind
            )
        ''',
        {
            'job_id': job_id,
            'kind': kind,
        })
        self.close_db()

    def update_job(self, job_id, status):
        self.open_db()
        self.cur.execute(f'''
            UPDATE job SET
                status = :status
            WHERE
                job_id = :job_id
        ''',
        {
            'job_id': job_id,
            'status': status,
        })
        self.close_db()
        return self.cur.rowcount == 1

    def get_all_jobs(self, pretty_print=False):
        self.open_db()
        sql = f'''
            SELECT
                *
            FROM
                job
        '''
        params = {}
        if pretty_print:
            results = pd.read_sql_query(sql, self.conn, params=params)
        else:
            self.cur.execute(sql, params)
            results = self.cur.fetchall()
        self.close_db()
        return results

    def get_job(self, job_id='', pretty_print=False):
        self.open_db()
        sql = f'''
            SELECT
                *
            FROM
                job
            WHERE
                job_id = :job_id
        '''
        params = {
            'job_id': job_id,
        }
        if pretty_print:
            results = pd.read_sql_query(sql, self.conn, params=params)
        else:
            self.cur.execute(sql, params)
            results = self.cur.fetchall()
        self.close_db()
        return results

    def delete_job(self, job_id):
        self.open_db()
        self.cur.execute(f'''
            DELETE FROM
                job
            WHERE
                job_id = :job_id
        ''',
        {
            'job_id': job_id,
        })
        self.close_db()
        return self.cur.rowcount == 1
