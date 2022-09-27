import os
import requests
import sqlite3
import json
from db import DbInterface


class DesAccessApi:
    def __init__(self, username='', password='', config={}):
        self.username = username
        self.password = password
        default_config = {
            'base_domain': 'des.ncsa.illinois.edu',
            'scheme': 'https',
            'auth_token': '',
            'api_base_path': 'desaccess/api',
            'files_base_path': 'files-desaccess',
            'database': 'desdr',
            'db_filepath': '/tmp/jobsdb.sqlite3',
        }
        conf = {}
        for key in default_config:
            if key in config:
                conf[key] = config[key]
            else:
                conf[key] = default_config[key]
        print('Active configuration:')
        print(json.dumps(conf, indent=2))
        self.api_base_url = f'''{conf['scheme']}://{conf['base_domain']}/{conf['api_base_path'].strip('/')}'''
        self.files_base_url = f'''{conf['scheme']}://{conf['base_domain']}/{conf['files_base_path'].strip('/')}'''
        self.database = conf['database']
        self.auth_token = conf['auth_token']
        
        self.db = DbInterface(db_filepath=conf['db_filepath'])
        
        self.login()

    def open_db(self):
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()

    def close_db(self):
        self.conn.commit()
        self.conn.close()



    def login(self):
        """Obtains an auth token using the username and password credentials for a given database.
        """
        # Login to obtain an auth token
        r = requests.post(
            '{}/login'.format(self.api_base_url),
            data={
                'username': self.username,
                'password': self.password,
                'database': self.database,
            }
        )
        # Store the JWT auth token
        self.auth_token = r.json()['token']
        return self.auth_token

    def submit_job(self, kind='cutout', release='', positions=''):
        assert kind in ['cutout', 'query']
        if kind == 'cutout':
            data={
                'db': self.database,
                'release': release,
                'positions': positions,
            }
        ## TODO: Implement query job support
        r = requests.put(
            '{}/job/{}'.format(self.api_base_url, kind),
            data=data,
            headers={'Authorization': 'Bearer {}'.format(self.auth_token)}
        )
        response = r.json()
        # print(json.dumps(response, indent=2))

        if response['status'] == 'ok':
            job_id = response['jobid']
            self.db.add_job(job_id=job_id, kind=kind)
            print('Job "{}" submitted.'.format(job_id))
            # Refresh auth token
            self.auth_token = response['new_token']
        else:
            job_id = None
            print('Error submitting job: '.format(response['message']))

        return response

    def get_job_status(self, job_id):
        """Returns the current status of the job identified by the unique job_id."""

        r = requests.post(
            '{}/job/status'.format(self.api_base_url),
            data={
                'job-id': job_id
            },
            headers={'Authorization': 'Bearer {}'.format(self.auth_token)}
        )
        response = r.json()
        # Refresh auth token
        self.auth_token = response['new_token']
        # print(json.dumps(response, indent=2))
        return self.db.update_job(job_id=job_id, status=response['jobs'][0]['job_status'])
        
    def update_unfinished_jobs_status(self):
        jobs = self.db.get_all_jobs(pretty_print=True)
        for index, job in jobs.iterrows():
            # print(job)
            if job['status'] not in ['success', 'failed']:
                print(f'''Getting status of job "{job['job_id']}"...''')
                self.get_job_status(job_id=job['job_id'])

    def get_job_url(self, job_id):
        job = self.db.get_job(job_id=job_id)
        # print(json.dumps(job, indent=2))
        job_url = f'''{self.files_base_url}/{self.username}/{job[0][2]}/{job_id}'''
        return job_url

    def list_job_files(self, url='', files=None):
        if not files:
            files = []
        r = requests.get('{}/json'.format(url))
        response = r.json()
        # print(json.dumps({"url": url, "files": files}, indent=2))
        for item in response:
            if item['type'] == 'directory':
                suburl = '{}/{}'.format(url, item['name'])
                files = self.list_job_files(suburl, files=files)
            elif item['type'] == 'file':
                files.append('{}/{}'.format(url, item['name']))
        return files

    def list_downloaded_files(self, download_dir=''):
        files = []
        for dirpath, dirnames, filenames in os.walk(download_dir):
            for filename in filenames:
                files.append(os.path.join(dirpath, filename))
        return files

    def verify_download(self, url='', download_dir=''):
        job_files = self.list_job_files(url)
        files = []
        for file in job_files:
            files.append(file.replace(url, ''))
        downloaded_files = self.list_downloaded_files(download_dir)
        downloads = []
        for download in downloaded_files:
            downloads.append(download.replace(download_dir, ''))
        # print(sorted(files))
        # print(sorted(downloads))
        return sorted(files) == sorted(downloads)
        
    def download_job_files(self, url='', outdir=''):
        os.makedirs(outdir, exist_ok=True)
        r = requests.get('{}/json'.format(url))
        response = r.json()
        for item in response:
            if item['type'] == 'directory':
                suburl = '{}/{}'.format(url, item['name'])
                subdir = '{}/{}'.format(outdir, item['name'])
                self.download_job_files(suburl, subdir)
            elif item['type'] == 'file':
                data = requests.get('{}/{}'.format(url, item['name']))
                with open('{}/{}'.format(outdir, item['name']), "wb") as file:
                    file.write(data.content)
