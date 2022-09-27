# DESaccess bulk downloader

## How to run

Execute the `run_jupyterlab.sh` script after defining the environment variables as described in that script file. This will launch a JupyterLab server you can access at `http://127.0.0.1:8888` (default `token` is the word `docker`) with this directory mounted to `/home/jovyan/work`.

Open the `bulk_download.ipynb` Jupyter notebook to launch cutout jobs.

## Structure

The code structure is the following: there is a module `desaccess_api.py` implementing a class that interfaces with the DESaccess API, and another module `db.py` that implements a class that interacts with a local SQLite file. For a robust automated solution for bulk cutouts, you need a non-trivial memory of what jobs have completed, which have failed, if you have downloaded the data for a job, etc. This is the purpose of the SQLite database.

## Development plan

Once all the local db fields are known and the db interface is solid, and once the necessary DESaccess API functions are implemented, then we will input a big list of cutout positions and have the script automatically partition it, launch jobs for each partition, track the job status, and download data when complete. It should be something you can stop and restart without re-running completed cutouts you've already downloaded.
