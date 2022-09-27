#/bin/bash
#
# Export your DESaccess username and password prior to launching JupyterLab
#
#     export DESACCESS_USERNAME=''
#     export DESACCESS_PASSWORD=''

docker run -e DESACCESS_USERNAME=$DESACCESS_USERNAME \
           -e DESACCESS_PASSWORD=$DESACCESS_PASSWORD \
           -e JUPYTER_ENABLE_LAB=yes \
           -e JUPYTER_TOKEN=docker \
           -v $(pwd):/home/jovyan/work \
           -p 8888:8888 \
           --rm --name jupyter \
           -d jupyter/datascience-notebook:latest
