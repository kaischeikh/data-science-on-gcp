#!/bin/bash

# install Google Python client on all nodes
apt-get update
apt-get install -y python-pip
pip install --upgrade google-api-python-client

# git clone on Master
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  cd $HOME
  git clone https://github.com/GoogleCloudPlatform/data-science-on-gcp
fi
