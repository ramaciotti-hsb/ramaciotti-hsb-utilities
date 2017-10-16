#!/bin/bash
set -e
echo "Downloading python3 installer..."
curl -s https://www.python.org/ftp/python/3.6.3/python-3.6.3-macosx10.6.pkg > /tmp/python3-installer.pkg
installer -pkg /tmp/python3-installer.pkg -target /
pip3 install -r requirements.txt