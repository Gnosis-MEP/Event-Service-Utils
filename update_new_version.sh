#!/bin/bash
set -e
# bumpversion major, minor, patch
python setup.py sdist

CUR_VERSION=$(cat setup.cfg | grep "current_version = .*" | awk '{split($0,a,"\= "); print a[2]}')

twine upload dist/event_service_utils-${CUR_VERSION}.tar.gz