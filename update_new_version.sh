#!/bin/bash
python setup.py sdist

CUR_VERSION=$(cat setup.cfg | grep "current_version = .*" | awk '{split($0,a,"\= "); print a[2]}')

echo "new version: ${CUR_VERSION}"
echo "copying to ${HEROKU_PYPI_LOCAL_DIR}/data/"
cp dist/event_service_utils-${CUR_VERSION}.tar.gz ${HEROKU_PYPI_LOCAL_DIR}/data/
pushd $HEROKU_PYPI_LOCAL_DIR
echo "commiting changes in pypi repo"
git add data/
git commit -m 'new version'
echo "pushing changes to heroku"
git push heroku master
popd