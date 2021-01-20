#!/bin/bash
set -xe

cd $HOME/prj/proxygen/proxygen
ln -Tsf _build.centos _build
cd $HOME/prj/callfwd
ln -Tsf deps.centos deps

docker build -t callfwd_builder docker/builder
docker run --rm -it -v $HOME/prj:/s callfwd_builder sh -c \
    "source /opt/rh/devtoolset-9/enable; \
    make -C /s/callfwd/build.centos all test || bash -i" \
    || true

cd $HOME/prj/proxygen/proxygen
ln -Tsf _build.arch _build
cd $HOME/prj/callfwd
ln -Tsf deps.arch deps

rsync --info=progress2 \
    build.centos/callfwd/callfwd 147.135.46.46:~
rsync callfwdctl 147.135.46.46:~
