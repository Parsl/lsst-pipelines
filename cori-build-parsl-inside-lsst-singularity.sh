#!/bin/bash -ex

# requires parsl source checkout in $(pwd)/parsl/

source /opt/lsst/software/stack/loadLSST.bash

rm -rf ~/.conda/envs/parsl-pipeplay

conda create -n parsl-pipeplay -y
source activate parsl-pipeplay

conda install pip -y

pushd parsl
pip install .
popd
