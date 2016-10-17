#!/usr/bin/env bash

conda env create -f environment.yml -p pyspark3
tar -czvf pyspark3.tar.gz @{$1}/pyspark3/