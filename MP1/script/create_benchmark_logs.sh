#!/usr/bin/env bash

cp ~/go/src/MP1/resource/vm10.log  ~/go/src/MP1/resource/benchmarkvm10.log
cat ~/go/src/MP1/resource/vm10.log >> ~/go/src/MP1/resource/benchmarkvm10.log

for i in {1..9}
do
    cp ~/go/src/MP1/resource/benchmarkvm10.log  ~/go/src/MP1/resource/benchmarkvm$i.log
done