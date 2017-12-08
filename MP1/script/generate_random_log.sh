#!/usr/bin/env bash
cd ~/go/src/MP1/resource/
for i in {1..10}
do
    cat /dev/urandom | tr -dc '[:graph:]' | head -c 5000000 > randomvm$i.log
    cat vm$i.log >> randomvm$i.log
done
cd ~