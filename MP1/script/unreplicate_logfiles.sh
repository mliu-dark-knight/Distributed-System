#!/usr/bin/env bash

VMNumberWithZero=$(hostname | cut -c16-17)
VMNumber=$(echo $VMNumberWithZero | sed 's/^0*//')
VMNumber=$((VMNumber))

for i in {1..10}
do
    if [ "$i" -ne "$VMNumber" ]
    then
        echo ~/go/src/MP1/resource/vm$i.log
        rm ~/go/src/MP1/resource/vm$i.log
    fi
done