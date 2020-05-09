#!/bin/bash

mkdir /hba{0..3}data

mapping=$(df | awk '{print $1, $6}' | grep /data | awk '{sub("/dev/", ""); print}')

for i in {0..3}; do
   disks=$(ls -l /sys/block/ | grep "expander-0:$i/" | awk '{print $9}';)
   echo "HBA${i}:"
   j=0
   for disk in ${disks}; do
      mount=$(echo "$mapping" | grep $disk'1' | awk '{print $2}')
      #echo $mount
      echo "link ${mount} to /hba${i}data/data${j}"
      ln -s $mount  /hba${i}data/data${j}
      j=$((j+1))
   done
done