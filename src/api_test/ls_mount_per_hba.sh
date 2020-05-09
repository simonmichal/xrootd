#!/bin/bash

mapping=$(df | awk '{print $1, $6}' | grep /data | awk '{sub("/dev/", ""); print}')

for i in {0..3}; do
   disks=$(ls -l /sys/block/ | grep "expander-0:$i/" | awk '{print $9}';)
   echo "HBA${i}:"
   for disk in ${disks}; do
      m=$(echo "$mapping" | grep $disk'1' | awk '{print $2}')
      echo $m
   done
done