#!/bin/bash

mapping=$(df | awk '{print $1, $6}' | grep /data | awk '{sub("/dev/", ""); print}')

for i in {0..3}; do
   disks=$(ls -l /sys/block/ | grep "expander-0:$i/" | awk '{print $9}';)
   echo "std::vector<std::string> ${1}_hba${i} = {"
   for disk in ${disks}; do
      echo "$mapping" | grep $disk'1' | awk '{print "  \"root://st-096-100gb001.cern.ch/"$2"\","}'
   done
   echo '  ""'
   echo "};"
   echo
done