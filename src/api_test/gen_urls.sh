#!/bin/bash

echo "  std::vector<std::string> st00${1}_hba0 = {"
for i in {0..23}; do
echo "    \"root://st-096-100gb00${1}.cern.ch:2094//hba0data/data${i}\","
done
echo "    \"\""
echo "  };"

echo

echo "  std::vector<std::string> st00${1}_hba1 = {"
for i in {0..23}; do
echo "    \"root://st-096-100gb00${1}.cern.ch:2095//hba1data/data${i}\","
done
echo "    \"\""
echo "  };"

echo 

echo "  std::vector<std::string> st00${1}_hba2 = {"
for i in {0..23}; do
echo "    \"root://st-096-100gb00${1}.cern.ch:2096//hba2data/data${i}\","
done
echo "    \"\""
echo "  };"

echo

echo "  std::vector<std::string> st00${1}_hba3 = {"
for i in {0..23}; do
echo "    \"root://st-096-100gb00${1}.cern.ch:2097//hba3data/data${i}\","
done
echo "    \"\""
echo "  };"