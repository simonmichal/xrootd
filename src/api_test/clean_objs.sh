#!/bin/bash

rm -rf /hba*data/data*/test/
mkdir /hba{0..3}data/data{0..23}/test
chown -R xrootd:xrootd /hba*data/data*/test