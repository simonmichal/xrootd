#!/bin/bash

iptables -I INPUT -p tcp --dport 2094 -j ACCEPT
iptables -I INPUT -p tcp --dport 2095 -j ACCEPT
iptables -I INPUT -p tcp --dport 2096 -j ACCEPT
iptables -I INPUT -p tcp --dport 2097 -j ACCEPT