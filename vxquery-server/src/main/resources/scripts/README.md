Utilities for Cluster Operations
=====================

# Introduction

Helpful scripts to work with the VXQuery cluster.

## Cluster Cli

The CLI script includes options to deploy, start, and stop a cluster.

Example commands:
python cluster_cli.py -c ../conf/cluster.xml -a deploy -d /apache-vxquery/vxquery-server
python cluster_cli.py -c ../conf/cluster.xml -a deploy -d /apache-vxquery/vxquery-cli
python cluster_cli.py -c ../conf/cluster.xml -a start
python cluster_cli.py -c ../conf/cluster.xml -a stop