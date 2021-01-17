#!/bin/sh
exec sipp -sf data/sipp-scenario.xml -inf data/uids.csv -nd 127.0.0.1:5061 $@
