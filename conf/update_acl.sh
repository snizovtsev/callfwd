#!/bin/bash -xe

ACL_OUT="/home/sergei/acl.csv"
ACL_QUERY="\\copy (select host(ipv4),last_updated_on,expired_on,cps from auth_ip) to $ACL_OUT with csv;"

psql -U postgres -c "$ACL_QUERY" lrn_engine
/home/sergei/callfwdctl acl "$ACL_OUT"
