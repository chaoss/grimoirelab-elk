#!/bin/bash

if [ $# -le 2 ]
    then
    echo "Arguments expected: sh_database user_name org_name"
    exit 1
fi

DB=$1
USERNAME=$2
ORG=$3

OUTPUT=`sortinghat --host mariadb -d $DB add --source github --username $USERNAME`
if [ $? -eq 0 ]
    then
    ID=`echo $OUTPUT|awk {'print $6'}`
    sortinghat --host mariadb -d $DB enroll $ID $ORG
fi
