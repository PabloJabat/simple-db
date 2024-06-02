#!/bin/bash

if [[ -e $1 ]]
then
  grep "$2," $1 | sed -e "s/^$2,//" | tail -n 1
else
  raise error "Database doesn't exist ..."
fi
