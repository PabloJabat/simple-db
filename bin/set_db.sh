#!/bin/bash

if [[ -e $1 ]]
then
  echo $2,$3 >> $1
else
  raise error "Database $1 doesn't exist ..."
fi