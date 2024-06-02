#!/bin/bash

if ! [[ -e $1 ]]
then
  touch $1
fi
