#! /bin/bash

if [[ $# -lt 1 ]]
then
  echo "USAGE: $0 SCHED_ALGO"
  exit 1
fi

mkdir -p "logs/SCHED_$1"

mv *.log "logs/SCHED_$1"
