#!/bin/bash

#
# This script stops launch-test-lily and waits until the process quits
#

set -e
set -u

LILY_PID=$(ps -eo pid,args | grep LilyLauncher | grep -v grep | awk '{print $1}')
if [ "" != "$LILY_PID" ]
then 
  echo Stopping launch-test-lily with pid $LILY_PID
  kill $LILY_PID
  # Wait for Lily Server to stop
  echo Waiting for launch-test-lily to stop
  while [ -n "$(ps -eo pid,args | grep LilyLauncher | grep -v grep)" ]
  do
    sleep 1
  done
fi

echo launch-test-lily stopped

