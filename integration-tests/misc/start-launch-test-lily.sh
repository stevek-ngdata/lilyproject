#!/bin/bash

#
# This script starts launch-test-lily and waits for it to be fully started
#

set -e
set -u

# Check no other/previous instance is running
PORTCHECK=$(netstat -ln | grep 8020 || true)
if [ -n "$PORTCHECK" ]; then
  echo launch-test-lily or launch-hadoop or other hadoop is already running
  exit 1
fi

COMMAND="$1/../../cr/standalone-launcher/target/launch-test-lily"

OUTPUT=/tmp/start-launch-test-lily-output

# Start it
"$COMMAND" > $OUTPUT 2>&1 &

# Wait until its fully started
echo Waiting for launch-test-lily to be started
started=`date +%s`
sleep 5
while [ -z "$(grep 'Lily is running' $OUTPUT)" ]
do
  now=`date +%s`
  elapsed=$((now - started))
  if [[ $elapsed  -gt 600 ]] ; then
    echo "Did not succeed within timeout, quiting"
    echo "Check $OUTPUT"
    exit 1
  fi
  sleep 2
done
echo launch-test-lily started
