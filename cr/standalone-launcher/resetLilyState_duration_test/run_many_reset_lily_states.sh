#!/bin/bash

# This script calls resetLilyState in an endless loop. The purpose is to detect
# if there are thread or memory leaks after repeated calling of resetLilyState.
# To use it, start launch-test-lily and then run this script.
# You need to have jmxterm installed (see variable below).

if [ -z $JMXTERM_JAR ]; then
  JMXTERM_JAR=~/opt/jmxterm-1.0-alpha-4/jmxterm-1.0-alpha-4-uber.jar
fi

if [ ! -e $JMXTERM_JAR ]; then
  echo jmxterm jar not found at $JMXTERM_JAR
  exit 1
fi

LILY_SOURCE=$(cd ../../../ && pwd)
INDEX=false

while getopts "s:i" OPTION; do
  case $OPTION in
  s)
    LILY_SOURCE="$OPTARG"
    ;;
  i)
    INDEX=true
    ;;
  esac
done

echo Lily source tree root: $LILY_SOURCE

i=0

while true
do
  i=$((i + 1))

  echo
  echo
  echo Iteration $i
  echo --------------

  rm Tester-metrics*
  rm failures.log*

  # Do an import
  $LILY_SOURCE/apps/tester/target/lily-tester -z localhost -w 1 -i 50 -c tester.json

  # Request an index rebuild
  if [[ "$INDEX" = "true" ]]; then
    $LILY_SOURCE/cr/indexer/admin-cli/target/lily-add-index -z localhost -n index1 -s shard1:http://localhost:8983/solr -c $LILY_SOURCE/samples/dynamic_indexerconf/dynamic_indexerconf.xml
    $LILY_SOURCE/cr/indexer/admin-cli/target/lily-update-index -z localhost -n index1 -b BUILD_REQUESTED
    while [ -z "$($LILY_SOURCE/cr/indexer/admin-cli/target/lily-list-indexes -z localhost | grep 'Last batch build')" ]
    do
      echo Waiting for batch build to be finished.
      sleep 2
    done
  fi

  # Call resetLilyState
  echo run -b LilyLauncher:name=Launcher -m resetLilyState | java -jar $JMXTERM_JAR -l localhost:10102 -v silent
done
