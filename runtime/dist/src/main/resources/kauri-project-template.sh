#! /bin/sh

#
# A shell script to invoke mvn archetype:generate
#

#
# Disclaimer: this script contains stuff stolen from Ant's startup script
#

if [ -z "$KAURI_HOME" -o ! -d "$KAURI_HOME" ] ; then
  ## resolve links - $0 may be a link to Kauri's home
  PRG="$0"
  progname=`basename "$0"`

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
    else
    PRG=`dirname "$PRG"`"/$link"
    fi
  done

  KAURI_HOME=`dirname "$PRG"`/..

  # make it fully qualified
  KAURI_HOME=`cd "$KAURI_HOME" && pwd`
fi

mvn -q archetype:generate -DarchetypeCatalog=file://$KAURI_HOME/lib
