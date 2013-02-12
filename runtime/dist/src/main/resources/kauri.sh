#! /bin/sh

#
# Kauri Runtime startup script
#

# You can use the following environment variables to customize the startup
#
# KAURI_CLI_CLASSPATH
#    additional entries to be added to the classpath
#
# KAURI_JAVA_OPTIONS
#    additional options to be passed to the java executable
#

if [ -z "$JAVA_HOME" ] ; then
  echo "JAVA_HOME not set"
  exit 1
fi

# This technique for detecting KAURI_HOME has been adapted from ant's startup script
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


M2_REPO=$KAURI_HOME/lib

LAUNCHER_JAR=$M2_REPO/org/kauriproject/kauri-runtime-launcher/${project.version}/kauri-runtime-launcher-${project.version}.jar

CLASSPATH=$LAUNCHER_JAR

# Only add KAURI_CLI_CLASSPATH when it is not empty, to avoid adding the working dir to
# the classpath by accident.
if [ ! -z "$KAURI_CLI_CLASSPATH" ] ; then
  CLASSPATH=$CLASSPATH:$KAURI_CLI_CLASSPATH
fi

export CLASSPATH

KAURI_LOG_OPTS="-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog -Dorg.apache.commons.logging.simplelog.defaultlog=error"

"$JAVA_HOME/bin/java" $KAURI_JAVA_OPTIONS $KAURI_LOG_OPTS -Dkauri.launcher.repository=$M2_REPO org.kauriproject.launcher.RuntimeCliLauncher $@
