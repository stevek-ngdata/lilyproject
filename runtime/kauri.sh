#! /bin/sh

#
# Kauri Runtime startup script for use during Kauri development
#  meaning: there is no KAURI_HOME, Kauri artifacts are in dev's local Maven repo
#

# You can use the following environment variables to customize the startup
#
# KAURI_CLI_CLASSPATH
#    additional entries to be added to the classpath
#
# KAURI_JAVA_ARGS
#    additional options to be passed to the java executable
#
# KAURI_DEBUG_ARGS
#    can be used to define alternative Java debug arguments
#
# KAURI_DEBUG_SUSPEND_ARGS
#    can be used to define alternative Java debug arguments for suspended mode
#

#
# Disclaimer: this script is based on stuff from Apache Cocoon's 'cocoon.sh' script
#

usage()
{
    echo "Usage: $0 (action)"
    echo "actions:"
    echo "  run               Run the Kauri Runtime (default)"
    echo "  debug             Run the Kauri Runtime with debug port 5005"
    echo "  debug-suspend     Run the Kauri Runtime, suspend for debugger to connect on port 5005"
    echo ""
    echo "To see help options for Kauri, use $0 run -h"
    echo ""
    exit 1
}


# ----- Handle action parameter ------------------------------------------------
DEFAULT_ACTION="run"
ACTION="$1"
if [ -n "$ACTION" ]
then
  shift
else
  ACTION=$DEFAULT_ACTION
  echo "$0: executing default action '$ACTION', use -h to see other actions"
fi
ARGS="$*"



# ----- Find out home dir of this script ---------------------------------------

#
# Disclaimer: this is based on things seen in Ant's startup script
#

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

KAURI_SRC_HOME=`dirname "$PRG"`

# make it fully qualified
KAURI_SRC_HOME=`cd "$KAURI_SRC_HOME" && pwd`


# ----- Verify and Set Required Environment Variables -------------------------
if [ -z "$JAVA_HOME" ] ; then
  echo "JAVA_HOME not set!"
  exit 1
fi

LAUNCHER_JAR=$KAURI_SRC_HOME/core/kauri-runtime-launcher/target/kauri-runtime-launcher.jar

[ ! -r "$LAUNCHER_JAR" ] \
  && echo "Launcher jar not found at $LAUNCHER_JAR" \
  && echo "Please build Kauri first using 'mvn install'" \
  && exit 1

CLASSPATH=$LAUNCHER_JAR

# Only add KAURI_CLI_CLASSPATH when it is not empty, to avoid adding the working dir to
# the classpath by accident.
if [ ! -z "$KAURI_CLI_CLASSPATH" ] ; then
  CLASSPATH=$CLASSPATH:$KAURI_CLI_CLASSPATH
fi

export CLASSPATH

if [ "$KAURI_DEBUG_ARGS" = "" ] ; then
  KAURI_DEBUG_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"
fi

if [ "$KAURI_DEBUG_SUSPEND_ARGS" = "" ] ; then
  KAURI_DEBUG_SUSPEND_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
fi

# ----- Do the action ----------------------------------------------------------

case "$ACTION" in
  run)
        "$JAVA_HOME/bin/java" -Djava.awt.headless=true $KAURI_JAVA_ARGS org.kauriproject.launcher.RuntimeCliLauncher $@
        ;;

  debug)
        "$JAVA_HOME/bin/java" -Djava.awt.headless=true $KAURI_JAVA_ARGS $KAURI_DEBUG_ARGS org.kauriproject.launcher.RuntimeCliLauncher $@
        ;;

  debug-suspend)
        echo "Don't forget: you'll need to connect with a debugger for Kauri to start"
        "$JAVA_HOME/bin/java" -Djava.awt.headless=true $KAURI_JAVA_ARGS $KAURI_DEBUG_SUSPEND_ARGS org.kauriproject.launcher.RuntimeCliLauncher $@
        ;;

  *)
        usage
        ;;
esac

exit 0
