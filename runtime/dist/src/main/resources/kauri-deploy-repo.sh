#! /bin/sh

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

CLASSPATH=$M2_REPO/org/kauriproject/kauri-deploy-repo/${project.version}/kauri-deploy-repo-${project.version}.jar:
export CLASSPATH

"$JAVA_HOME/bin/java" -Dkauri.home="$KAURI_HOME" org.kauriproject.tools.deployrepo.DeployRepo $@
