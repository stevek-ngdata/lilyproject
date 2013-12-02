#!/bin/bash

if [ -z "${HBASE_INDEXER_HOME}" ]; then
    echo "Set the HBASE_INDEXER_HOME variable";
    exit 1;
fi

echo -n "export HBASE_INDEXER_CLASSPATH=\${HBASE_INDEXER_CLASSPATH}:@@@MAPPER@@@:" | cat - target/dev-classpath.txt >> ${HBASE_INDEXER_HOME}/conf/hbase-indexer-env.sh

xmlstarlet ed -L -S \
    -s /configuration[not\(property/name/text\(\)='hbaseindexer.lifecycle.listeners'\)] -t elem -n property -v "" \
    -s /configuration/property[not\(name\)] -t elem -n name -v "hbaseindexer.lifecycle.listeners" \
    -s /configuration/property[not\(value\)] -t elem -n value -v "org.lilyproject.indexer.hbase.mapper.LilyIndexerLifecycleEventListener" \
    ${HBASE_INDEXER_HOME}/conf/hbase-indexer-site.xml