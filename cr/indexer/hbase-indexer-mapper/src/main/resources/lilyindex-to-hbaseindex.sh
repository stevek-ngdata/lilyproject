#!/bin/bash

usage() {
cat << EOF
    usage : $0 options

    Migrates an existing lily indexer-conf to a hbase indexer conf

    OPTIONS:
        -h  Show this message
        -z  Zookeeper connection string
        -c  Indexerconf file
        -n  Index name
       [-r  Repository name]
       [-t  Table name]
EOF
}

ZOOKEEPER=
INDEXERCONF=
REPOSITORY=
INDEXNAME=
TABLE=record
REPOTABLE=record

while getopts "z:c:n:r:t:" OPTION; do
    case $OPTION in
        h)
            usage
            exit 0
            ;;
        z)
            ZOOKEEPER=$OPTARG
            ;;
        c)
            INDEXERCONF=$OPTARG
            ;;
        n)
            INDEXNAME=$OPTARG
            ;;
        r)
            REPOSITORY=$OPTARG
            ;;
        t)
            TABLE=$OPTARG
            ;;
    esac
done

if [[ -z $ZOOKEEPER ]] || [[ -z $INDEXERCONF ]] || [[ -z $INDEXNAME ]]; then
    usage
    exit 0
fi

if [[ -z $REPOSITORY ]]; then
    REPOTABLE=$REPOSITORY__$TABLE
    REPOSITORY=default
fi

CONF=`cat ${INDEXERCONF} | tr -d '\n'  | xmlstarlet esc`
Q="'"

cat << EOF
<?xml version="1.0"?>
<indexer
	table="${REPOTABLE}"
	mapper="org.lilyproject.indexer.hbase.mapper.LilyResultToSolrMapper"
	unique-key-formatter="org.lilyproject.indexer.hbase.mapper.LilyUniqueKeyFormatter"
    unique-key-field="lily.key"
>
	<param name="zookeeper" value="${ZOOKEEPER}"/>
	<param name="name" value="${INDEXNAME}"/>
	<param name="repository" value="${REPOSITORY}"/>
	<param name="table" value="${TABLE}"/>

<!-- an xml encoded version of what would have been found in the lily indexer conf -->
	<param name="indexerConf" value="${CONF//\"/${Q}}"/>
</indexer>
EOF
