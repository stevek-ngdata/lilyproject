HBASE_VERSION=0.21.0-dev-r908077
ZK_VERSION=3.2.2

mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hbase -Dversion=$HBASE_VERSION -Dpackaging=jar -Dfile=hbase-$HBASE_VERSION.jar -DpomFile=hbase.pom
mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hbase-test -Dversion=$HBASE_VERSION -Dpackaging=jar -Dfile=hbase-test-$HBASE_VERSION.jar -DpomFile=hbase-test.pom

mvn install:install-file -DgroupId=org.apache.hadoop.zookeeper -DartifactId=zookeeper -Dversion=$ZK_VERSION -Dpackaging=jar -Dfile=zookeeper-$ZK_VERSION.jar -DgeneratePom=true