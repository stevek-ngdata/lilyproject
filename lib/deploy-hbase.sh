HBASE_VERSION=0.21.0-dev-r910185

mvn deploy:deploy-file -DrepositoryId=org.lilycms.maven-deploy -Durl=scp://lilycms.org/var/www/lily/maven/maven2/deploy -DgroupId=org.apache.hadoop -DartifactId=hbase -Dversion=$HBASE_VERSION -Dpackaging=jar -Dfile=hbase-$HBASE_VERSION.jar -DpomFile=hbase.pom
mvn deploy:deploy-file -DrepositoryId=org.lilycms.maven-deploy -Durl=scp://lilycms.org/var/www/lily/maven/maven2/deploy -DgroupId=org.apache.hadoop -DartifactId=hbase-test -Dversion=$HBASE_VERSION -Dpackaging=jar -Dfile=hbase-test-$HBASE_VERSION.jar -DpomFile=hbase-test.pom
