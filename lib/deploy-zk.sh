ZK_VERSION=3.2.2
mvn deploy:deploy-file -DrepositoryId=org.lilycms.maven-deploy -Durl=scp://lilycms.org/var/www/lily/maven/maven2/deploy -DgroupId=org.apache.hadoop.zookeeper -DartifactId=zookeeper -Dversion=$ZK_VERSION -Dpackaging=jar -Dfile=zookeeper-$ZK_VERSION.jar -DgeneratePom=true
