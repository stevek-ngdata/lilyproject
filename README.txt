                             Welcome to Lily CMS
                           http://www.lilycms.org


Prerequisites
=============

Install Maven 2.2.x
-------------------

From http://maven.apache.org

Download Solr
-------------

Download Solr 1.4.0 from http://lucene.apache.org/solr/ and extract it somewhere.

Configuring settings.xml
------------------------

Open or create ~/.m2/settings.xml and add the following profile to it:

<settings>
  <profiles>
    <profile>
      <id>org.lilycms.solr</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>

      <properties>
        <solr.war>/home/bruno/projects/lily-trunk/deps/apache-solr-1.4.0/dist/apache-solr-1.4.0.war</solr.war>
      </properties>
    </profile>
  </profiles>
</settings>

Adjust the solr.war property to match the path you extracted Solr to.

Building Lily
=============

Execute

mvn install

or to run it faster (without the tests):

mvn -Pfast install

On running test
===============

Log output of testcases is by default sent to a target/log.txt, errors are however always logged to the
console. Debug output to the console of selected log categories can be enabled by running tests as follows:

mvn -DargLine=-DlilyTestDebug test

Running tests faster
--------------------

THIS DOES CURRENTLY NOT WORK, JUST LEFT IN HERE IN THE HOPE IT MIGHT IN THE FUTURE
See also description of the problem in the javadoc of class HBaseProxy.

Test run rather slow because HBase-based tests launch a mini Hadoop/ZooKeeper/HBase-cluster as part of the
testcase. While this takes some time in itself, it is especially the creation of tables in HBase which
takes time.

The tests can be sped up by starting an independent cluster and running the tests against that. Instead of
dropping and recreating tables between each test, the tables are emptied by deleting all rows from them (thus
be very careful against which HBase you run this!).

The easy way to do this is:

cd testfw
mvn exec:java

This prints a line "Minicluster is up" when it is started, though it is
quickly followed by more logging so you might not easily notice it.

And then run the tests with

mvn -Pconnect test

The first time this will still take more time (though already quite a bit less than before), since the
tables still need to be created. Subsequent runs should be way faster.