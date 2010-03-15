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
