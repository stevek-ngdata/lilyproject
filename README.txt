                             Welcome to Lily CMS
                           http://www.lilycms.org


Prerequisites
=============

Extra jars
----------
First install some extra jars in your local Maven repository by executing:

cd lib
sh install.sh


Install Maven 2.2.x
-------------------

From http://maven.apache.org


Building Lily
=============

Execute

mvn install

or to run it faster (without the tests):

mvn -Pfast install