              Standalone Lily repository process: how to use
          - o - o - o - o - o - o - o - o - o - o - o - o - o - o -

Prerequisites
=============

You should have HBase running in order to run the Lily repository server.

Install Kauri
=============

At this time we use an untagged Kauri trunk snapshot.

Checkout or export kauri trunk:

svn co https://outerthought.repositoryhosting.com/svn/outerthought_kauri/trunk kauri-trunk

Build it using:

mvn install

or faster, without tests:

mvn -P fast install


Configure
=========

If your zookeeper quorum does not consist of a single, local-host server
listening on port 2181, you will need to adjust some configuration.

Perform (in the same directory as this README.txt), the following

cp -r conf myconf
cd myconf
find -name .svn rm -rf {} \;

and then edit the files

conf/hbase/hbase.xml
conf/repository/repository.xml

Run
===

From within the same directory as this README.txt, execute:

With standard configuration:
/path/to/kauri-trunk/kauri.sh

With custom configuration:
/path/to/kauri-trunk/kauri.sh run -c myconf

You can start as many of these processes as you want. By default the server sockets
are bound to ephemeral ports, so there will be no conflicts.

Once you have servers running, you can use the client library to connect to them.