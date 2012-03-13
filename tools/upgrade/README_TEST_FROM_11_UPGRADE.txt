
Here's a manual procedure for testing the upgrade-from-1.1 tool using launch-hadoop:

 * Start launch-hadoop (cd global/hadoop-test-fw; ./target/launch-hadoop)
 * Start lily-server as included with Lily 1.1 (./bin/lily-server)
 * Create records. Two scenario's are possible:
     - create many records
     - create specific records
     See details below.
 * Leave Lily running and try to run the upgrade tool. It should complain
   that Lily is running.
 * Stop lily-server
 * Run the tool
 * Run the tool again: it should complain the target table already exists
 * Follow the instructions to rename the table at the end of the tool
   For HBase shell usage: see HBase configuration below.
 * Start lily-server of 1.2/trunk (cd cr/process/server; ./target/lily-server)
 * Use the 'lily-scan-records -p' tool to check it is able to decode the
   record IDs (also those embedded in link fields)


Create specific records
=======================

For this test scenario, use the Lily 1.1 version of the import tool to upload
some crafted records:

./bin/lily-import ~/projects/lily-scanners/tools/upgrade/upgrade_from_11_testdata.json

Take a dump of the record-row-visualizer output of the record USER.versions:

./bin/lily-record-row-visualizer -r USER.versions  | w3m -T text/html -dump -cols 120 > record.txt

Now follow the remainder of the upgrade instructions.

Some checks you can do after the upgrade is done:

Go look at some records through the REST interface:

http://localhost:12060/repository/record/USER.versions

Check a record with links to verify it is properly decoded:

http://localhost:12060/repository/record/USER.links

Check the storage of the versioned record is the same, now using the
new version of lily-record-row-visualizer:

lily-trunk/tools/record-row-visualizer/target/lily-record-row-visualizer -r USER.versions  | w3m -T text/html -dump -cols 120 > record2.txt

and compare the output with what we had before:

diff -u record.txt record2.txt

(the output should be empty)

Use lily-scan-records to dump all records (to check there are no decoding failures)


Create many records
===================

Use lily-tester from Lily 1.1 to generate some data:

./bin/lily-tester -c upgrade_from_11_tester.json -i 1000 -w 5

This should create 50000 records (and 10000 deleted records)

You can verify these numbers during upgrade, and use lily-scan-records
to check the new record table contains the correct amount of records.

HBase configuration
===================
For using the HBase shell against launch-hadoop, you'll need to
have something similar to this in conf/hbase-site.xml:

 <configuration>
   <property>
     <name>hbase.rootdir</name>
     <value>hdfs://localhost:8020/user/bruno</value>
     <description>The directory shared by region servers.
     </description>
   </property>
   <property>
     <name>hbase.zookeeper.quorum</name>
     <value>localhost</value>
   </property>
   <property>
     <name>dfs.replication</name>
     <value>1</value>
   </property>
 </configuration>



