About this MapReduce job
========================

This is a template project for writing your own Lily MapReduce job.

It contains a sample mapper which uses the LilyInputFormat to read
data from Lily. It also contains a sample reducer, which writes its
output to Lily using the LilyClient.

If you don't need a reducer and want to write to Lily directly
from your mapper, just copy over the relevant parts from the reducer
sample.

As part of the build, a job-jar is created which contains all
required dependencies in the lib subdir.

To run this example:

 - check the jobtracker port in MyJob.java (check mapred-site.xml on your jobtracker host. Common values are 8021, 9001 and 8012.)

 - compile this project using "mvn install"

 - have a Lily stack running, e.g. using launch-test-lily

 - import the testdata.json sample data:

   lily-import testdata.json

 - have the "hadoop" command available

   For this you need to have hadoop installed, in the "MapReduce v1" variant.
   If you do this on one of the cluster nodes on which Lily is installed,
   this should already be fine.

   If you do this from your workstation, you can download the "mr1" package
   from the Cloudera website, at:
   https://ccp.cloudera.com/display/SUPPORT/CDH4+Downloadable+Tarballs
   You need the "mr1-{version}" tar.gz, not the hadoop download.
   After downloading, just extract it, no further installation is needed.

 - set the classpath

   export HADOOP_USER_CLASSPATH_FIRST=true
   export HADOOP_CLASSPATH=`lily-mapreduce-classpath`

 - start the MapReduce job

   /path/to/hadoop-2.0.0-mr1-cdh4.0.X/bin/hadoop jar target/my-lily-mrjob-1.0-SNAPSHOT-mapreduce-job.jar com.mycompany.MyJob [jobtrackerhost]

   (this assumes JobTracker/Namenode/ZooKeeper is running on localhost,
   see MyJob code to adjust)

 - if the job has run correctly, you can run lily-scan-records to
   check the output produced by the reducer:

   This will print out entries like the following:

   ID = USER.and
   Version = null
   Non-versioned scope:
     Record type = {mrsample}Summary, version 1
     {mrsample}wordcount = 2

   which means the word "and" has been counted twice in the input
