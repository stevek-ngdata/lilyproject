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

 - have a Lily stack running, e.g. using launch-test-lily

 - import testdata.json sample data:
   lily-import testdata.json

 - start the MapReduce job using:
   cd /to/hadoop
   ./bin/hadoop jar my-lily-mrjob/target/my-lily-mrjob-1.0-SNAPSHOT-mapreduce-job.jar com.mycompany.MyJob

   (this assumes JobTracker/Namenode/ZooKeeper is running on localhost,
   see MyJob code to adjust)

 - if the job has run correctly, you can run lily-scan-records to
   check the output produced by the reducer

If you want to run the job a second time, you will have to remove
the output directory first:

./bin/hadoop fs -rmr hdfs://localhost:8020/user/bruno/my-job-output
