This package contains fixed HBase MapReduce classes which close the HBase
connection, and hence the ZooKeeper connection.

For normal usage, this does not make much of a difference, but when using
launch-test-lily (LilyLauncher) where everything runs in one VM, this can
cause a real issue with dozens or hundreds of ZooKeeper threads running
after launching MR jobs repeatedly.

This issue is also discussed here, the solution used is the same (create
HTable instance in getSplits() and getRecordReader() methods):

https://issues.apache.org/jira/browse/HBASE-3792
http://markmail.org/message/iumuq4cjlthqhdig