package org.lilycms.repository.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableUtil {
	
	private static final String RECORD_TABLE = "recordTable";
	
	public static final byte[] NON_VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("NVSCF");
	public static final byte[] VERSIONED_SYSTEM_COLUMN_FAMILY = Bytes.toBytes("VSCF");
	public static final byte[] NON_VERSIONED_COLUMN_FAMILY = Bytes.toBytes("NVCF");
	public static final byte[] VERSIONED_COLUMN_FAMILY = Bytes.toBytes("VCF");
	public static final byte[] VERSIONED_MUTABLE_COLUMN_FAMILY = Bytes.toBytes("VMCF");

	public static final byte[] PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("PAYLOADCF");
	public static final byte[] WAL_COLUMN_FAMILY = Bytes.toBytes("WALCF");
	public static final byte[] MQ_COLUMN_FAMILY = Bytes.toBytes("MQCF");

	public static HTable getRecordTable(Configuration configuration) throws IOException {
		HTable recordTable;
		try {
			recordTable = new HTable(configuration, RECORD_TABLE); 
		} catch (TableNotFoundException e) {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			HTableDescriptor tableDescriptor = new HTableDescriptor(RECORD_TABLE);
			tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_SYSTEM_COLUMN_FAMILY));
			tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_SYSTEM_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
			tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_COLUMN_FAMILY));
			tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
			tableDescriptor.addFamily(new HColumnDescriptor(VERSIONED_MUTABLE_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
			tableDescriptor.addFamily(new HColumnDescriptor(PAYLOAD_COLUMN_FAMILY));
			tableDescriptor.addFamily(new HColumnDescriptor(WAL_COLUMN_FAMILY));
			tableDescriptor.addFamily(new HColumnDescriptor(MQ_COLUMN_FAMILY));
			admin.createTable(tableDescriptor);
			recordTable = new HTable(configuration, RECORD_TABLE);
		}
		return recordTable;
	}
}
