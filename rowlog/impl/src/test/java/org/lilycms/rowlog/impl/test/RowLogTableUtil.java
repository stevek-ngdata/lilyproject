package org.lilycms.rowlog.impl.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class RowLogTableUtil {
	
	private static final String ROW_TABLE = "rowTable";
	
	public static final byte[] PAYLOAD_COLUMN_FAMILY = Bytes.toBytes("PAYLOADCF");
	public static final byte[] ROWLOG_COLUMN_FAMILY = Bytes.toBytes("ROWLOGCF");

	public static HTable getRowTable(Configuration configuration) throws IOException {
		HTable rowTable;
		try {
			rowTable = new HTable(configuration, ROW_TABLE); 
		} catch (IOException e) {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			HTableDescriptor tableDescriptor = new HTableDescriptor(ROW_TABLE);
			tableDescriptor.addFamily(new HColumnDescriptor(PAYLOAD_COLUMN_FAMILY));
			tableDescriptor.addFamily(new HColumnDescriptor(ROWLOG_COLUMN_FAMILY));
			admin.createTable(tableDescriptor);
			rowTable = new HTable(configuration, ROW_TABLE);
		}
		return rowTable;
	}
}
