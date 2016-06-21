package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by fuweimin on 2016/6/20.
 */
public class HbaseUtils {

	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "master");
		configuration.set("hbase.master", "master:60000");
	}

	public static void main(String[] args) throws IOException {
		System.out.println(getResult("java"));
	}



//	public void createTable(String tableName, String[] families) throws IOException {
//		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
//		if (admin.tableExists(tableName)) {
//		} else {
//			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
//			for (int i = 0; i < families.length; i++) {
//				tableDesc.addFamily(new HColumnDescriptor(families[i]));
//			}
//			admin.createTable(tableDesc);
//		}
//		setTable(tableName, families);
//	}

	public static void insertData(String key, String values) throws IOException {
		System.out.println("start insert data ......");
		HTable table  = new HTable(configuration, "relatedsw");
		table.setAutoFlush(true, false);
		Put put = new Put(key.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		put.add("words".getBytes(), null, values.getBytes());// 本行数据的第一列

		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data ......");
	}

	public static String getResult(String key) throws IOException {
		HTable table  = new HTable(configuration, "relatedsw");
		table.setAutoFlush(true, false);
		Get get = new Get(key.getBytes());
		Result rs = table.get(get);

		for(KeyValue kv : rs.raw()){
			return new String(kv.getValue());
		}
		return "";
	}
}
