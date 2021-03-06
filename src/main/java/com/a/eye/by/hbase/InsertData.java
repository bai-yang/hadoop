package com.a.eye.by.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class InsertData {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quonum", "zk1,zk2");

        Connection connection = ConnectionFactory.createConnection(conf);

        String tableName = "test";
        String columnName = "info";

        String rowkey = "rk1";
        String qulifier = "c1";

        String value = "value1";

        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(rowkey.getBytes());
        put.addColumn(columnName.getBytes(), qulifier.getBytes(), value.getBytes());

        table.put(put);

        table.close();
        connection.close();
        
        Put put1 = new Put(rowkey.getBytes());
        put1.addColumn(columnName.getBytes(), qulifier.getBytes(), value.getBytes());
        
        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        puts.add(put1);
        
        table.put(puts);

    }

}
