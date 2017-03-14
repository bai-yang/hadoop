package com.a.eye.by.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;

public class BatchData {

    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quonum", "zk1,zk2,zk3");

        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf("test"));

        Get get = new Get("row1".getBytes());
        get.addColumn("c1".getBytes(), "q1".getBytes());

        Put put = new Put("row2".getBytes());
        put.addColumn("c1".getBytes(), "q1".getBytes(), "v1".getBytes());

        List<Row> actions = new ArrayList<Row>();

        Object[] results = new Object[actions.size()];
        table.batch(actions, results);

        for (Object o : results) {
            System.out.println("result:" + o.toString());
        }
    }
}
