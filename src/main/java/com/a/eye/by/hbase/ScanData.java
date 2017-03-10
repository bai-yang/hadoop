package com.a.eye.by.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanData {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quonum", "zk1,zk2,zk3");

        Connection connection = ConnectionFactory.createConnection(conf);

        String tableName = "test";
        String columnName = "info";
        String startRow = "rk1";
        String endRow = "rk10";

        String qualifier = "c1";

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(endRow.getBytes());

        scan.addColumn(columnName.getBytes(), qualifier.getBytes());

        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs) {
            String valueStr = Bytes.toString(result.getValue(columnName.getBytes(), qualifier.getBytes()));
            System.out.println(valueStr);
        }

        table.close();
        connection.close();

    }

}
