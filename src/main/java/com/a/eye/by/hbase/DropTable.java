package com.a.eye.by.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DropTable {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        conf.set("hbase.zookeeper.quonum", "zk1,zk2,zk3");

        Connection connection = ConnectionFactory.createConnection(conf);

        String tableName = "test";

        Admin admin = connection.getAdmin();

        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));

        admin.close();

        connection.close();

    }

}
