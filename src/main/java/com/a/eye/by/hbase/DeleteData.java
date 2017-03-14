package com.a.eye.by.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

public class DeleteData {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quonum", "zk1,zk2,zk3");

        Connection connection = ConnectionFactory.createConnection(conf);

        String tableName = "test";
        String columnName = "info";
        String rowKey = "rk1";

        String qualifier = "c1";

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(rowKey.getBytes());

        delete.addColumn(columnName.getBytes(), qualifier.getBytes());

        table.delete(delete);

        table.close();
        connection.close();

        Delete delete2 = new Delete(rowKey.getBytes());
        delete2.addColumn(columnName.getBytes(), qualifier.getBytes());

        List<Delete> deletes = new ArrayList<Delete>();
        deletes.add(delete);
        deletes.add(delete2);

        table.delete(deletes);

        table.checkAndDelete(columnName.getBytes(), qualifier.getBytes(), qualifier.getBytes(), null, delete2);

    }

}
