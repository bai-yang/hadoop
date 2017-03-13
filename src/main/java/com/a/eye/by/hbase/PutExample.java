package com.a.eye.by.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class PutExample {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();

        try {
            HTable table = new HTable(conf, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
