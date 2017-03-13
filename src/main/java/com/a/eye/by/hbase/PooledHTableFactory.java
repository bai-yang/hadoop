package com.a.eye.by.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class PooledHTableFactory {

    public static final int DEFAULT_POOL_SIZE = 256;
    public static final int DEFAULT_WORKER_QUEUE_SIZE = 1024;
    public static final boolean DEFAULT_PRESTART_THREAD_POOL = false;

    private final ExecutorService executor;
    private final Connection connection;

    public PooledHTableFactory(Configuration conf) throws IOException {
        this(conf, DEFAULT_POOL_SIZE, DEFAULT_WORKER_QUEUE_SIZE, DEFAULT_PRESTART_THREAD_POOL);
    }

    public PooledHTableFactory(Configuration conf, int poolSize, int workQueueSize, boolean prestartThreadPool)
            throws IOException {
        ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(workQueueSize));
        if (prestartThreadPool) {
            threadPoolExecutor.prestartAllCoreThreads();
        }
        this.executor = threadPoolExecutor;
        this.connection = ConnectionFactory.createConnection(conf, executor);
    }

    public Connection getConnection() {
        return this.connection;
    }

    public Table getTable(TableName tableName) throws IOException {
        return this.connection.getTable(tableName);
    }

    public void releaseTable(Table table) {
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void destory() {
        if (null != this.connection) {
            try {
                this.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != this.executor) {
            this.executor.shutdown();
            try {
                final boolean shutdown = this.executor.awaitTermination(1000 * 5, TimeUnit.MILLISECONDS);
                if (!shutdown) {
                    final List<Runnable> discardTask = this.executor.shutdownNow();
                    System.out.println("discard task size:" + discardTask.size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
    }
}
