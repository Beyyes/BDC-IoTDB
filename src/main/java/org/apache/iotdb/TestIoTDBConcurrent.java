package org.apache.iotdb;


import org.apache.iotdb.rpc.*;
import org.apache.iotdb.session.*;

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * 测试IOTDB并发连接数
 */
public class TestIoTDBConcurrent {

    static String host = "172.20.31.26";

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 0) {
            System.out.println("Please provide an argument");
            System.exit(1);
        }
        int n = 4;

        ExecutorService executorService = Executors.newFixedThreadPool(n);
        AtomicInteger atomic_count = new AtomicInteger(0);
        AtomicIntegerArray conn_count_array = new AtomicIntegerArray(n);
        for (int i = 0; i < conn_count_array.length(); i++) {
            conn_count_array.set(i, 0);
        }

        for (int i = 0; i < n; i++) {
            executorService.submit(() -> {
                int tid = atomic_count.incrementAndGet() - 1;
                int conn_count = 0;
                System.out.println("tid=" + tid + " running");
                while (true) {
                    try {
                        Session session = new Session.Builder().host(host).port(6667).username("root").password("root").build();
                        session.open(false);
                        session.testInsertRecord(null, 1, null, null, null);
                    } catch (IoTDBConnectionException e) {
                        System.out.println("Meets exception: ");
                        break;
                    } catch (StatementExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    conn_count++;
                    if (conn_count % 10 == 0) {
                        conn_count_array.set(tid, conn_count);
                    }
                }
            });
        }

        int prev_total_count = 0;
        int sum = 0;
        int count = 0;
        while (true) {
            int total_count = 0;
            for (int i = 0; i < conn_count_array.length(); i++) {
                total_count += conn_count_array.get(i);
            }
            sum += (total_count - prev_total_count);
            count++;
            System.out.println("in 1 second, total_count=" + (total_count - prev_total_count) +
                    ", sum=" + sum + ", avg=" + sum / count);
            prev_total_count = total_count;
            Thread.sleep(1000);
        }
    }
}