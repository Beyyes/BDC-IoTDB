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
public class TestIoTDBConcurrent
{

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, InterruptedException
    {
        Scanner scanner = new Scanner(System.in);
        if (args.length == 0) {
            System.out.println("Please provide an argument");
            System.exit(1);
        }
        String my_count = args[0];
        String host = args[1];
        int n = Integer.parseInt(my_count);

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
                // Session session = new Session.Builder().host("127.0.0.1").port(6667).username("root").password("root").build();
                Session.Builder builder = new Session.Builder().host(host).port(6667).username("root").password("root");
                while (true) {
                    Session session = builder.build();
                    try {
                        session.open(false);
                        session.close(); //长连接
                    } catch (IoTDBConnectionException e) {
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
            sum +=  (total_count - prev_total_count);
            count++;
            System.out.println("in 1 second, total_count=" + (total_count - prev_total_count) + ", sum=" + sum + ", avg=" + sum/count);
            prev_total_count = total_count;
            Thread.sleep(1000);
        }
    }
}