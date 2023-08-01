package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class AddConnection {
    static String host = "11.101.17.33";

    public static void main(String[] args) throws InterruptedException, IoTDBConnectionException {

        int n = 2;

        ExecutorService executorService = Executors.newFixedThreadPool(n);

        while (n-- > 0) {
            executorService.submit(() -> {
                        for (int i = 0; i < 40000; i++) {
                            Session session = new Session.Builder().host(host).port(6667).username("root").password("root").build();
                            try {
                                session.open(false);
                            } catch (IoTDBConnectionException e) {
                                System.out.println("number: " + i);
                                System.out.println(e);
                            }
                        }
                    }
            );

        }

        TimeUnit.MINUTES.sleep(100);
    }
}
