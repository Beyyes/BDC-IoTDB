package org.apache.iotdb;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.rpc.*;
import org.apache.iotdb.session.*;

public class IoTDBPerCorePerformance {
    static String host = "172.20.31.60";
    static int threadCount = 8;

    public static void main(String[] args) throws InterruptedException {
        AtomicInteger atomicCount = new AtomicInteger(0);
        AtomicIntegerArray reqCountArr = new AtomicIntegerArray(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                int tid = atomicCount.incrementAndGet() - 1;
                System.out.println("Thread " + tid + " is running");
                Session session = new Session.Builder().host(host).port(6667).username("root").password("root").build();
                try {
                    int rowCount = 0;
                    session.open(false);
                    String deviceId = "root.db.dev" + tid;
                    long timestamp = 1000;
                    List<String> measurements = Collections.singletonList("s1");
                    List<TSDataType> types = Collections.singletonList(TSDataType.INT32);

                    while (true) {
                        List<Object> values = Collections.singletonList(rowCount);
                        session.testInsertRecord(deviceId, timestamp++, measurements, types, values);
                        rowCount++;
                        if (rowCount % 10 == 0) {
                            reqCountArr.set(tid, rowCount);
                        }
                    }
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    System.out.println(e);
                }
            });
            thread.start();
        }
        Thread.sleep(3000);
        int prevTotalCount = 0, sum = 0, count = 0;
        while (true) {
            int totalCount = 0;
            for (int i = 0; i < reqCountArr.length(); i++) {
                totalCount += reqCountArr.get(i);
            }
            sum += (totalCount - prevTotalCount);
            count++;
            System.out.println("in 1 second, total_count=" + (totalCount - prevTotalCount) + ", " +
                    "sum=" + sum + ", avg=" + sum / count);
            prevTotalCount = totalCount;
            Thread.sleep(1000);
        }
    }
}