package org.apache.iotdb;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * 广利核查询POC
 */
public class GuangLiHeQueryTest {
    public static void main(String[] args)
            throws IoTDBConnectionException, StatementExecutionException, InterruptedException {

        Session.Builder builder = new Session.Builder().host("172.20.31.62").port(6667).username("root").password("root");
        Session session = builder.build();
        SessionPool pool = new SessionPool("172.20.31.62", 6667, "root", "root", 20);

        try {
            session.open(false);
            long startTime = System.currentTimeMillis();
            session.executeQueryStatement("select first_value(*) from root.test.g_0.** group by ([2022-01-01T00:00:00.500+08:00, 2022-04-01T00:00:00.000+08:00), 90m)");
            long cost = System.currentTimeMillis() - startTime;
            System.out.println(String.format("single thread cost: %sms", cost));
            session.close();


            CountDownLatch latch = new CountDownLatch(10);
            List<QueryThread> threads = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                session = new Session.Builder().host("172.20.31.62").port(6667).username("root").password("root").build();
                session.open(false);
                String path = String.format("root.test.g_0.d_%s", i);
                QueryThread queryThread = new QueryThread(session, latch, path);
                threads.add(queryThread);
            }

            ExecutorService service = Executors.newFixedThreadPool(10);

            startTime = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                service.submit(threads.get(i));
            }
            latch.await();
            cost = System.currentTimeMillis() - startTime;
            System.out.println(String.format("multi-thread cost: %sms", cost));

        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    static class QueryThread implements Runnable {

        Session session;
        CountDownLatch latch;
        String sql;
        List<String> paths;
        List<TAggregationType> type = Arrays.asList(TAggregationType.FIRST_VALUE, TAggregationType.FIRST_VALUE);
        long startTime = 1640966400500L;
        long endTime = 1648742400000L;
        long step = 5400000;

        QueryThread(Session session, CountDownLatch latch, String path) {
            this.session = session;
            this.latch = latch;
            this.paths = Arrays.asList(path + ".s_0", path + ".s_1");
        }

        @Override
        public void run() {
            try {
                SessionDataSet dataSet = session.executeAggregationQuery(paths, type, startTime, endTime, step);
//                while (dataSet.hasNext()) {
//                    System.out.println(dataSet.next());
//                }
            } catch (StatementExecutionException e) {
                throw new RuntimeException(e);
            } catch (IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        }
    }
}
