package org.apache.iotdb;

import ch.qos.logback.core.util.AggregationType;
import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {

    private static String LAST_QUERY = "last";

    private static String AVG_QUERY = "avg";

    private static String LAST_QUERY_SQL = "select last(Value4) from root.vehicle.*";

    private static Session session;

    private static List<String> aggregatePaths =
            Arrays.asList("Value6", "Value7", "Value21", "Value22", "Value23", "Value26", "Value28", "Value30", "Value35", "Value41");

    private static List<TAggregationType> aggregationTypes = Collections.singletonList(TAggregationType.AVG);

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {

        // host port deviceId last/avg times print-response
        if (args.length != 5) {
            System.out.println("The size of parameters must be five! host, port, deviceId, queryType, times");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String deviceId = args[2];
        String queryType = args[3];
        int times = Integer.parseInt(args[4]);

        if (!LAST_QUERY.equalsIgnoreCase(queryType) && !AVG_QUERY.equalsIgnoreCase(queryType)) {
            System.out.println("QueryType must be last or avg!");
            return;
        }

        executeQuery(host, port, deviceId, queryType, times, false);

        System.out.println("Hello world!");

    }

    public static void executeQuery(String host, int port, String deviceId, String queryType, int times, boolean printResponse) throws IoTDBConnectionException, StatementExecutionException {
        session =
                new Session.Builder()
                        .host(host)
                        .port(port)
                        .username("root")
                        .password("root")
                        .version(Version.V_1_0)
                        .build();
        session.open(false);

        // set session fetchSize
        session.setFetchSize(20000);

        if (LAST_QUERY.equalsIgnoreCase(queryType)) {
            long allTime = 0, minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
            for (int i = 1; i <= times; i++) {
                long startTime = System.currentTimeMillis();
                SessionDataSet dataSet = session.executeQueryStatement(LAST_QUERY_SQL);
                long costTime = System.currentTimeMillis() - startTime;
                allTime += costTime;
                minTime = Math.min(minTime, costTime);
                maxTime = Math.max(maxTime, costTime);
                System.out.printf("Execute result, sql: %s, cost time: %sms", LAST_QUERY_SQL, costTime);
                if (printResponse) {
                    while (dataSet.hasNext()) {
                        System.out.println(dataSet.next());
                    }
                }
            }

            System.out.printf("Execute sql: %s after %s times, min cost time: %sms, " +
                            "max cost time: %sms, all cost time: %sms, avg cost time: %sms%n",
                    LAST_QUERY_SQL, times, minTime, maxTime, allTime, allTime/(double)times);
        }


        if (AVG_QUERY.equalsIgnoreCase(queryType)) {
            long allTime = 0, minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
            for (int i = 1; i <= times; i++) {

                // SELECT avg(Value6) FROM root.vehicle.VIN_X WHERE time ~ 1d
                long queryStartTime = 0, queryEndTime = 1;
                for (String path : aggregatePaths) {
                    long time0 = System.currentTimeMillis();
                    SessionDataSet dataSet = session.executeAggregationQuery(Collections.singletonList(path), aggregationTypes, queryStartTime, queryEndTime);
                    long costTime = System.currentTimeMillis() - time0;
                    allTime += costTime;
                    System.out.println(String.format("Execute result, sql: %s, cost time: %sms", LAST_QUERY_SQL, costTime));
                    printResponse(dataSet, printResponse);
                }

                long startTime = System.currentTimeMillis();
                SessionDataSet dataSet = session.executeQueryStatement(LAST_QUERY_SQL);
                long costTime = System.currentTimeMillis() - startTime;
                allTime += costTime;
                minTime = Math.min(minTime, costTime);
                maxTime = Math.max(maxTime, costTime);
                System.out.printf("Execute result, sql: %s, cost time: %sms", LAST_QUERY_SQL, costTime);

            }
        }
    }

    public static void printResponse(SessionDataSet dataSet, boolean print)
            throws IoTDBConnectionException, StatementExecutionException {
        if (print) {
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        }
    }

}