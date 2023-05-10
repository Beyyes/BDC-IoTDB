package org.apache.iotdb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {

    static final String LAST_QUERY = "last";

    static final String AVG_QUERY = "avg";

    static final String LAST_QUERY_SQL = "select last(value4) from root.vehicle.g_0.*";

    static final String FORMAT_LAST_QUERY_SQL = "select last(value4) from %s.*";

    static final String LAST_QUERY_SQL_WITH_TIME_RANGE = "select last(value4) from root.** where time>=1262336399000 and time<=1262336399000";

    static final String FORMAT_LAST_QUERY_RPC = "%s.*.value4";

    static Session session;

    static final List<String> aggregatePaths =
            Arrays.asList("value6", "value7", "value21", "value22", "value23", "value26", "value28", "value30", "value35", "value41");

    static final String AVG_QUERY_SQL = "select avg(%s) where time>=%s and time<=%s";

    static final String DEFAULT_DEVICE_ID = "root.vehicle.g_0.LSVNV2182E2119996";

    static final int WARM_UP_NUM = 3;

    static final String HOST_ARGS = "h";
    static final String HOST_NAME = "host";

    static final String HELP_ARGS = "help";

    static final String PORT_ARGS = "p";
    static final String PORT_NAME = "port";

    static final String PASSWORD_ARGS = "pw";
    static final String PASSWORD_NAME = "password";

    static final String USERNAME_ARGS = "u";
    static final String USERNAME_NAME = "username";

    static final String START_TIME_ARGS = "start";
    static final String START_TIME_NAME = "startTime";


    static final String END_TIME_ARGS = "end";
    static final String END_TIME_NAME = "endTime";

    static final String RPC_COMPRESS_ARGS = "c";
    static final String RPC_COMPRESS_NAME = "rpcCompressed";

    static final String FETCH_SIZE_ARGS = "fetch";
    static final String FETCH_SIZE_NAME = "fetchSize";

    static final String EXECUTE_ARGS = "e";
    static final String EXECUTE_NAME = "execute";

    static final String DEVICE_ARGS = "device";
    static final String DEVICE_NAME = "deviceId";

    static final String QUERY_TYPE_ARGS = "type";
    static final String QUERY_TYPE_NAME = "queryType";

    static final String REPEAT_ARGS = "repeat";
    static final String REPEAT_NAME = "repeatTimes";

    static final String PRINT_RESPONSE_ARGS = "print";
    static final String PRINT_RESPONSE_NAME = "printResponse";

    static String host;
    static int port;
    static String user = "root";
    static String passWord = "root";
    static String queryType;
    static int repeatTimes;
    static String deviceId = "";
    static long startTime;
    static long endTime;
    static boolean printResponse;
    static int fetchSize;

    static final List<TAggregationType> aggregationTypes = Collections.singletonList(TAggregationType.AVG);

    static final long LARGE_DATA_START_TIME = 1199174400000L;
    static final long DAY_STEP = 86400000L;
    static final long WEEK_STEP = 86400000 * 7L;
    static final long MONTH_STEP = 86400000 * 30L;

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {

        Options options = createOptions();

        if (args == null || args.length == 0) {
            System.out.println("no input parameters, please input parameters");
            return;
        }

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            host = commandLine.getOptionValue(HOST_ARGS, "127.0.0.1");
            port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, "6667"));
            queryType = commandLine.getOptionValue(QUERY_TYPE_ARGS);
            repeatTimes = Integer.parseInt(commandLine.getOptionValue(REPEAT_ARGS, "1"));
            startTime = Long.parseLong(commandLine.getOptionValue(START_TIME_ARGS, "1"));
            endTime = Long.parseLong(commandLine.getOptionValue(END_TIME_ARGS, "2"));
            fetchSize = Integer.parseInt(commandLine.getOptionValue(FETCH_SIZE_ARGS, "20000"));
            printResponse = Boolean.parseBoolean(commandLine.getOptionValue(PRINT_RESPONSE_ARGS, "false"));
            deviceId = commandLine.getOptionValue(DEVICE_ARGS, "");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        if (!LAST_QUERY.equalsIgnoreCase(queryType) && !AVG_QUERY.equalsIgnoreCase(queryType)) {
            System.out.println("QueryType must be LAST or AVG!");
            return;
        }

        executeQuery();
    }

    public static void executeQuery()
            throws IoTDBConnectionException, StatementExecutionException {
        session = new Session.Builder()
                .host(host)
                .port(port)
                .username(user)
                .password(passWord)
                .version(Version.V_1_0)
                .build();
        session.open(false);
        session.setFetchSize(fetchSize);

        if (LAST_QUERY.equalsIgnoreCase(queryType)) {
            executeLastQuery();
        }

        if (AVG_QUERY.equalsIgnoreCase(queryType)) {
            for (int i = 1; i <= repeatTimes; i++) {
                for (String path : aggregatePaths) {
                    executeAvgQuery(path, DAY_STEP);
                }
                for (String path : aggregatePaths) {
                    executeAvgQuery(path, WEEK_STEP);
                }
                for (String path : aggregatePaths) {
                    executeAvgQuery(path, MONTH_STEP);
                }
            }
        }

        session.close();
    }

    public static void executeLastQuery() throws IoTDBConnectionException, StatementExecutionException {
        long allTime = 0, minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
        for (int idx = 1 - WARM_UP_NUM; idx <= repeatTimes; idx++) {

            List<String> paths = new ArrayList<>();
            paths.add("root.realtime1.*.value4");
            paths.add("root.realtime2.r_0.*.value4");
            paths.add("root.realtime3.r_0.*.value4");
            long startTime = System.currentTimeMillis();
            SessionDataSet dataSet = session.executeLastDataQuery(paths);
            long costTime = System.currentTimeMillis() - startTime;

//            long startTime = System.currentTimeMillis();
//            SessionDataSet dataSet = session.executeQueryStatement(LAST_QUERY_SQL_WITH_TIME_RANGE);
//            long costTime = System.currentTimeMillis() - startTime;

            if (idx >= 1) {
                allTime += costTime;
                minTime = Math.min(minTime, costTime);
                maxTime = Math.max(maxTime, costTime);

                String lastSql = LAST_QUERY_SQL;
                if (!deviceId.isEmpty()) {
                    lastSql = String.format(FORMAT_LAST_QUERY_SQL, deviceId);
                }
                System.out.printf("Execute last query for %s times, sql: %s, cost time: %sms%n",
                        idx, lastSql, costTime);
                printResponse(dataSet);
            }
        }

        System.out.printf("Execute last query after %s times, min cost time: %sms, " +
                        "max cost time: %sms, all cost time: %sms, avg cost time: %sms%n",
                repeatTimes, minTime, maxTime, allTime, allTime / (double) repeatTimes);
    }


    public static void executeAvgQuery(String path,
                                       long step) throws IoTDBConnectionException, StatementExecutionException {

        if (deviceId.isEmpty()) {
            deviceId = DEFAULT_DEVICE_ID;
        }
        String fullPath = deviceId + "." + path;

        long allTime = 0;
        long sTime = LARGE_DATA_START_TIME, eTime;
        int cnt = 0;
        for (; cnt < 10; cnt++) {
            eTime = sTime + step;
            long time0 = System.currentTimeMillis();
            SessionDataSet dataSet = session.executeAggregationQuery(Collections.singletonList(fullPath), aggregationTypes,
                    sTime, eTime);
            long costTime = System.currentTimeMillis() - time0;
            allTime += costTime;
            sTime = eTime;
            printResponseWithTime(dataSet, fullPath, sTime, eTime);
        }

        System.out.printf("Execute avg query result, path: %s, step: %sday, all cost time: %sms, avg cost time: %sms%n",
                fullPath, step/86400000, allTime, allTime / 10.0);
    }

    private static void printResponse(SessionDataSet dataSet)
            throws IoTDBConnectionException, StatementExecutionException {
        if (printResponse) {
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        }
    }

    private static void printResponseWithTime(SessionDataSet dataSet, String path, long sTime, long eTime)
            throws IoTDBConnectionException, StatementExecutionException {
        if (printResponse) {
            System.out.println(String.format(AVG_QUERY_SQL, path, sTime, eTime));
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        }
    }

    static Options createOptions() {
        Options options = new Options();
        Option help = new Option(HELP_ARGS, false, "Display help information(optional)");
        help.setRequired(false);
        options.addOption(help);

        Option queryType =
                Option.builder(QUERY_TYPE_ARGS)
                        .hasArg()
                        .argName(QUERY_TYPE_NAME)
                        .desc("Query Type, Last or AVG")
                        .build();
        options.addOption(queryType);

        Option startTimes =
                Option.builder(START_TIME_ARGS)
                        .hasArg()
                        .argName(START_TIME_NAME)
                        .desc("Query Start Time")
                        .build();
        options.addOption(startTimes);

        Option endTimes =
                Option.builder(END_TIME_ARGS)
                        .hasArg()
                        .argName(END_TIME_NAME)
                        .desc("Query End Time")
                        .build();
        options.addOption(endTimes);

        Option printResponse =
                Option.builder(PRINT_RESPONSE_ARGS)
                        .hasArg()
                        .argName(PRINT_RESPONSE_NAME)
                        .desc("Whether to print response")
                        .build();
        options.addOption(printResponse);

        Option host =
                Option.builder(HOST_ARGS)
                        .argName(HOST_NAME)
                        .hasArg()
                        .required()
                        .desc("Host Name (optional, default 127.0.0.1)")
                        .build();
        options.addOption(host);

        Option port =
                Option.builder(PORT_ARGS)
                        .argName(PORT_NAME)
                        .desc("Port (optional, default 6667)")
                        .build();
        options.addOption(port);

        Option username =
                Option.builder(USERNAME_ARGS)
                        .argName(USERNAME_NAME)
                        .desc("User name (optinal, default root)")
                        .build();
        options.addOption(username);

        Option password =
                Option.builder(PASSWORD_ARGS)
                        .argName(PASSWORD_NAME)
                        .hasArg()
                        .desc("password (optional)")
                        .build();
        options.addOption(password);

        Option execute =
                Option.builder(EXECUTE_ARGS)
                        .argName(EXECUTE_NAME)
                        .hasArg()
                        .desc("execute statement (optional)")
                        .build();
        options.addOption(execute);

        Option maxPrintCount =
                Option.builder(FETCH_SIZE_ARGS)
                        .argName(FETCH_SIZE_NAME)
                        .hasArg()
                        .desc("Maximum number of rows displayed (optional)")
                        .build();
        options.addOption(maxPrintCount);

        Option isRpcCompressed =
                Option.builder(RPC_COMPRESS_ARGS)
                        .argName(RPC_COMPRESS_NAME)
                        .desc("Rpc Compression enabled or not")
                        .build();
        options.addOption(isRpcCompressed);

        Option repeatTimes =
                Option.builder(REPEAT_ARGS)
                        .argName(REPEAT_NAME)
                        .hasArg()
                        .desc("Repeat Times")
                        .build();
        options.addOption(repeatTimes);

        Option device =
                Option.builder(DEVICE_ARGS)
                        .argName(DEVICE_NAME)
                        .hasArg()
                        .desc("DeviceId")
                        .build();
        options.addOption(device);

        return options;
    }

}