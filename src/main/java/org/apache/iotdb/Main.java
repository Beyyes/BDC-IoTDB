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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Main {

    private static final String LAST_QUERY = "last";

    private static final String AVG_QUERY = "avg";

    private static final String LAST_QUERY_SQL = "select last(Value4) from root.vehicle.*";

    private static Session session;

    private static final List<String> aggregatePaths =
            Arrays.asList("Value6", "Value7", "Value21", "Value22", "Value23", "Value26", "Value28", "Value30", "Value35", "Value41");

    private static final String AVG_QUERY_SQL = "select avg(%s) from root.vehicle.xx where time>%s and time<%s";

    static final String HOST_ARGS = "h";
    static final String HOST_NAME = "host";

    static final String HELP_ARGS = "help";

    static final String PORT_ARGS = "p";
    static final String PORT_NAME = "port";

    static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    static final String USERNAME_ARGS = "u";
    static final String USERNAME_NAME = "username";

    static final String START_TIME_ARGS = "start";
    static final String START_TIME_NAME = "startTime";


    static final String END_TIME_ARGS = "end";
    static final String END_TIME_NAME = "endTime";

    static final String RPC_COMPRESS_ARGS = "c";
    private static final String RPC_COMPRESS_NAME = "rpcCompressed";

    static final String MAX_PRINT_ROW_COUNT_ARGS = "maxPRC";
    private static final String MAX_PRINT_ROW_COUNT_NAME = "maxPrintRowCount";

    private static final String EXECUTE_ARGS = "e";
    private static final String EXECUTE_NAME = "execute";

    private static final String DEVICE_ARGS = "device";
    private static final String DEVICE_NAME = "deviceId";

    private static final String QUERY_TYPE_ARGS = "type";
    private static final String QUERY_TYPE_NAME = "queryType";

    static final String REPEAT_ARGS = "repeat";
    static final String REPEAT_NAME = "repeatTimes";

    static final String PRINT_RESPONSE_ARGS = "print";
    static final String PRINT_RESPONSE_NAME = "printResponse";

    static String host;
    static int port;
    static String user = "root";
    static String passWord = "root";
    static String queryType;
    static int repeat;
    static String deviceId;
    static long startTime;
    static long endTime;
    static boolean printResponse;
    static int maxPrintCount;


    private static List<TAggregationType> aggregationTypes = Collections.singletonList(TAggregationType.AVG);

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {

        Options options = createOptions();

        if (args == null || args.length == 0) {
            System.out.println("no args");
            return;
        }

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            host = commandLine.getOptionValue(HOST_ARGS, "127.0.0.1");
            port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, "6667"));
            queryType = commandLine.getOptionValue(QUERY_TYPE_ARGS);
            repeat = Integer.parseInt(commandLine.getOptionValue(REPEAT_ARGS, "1"));
            startTime = Long.parseLong(commandLine.getOptionValue(START_TIME_ARGS));
            endTime = Long.parseLong(commandLine.getOptionValue(END_TIME_ARGS));
            maxPrintCount = Integer.parseInt(commandLine.getOptionValue(MAX_PRINT_ROW_COUNT_ARGS, "20000"));
            printResponse = Boolean.parseBoolean(commandLine.getOptionValue(PRINT_RESPONSE_ARGS, "false"));

        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        if (!LAST_QUERY.equalsIgnoreCase(queryType) && !AVG_QUERY.equalsIgnoreCase(queryType)) {
            System.out.println("QueryType must be last or avg!");
            return;
        }

        executeQuery(host, port, deviceId, queryType, repeat, printResponse);
    }

    public static void executeQuery(String host,
                                    int port,
                                    String deviceId,
                                    String queryType,
                                    int repeatTimes,
                                    boolean printResponse)
            throws IoTDBConnectionException, StatementExecutionException {
        session =
                new Session.Builder()
                        .host(host)
                        .port(port)
                        .username(user)
                        .password(passWord)
                        .version(Version.V_1_0)
                        .build();
        session.open(false);

        session.setFetchSize(maxPrintCount);

        if (LAST_QUERY.equalsIgnoreCase(queryType)) {
            long allTime = 0, minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
            for (int i = 1; i <= repeatTimes; i++) {
                long startTime = System.currentTimeMillis();
                SessionDataSet dataSet = session.executeQueryStatement(LAST_QUERY_SQL);
                long costTime = System.currentTimeMillis() - startTime;
                allTime += costTime;
                minTime = Math.min(minTime, costTime);
                maxTime = Math.max(maxTime, costTime);
                System.out.printf("Execute result, sql: %s, cost time: %sms", LAST_QUERY_SQL, costTime);
                printResponse(dataSet, printResponse);
            }

            System.out.printf("Execute sql: %s after %s times, min cost time: %sms, " +
                            "max cost time: %sms, all cost time: %sms, avg cost time: %sms%n",
                    LAST_QUERY_SQL, repeatTimes, minTime, maxTime, allTime, allTime / (double) repeatTimes);
        }


        if (AVG_QUERY.equalsIgnoreCase(queryType)) {

            for (int i = 1; i <= repeatTimes; i++) {
                // SELECT avg(Value6) FROM root.vehicle.VIN_X WHERE time ~ 1d
                executeAvgQuery(aggregatePaths, startTime, endTime, false);
            }
        }

        session.close();
    }

    public static void executeAvgQuery(List<String> aggregatePaths,
                                       long queryStartTime,
                                       long queryEndTime,
                                       boolean printResponse) throws IoTDBConnectionException, StatementExecutionException {
        long allTime = 0;
        for (String path : aggregatePaths) {
            long time0 = System.currentTimeMillis();
            SessionDataSet dataSet = session.executeAggregationQuery(Collections.singletonList(path), aggregationTypes,
                    queryStartTime, queryEndTime);
            long costTime = System.currentTimeMillis() - time0;
            allTime += costTime;
            System.out.printf("Execute result, sql: %s, cost time: %sms%n",
                    String.format(AVG_QUERY_SQL, path, queryStartTime, queryEndTime), costTime);
            printResponse(dataSet, printResponse);
        }
        System.out.printf("Execute result, all cost time: %sms%n", allTime);
    }

    public static void printResponse(SessionDataSet dataSet, boolean print)
            throws IoTDBConnectionException, StatementExecutionException {
        if (print) {
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
                Option.builder(MAX_PRINT_ROW_COUNT_ARGS)
                        .argName(MAX_PRINT_ROW_COUNT_NAME)
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
                        .desc("Repeat Times")
                        .build();
        options.addOption(queryType);

        return options;
    }

}