package org.apache.iotdb;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

/**
 * 测试最多能创建多少序列
 */
public class CreateMostTimeseriesUsingTemplate {

    static String host = "127.0.0.1";
    static int port = 6667;
    static String user = "root";
    static String passWord = "root";

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host(host)
                .port(port)
                .username(user)
                .password(passWord)
                .version(Version.V_1_0)
                .build();
        session.open(false);
        session.setFetchSize(100000);

//        String createTmpSql = "CREATE SCHEMA TEMPLATE t(";
//        for (int i = 1; i <= 99999; i++) {
//            createTmpSql = createTmpSql + "s" + i + " INT32,";
//        }
//        createTmpSql = createTmpSql + "s100000 INT32);";
//        session.executeNonQueryStatement(createTmpSql);
//        System.out.println(createTmpSql);

        for (int i = 20001; i <= 50000; i++) {
            session.executeNonQueryStatement(String.format("CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg.d%s", i));
        }

        // create database root.sg;
        //

//        String str = "CREATE TIMESERIES OF SCHEMA TEMPLATE ON root.sg.d%s";
//        for (int i = 1; i <= 10000; i++) {
//
//            System.out.print(String.format(str, i));
//        }
        //System.out.println(str);

    }
}
