package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Create2000WDevice {
    static String host = "172.20.31.27";
    static String user = "root";
    static String passWord = "root";
    static int port = 6667;

    static int num = 1;

    static int step = 2500000;

    public static void main(String[] args) throws IoTDBConnectionException, InterruptedException {

        Session session = new Session.Builder().host(host).port(6667).username("root").password("root").build();

        session.open(false);

        TimeUnit.MINUTES.sleep(100);
    }
}
