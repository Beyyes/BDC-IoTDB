package org.apache.iotdb;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConnectionExample {

    static String host = "11.101.17.33";
    static String user = "root";
    static String passWord = "root";

    public static void main(String[] args) throws IoTDBConnectionException, InterruptedException {

        if (args == null || args.length == 0) {
            System.out.println("no input parameters, please input parameters");
            return;
        }

        host = args[0];
        int port = Integer.parseInt(args[1]);
        int num = Integer.parseInt(args[2]);

        List<Session> sessionList = new ArrayList<>();

        for (int i = 1; i <= num; i++) {
            Session session = new Session.Builder()
                    .host(host)
                    .port(port)
                    .username(user)
                    .password(passWord)
                    .version(Version.V_1_0)
                    .build();
            session.open(false);
            sessionList.add(session);
            if (i % 1000 == 0) {
                System.out.println("connection " + i + " established");
            }
        }

        for (Session session : sessionList) {
            session.getFetchSize();
        }
        TimeUnit.MINUTES.sleep(1000);

    }
}
