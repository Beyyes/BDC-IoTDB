package org.apache.iotdb;

import okhttp3.OkHttpClient;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.concurrent.TimeUnit;

public class InfluxDBCli {

    static String sql = "select * from g_0 order by time desc limit 10";

    private static org.influxdb.InfluxDB influxDbInstance;

    public static void main(String[] args) {

        String influxUrl = "http://172.20.31.25:8086";
        String influxDbName = "test";

        OkHttpClient.Builder client =
                new OkHttpClient.Builder()
                        .connectTimeout(5, TimeUnit.MINUTES)
                        .readTimeout(5, TimeUnit.MINUTES)
                        .writeTimeout(5, TimeUnit.MINUTES)
                        .retryOnConnectionFailure(true);
        influxDbInstance = org.influxdb.InfluxDBFactory.connect(influxUrl, client);

        long start = System.nanoTime();
        QueryResult results = influxDbInstance.query(new Query(sql, influxDbName));
        System.out.println(String.format("查询耗时: %s", System.nanoTime() - start));
    }
}
