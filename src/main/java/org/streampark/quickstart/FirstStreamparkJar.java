package org.streampark.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FirstStreamparkJar {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        String source = "create table source(\n" +
                "  id string,\n" +
                "  data string\n" +
                ") with(\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'streamx_test_source',\n" +
                "  'properties.bootstrap.servers' = '10.250.250.7:9092',\n" +
                "  'properties.group.id' = 'streamx_test_source',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        String sink = "create table sink(\n" +
                "  id string,\n" +
                "  data string\n" +
                ") with(\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'streamx_test_sink',\n" +
                "  'properties.bootstrap.servers' = '10.250.250.7:9092',\n" +
                "  'format' = 'csv'\n" +
                ")";
        String insert = "insert into sink select * from source";
        tableEnv.executeSql(source).print();
        tableEnv.executeSql(sink).print();
        tableEnv.executeSql(insert).print();
    }
}
