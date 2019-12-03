package org.stsffap.state.utils;

import akka.actor.ActorPath;

import java.io.*;
import java.util.Properties;
import java.util.UUID;

public class SystemContext {
    private static String kafkaUrl;
    private static String auto_commit_interval_ms;
    private static String auto_offset_reset;
    private static String max_poll_records;
    private static String key_serializer;
    private static String value_serializer;
    private static String key_deserializer;
    private static String value_deserializer;
    private static String group_id;
    private static ActorPath modelCenterPath;
    private static ActorPath kafkaDataPackageProducerPath;
    private static String datasource_url;
    private static String datasource_username;
    private static String datasource_password;

    public static void loadConfig(String filePath){
        Properties props = new Properties();
        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
            props.load(inputStream);
            kafkaUrl =props.getProperty("kafka.server.url");
            auto_commit_interval_ms = props.getProperty("auto.commit.interval.ms");
            auto_offset_reset = props.getProperty("auto.offset.reset");
            max_poll_records = props.getProperty("max.poll.records");
            key_serializer=props.getProperty("key.serializer");
            value_serializer = props.getProperty("value.serializer");
            key_deserializer = props.getProperty("key.deserializer");
            value_deserializer = props.getProperty("value.deserializer");
            group_id = props.getProperty("group.id");
            datasource_url = props.getProperty("datasource.url");
            datasource_username = props.getProperty("datasource.username");
            datasource_password = props.getProperty("datasource.password");
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getKafkaUrl() {
        return kafkaUrl;
    }

    public static String getAuto_commit_interval_ms() {
        return auto_commit_interval_ms;
    }

    public static String getAuto_offset_reset() {
        return auto_offset_reset;
    }

    public static String getMax_poll_records() {
        return max_poll_records;
    }

    public static String getKey_serializer() {
        return key_serializer;
    }

    public static String getValue_serializer() {
        return value_serializer;
    }

    public static String getKey_deserializer() {
        return key_deserializer;
    }

    public static String getValue_deserializer() {
        return value_deserializer;
    }

    public static String getGroup_id() {
        return group_id;
    }

    public static String getDatasource_url() {
        return datasource_url;
    }

    public static String getDatasource_username() {
        return datasource_username;
    }

    public static String getDatasource_password() {
        return datasource_password;
    }

    public static ActorPath getModelCenterPath() {
        return modelCenterPath;
    }

    public static void setModelCenterPath(ActorPath modelCenterPath) {
        SystemContext.modelCenterPath = modelCenterPath;
    }

    public static ActorPath getKafkaDataPackageProducerPath() {
        return kafkaDataPackageProducerPath;
    }

    public static void setKafkaDataPackageProducerPath(ActorPath kafkaDataPackageProducerPath) {
        SystemContext.kafkaDataPackageProducerPath = kafkaDataPackageProducerPath;
    }

    public static Properties getKafkaProperties(String name) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaUrl);
        properties.put("auto.commit.interval.ms", auto_commit_interval_ms);
        properties.put("auto.offset.reset", auto_offset_reset);
        properties.put("max.poll.records", max_poll_records);
        properties.put("key.deserializer", key_deserializer);
        properties.put("value.deserializer", value_deserializer);
        properties.put("key.serializer", key_serializer);
        properties.put("value.serializer", value_serializer);
        properties.put("group.id", SystemContext.getGroup_id() + "_" + name + "_" + UUID.randomUUID().toString());
        return properties;
    }
}
