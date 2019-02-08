package com.indizen.labs.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import java.io.File;
import java.util.Map;
import java.util.Properties;

import static com.indizen.labs.constants.Constants.*;

public class ExecutionConfig {

    private String applicationName;

    private Map<String, String> kafkaConfig;
    private Map<String, String> kafkaTopics;
    private Map<String, String> queries;
    private Map<String, String> streamExecutionEnvironmentConfig;


    public static ExecutionConfig readYamlFile(String path){

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ExecutionConfig config = new ExecutionConfig();

        try {
            config = mapper.readValue(new File(path), ExecutionConfig.class);

            System.out.println(ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return config;
    }

    public Properties getKafkaConfiguration(ExecutionConfig config){

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAPSERVERS, config.getKafkaTopics().get(BOOTSTRAPSERVERS));
        properties.setProperty(ZOOKEEPER, config.getKafkaTopics().get(ZOOKEEPER));
        properties.setProperty(GROUPID, config.getKafkaTopics().get(GROUPID));

        return properties;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public Map<String, String> getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(Map<String, String> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public Map<String, String> getKafkaTopics() {
        return kafkaTopics;
    }

    public void setKafkaTopics(Map<String, String> kafkaTopics) {
        this.kafkaTopics = kafkaTopics;
    }

    public Map<String, String> getQueries() {
        return queries;
    }

    public void setQueries(Map<String, String> queries) {
        this.queries = queries;
    }

    public Map<String, String> getStreamExecutionEnviornmentConfig() {
        return streamExecutionEnvironmentConfig;
    }

    public void setStreamExecutionEnviornmentConfig(Map<String, String> streamExecutionEnviornmentConfig) {
        this.streamExecutionEnvironmentConfig = streamExecutionEnviornmentConfig;
    }
}
