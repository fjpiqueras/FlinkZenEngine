package com.indizen.labs.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TestExecutionConfig {

    @Before
    public void init(){

    }

    @Test
    public void testExecutionConfig(){
        ExecutionConfig config = ExecutionConfig.readYamlFile("/home/fjpiqueras/Workspace/FlinkZenEngine/src/test/resources/config.yaml");

        assertEquals(config.getApplicationName(), "FlinkJob1");
        assertEquals(config.getKafkaConfig().get("kafkabrokerurl"), "localhost");
        assertEquals(config.getKafkaConfig().get("schemaregistryurl"), "localhost");

        assertEquals(config.getKafkaTopics().get("topic1"), "deals1");
        assertEquals(config.getKafkaTopics().get("topic2"), "deals2");

        assertEquals(config.getQueries().get("query1"), "select * from deals1");
        assertEquals(config.getQueries().get("query2"), "select * from deals2");
    }
}
