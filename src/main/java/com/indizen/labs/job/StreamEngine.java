package com.indizen.labs.job;

import com.indizen.labs.config.ExecutionConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.indizen.labs.constants.Constants.*;

public class StreamEngine{


    private static final Logger logger = LoggerFactory.getLogger(StreamEngine.class);
    private static ExecutionConfig config;
    private static StreamExecutionEnvironment env;
    private String runningMode;

    public StreamEngine(ExecutionConfig config, String runningMode){

        this.config = config;
        this.runningMode = runningMode;
    }

    public void init(){

        createStream(getOptimizedExecutionEnvironment(runningMode));

        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), config.getKafkaConfiguration(config)));

    }

    public DataStream<GenericRecord> createStream(StreamExecutionEnvironment env){

        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<>(...);
        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        myConsumer.setStartFromLatest();       // start from the latest record
        myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
        myConsumer.setStartFromGroupOffsets(); // the default behaviour

        DataStream<String> stream = env.addSource(myConsumer);

        FlinkKafkaConsumer010<GenericRecord> consumer = new FlinkKafkaConsumer010<GenericRecord>(
                config.getKafkaTopics().get(0),
                serdeDealMessage, config.getKafkaConfiguration());

        DataStream<GenericRecord> stream = env.addSource(consumer);

        return null;
    }

    public void createProcess(){

    }

    public void createSink(){

    }

    public static StreamExecutionEnvironment getOptimizedExecutionEnvironment(String mode){

        if(mode.equalsIgnoreCase("LOCAL"))
            return StreamExecutionEnvironment.getExecutionEnvironment();
        else {
            Configuration cfg = new Configuration();
            Path checkpointsDir = new org.apache.flink.core.fs.Path(config.getStreamExecutionEnviornmentConfig().get(CHECKPOINTSDIR));
            Path backendDir = new org.apache.flink.core.fs.Path(config.getStreamExecutionEnviornmentConfig().get(BACKENDDIR));

            cfg.setString("state.backend", STATEBACKEND);
            cfg.setString("state.checkpoints.dir", CHECKPOINTSDIR);
            cfg.setString("state.backend.fs.checkpointdir", BACKENDDIR);
            env = StreamExecutionEnvironment.createLocalEnvironment(1, cfg);
        }

        CheckpointConfig configCheckpoint = env.getCheckpointConfig();
        env.enableCheckpointing(Long.parseLong(config.getStreamExecutionEnviornmentConfig().get(MILLISCHECKPOINTS)));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        configCheckpoint.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        configCheckpoint.setMinPauseBetweenCheckpoints(Long.parseLong(config.getStreamExecutionEnviornmentConfig().get(MILLISPAUSEBETWEENCHECKPOINTS)));
        configCheckpoint.setCheckpointTimeout(Long.parseLong(config.getStreamExecutionEnviornmentConfig().get(CHECKPOINTTIMEOUT)));
        configCheckpoint.setMaxConcurrentCheckpoints(Integer.parseInt(config.getStreamExecutionEnviornmentConfig().get(MAXCONCURRENTCHECKPOINTS)));
        configCheckpoint.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;
    }
}
