package com.gslab.pepper.test;

import com.gslab.pepper.PepperBoxLoadGenerator;
import kafka.server.KafkaServer;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by satish on 5/3/17.
 */
@Ignore // TODO fix this test
public class PepperBoxLoadGenTest {
    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "test";

    private EmbeddedZookeeper zkServer = null;

    private KafkaServer kafkaServer = null;

//    private ZkClient zkClient = null;

    @Before
    public void setup() throws IOException {

//        zkServer = new EmbeddedZookeeper();
//
//        String zkConnect = ZKHOST + ":" + zkServer.port();
//        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
//        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
//
//        Properties brokerProps = new Properties();
//        brokerProps.setProperty("zookeeper.connect", zkConnect);
//        brokerProps.setProperty("broker.id", "0");
//        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
//        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
//        KafkaConfig config = new KafkaConfig(brokerProps);
//        Time mock = new MockTime();
//        kafkaServer = TestUtils.createServer(config, mock);
//        //AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
//
//        JMeterContext jmcx = JMeterContextService.getContext();
//        jmcx.setVariables(new JMeterVariables());

    }
    @Test
    public void consoleLoadGenTest() throws IOException {
        File schemaFile = File.createTempFile("json", ".schema");
        schemaFile.deleteOnExit();
        FileWriter schemaWriter = new FileWriter(schemaFile);
        schemaWriter.write(TestInputUtils.testSchema);
        schemaWriter.close();

        File producerFile = File.createTempFile("producer", ".properties");
        producerFile.deleteOnExit();
        FileWriter producerPropsWriter = new FileWriter(producerFile);
        producerPropsWriter.write(String.format(TestInputUtils.producerProps, BROKERHOST, BROKERPORT, ZKHOST, zkServer.port()));
        producerPropsWriter.close();

        String vargs []  = new String[]{"--schema-file", schemaFile.getAbsolutePath(), "--producer-config-file", producerFile.getAbsolutePath(), "--throughput-per-producer", "10", "--test-duration", "1", "--num-producers", "1"};
        PepperBoxLoadGenerator.main(vargs);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC));
        ConsumerRecords<String, String> records = consumer.poll(30000);
        Assert.assertTrue("PepperBoxLoadGenerator validation failed", records.count() > 0);

    }

    @After
    public void teardown(){
        kafkaServer.shutdown();
//        zkClient.close();
        zkServer.shutdown();

    }

}
