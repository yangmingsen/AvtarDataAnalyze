package top.ccw.avtar.instream;

import com.google.gson.Gson;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import top.ccw.avtar.entity.JobDataOne;
import top.ccw.avtar.instream.clearn.ClearnProcess;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/***
 * 接收kafka数据
 *
 * date: 2019/03/08
 * @author yangmingsen
 */
public class KafkaConsumer {

    //主题
    private static final String topic = "sortware";
    private static final Integer threads = 2;

    public static void inStream() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "yms1:2181,yms2:2181,yms3:2181");
        props.put("group.id", "vvvvv");
        props.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(props);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
            for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){

                //获取kafka中数据
                String msg = new String(mm.message());

                Gson gson = new Gson();
                //得到一个数据对象
                JobDataOne jobDataOne = gson.fromJson(msg,JobDataOne.class);

                //go data clear program
                ClearnProcess.start(jobDataOne);

            }
        }

    }

    public static void main(String[] args) {
        inStream();
    }


}
