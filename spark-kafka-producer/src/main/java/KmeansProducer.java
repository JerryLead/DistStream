
import kafka.producer.KeyedMessage;
import kafka.producer.KeyedMessage$;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Int;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by Chongrui on 2018/5/3 0003.
 */


public class KmeansProducer{

    private String topic;
    private KafkaProducer<String,String> producer;
    //private KafkaProducer<String,Tuple2<String,Integer>> producer;

    //程序运行时长
    public static long runTime = 3 * 60 * 1000;
    public KmeansProducer(String topic){
        super();
        this.topic = topic;
    }

    public void init() {
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "133.133.20.8:9092,133.133.20.10:9093,133.133.20.7:9094,133.133.20.8:9095");
        properties.put("bootstrap.servers", "133.133.20.1:9092,133.133.20.2:9092,133.133.20.3:9092,133.133.20.4:9092,133.133.20.5:9092,133.133.20.6:9092,133.133.20.7:9092,133.133.20.8:9092");
        //properties.put("bootstrap.servers", "133.133.20.8:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
        //producer = new KafkaProducer<String, Tuple2<String, Integer>>(properties);
    }
    /*public void sendMessage(String message) {
        //System.out.println("send to kafka: "+message);
        producer.send(new ProducerRecord<String,String>(topic, message));
        //producer.send(new KeyedMessage<Integer,Int>(message));
    }*/
    public void sendMessage(String message,int par) {
        //System.out.println("send to kafka: "+message);
        //producer.send(new ProducerRecord<String,String>(topic, message));
        //producer.send(new KeyedMessage<Integer,Int>(message));
        producer.send(new ProducerRecord<String,String>(topic,par,null,message));
    }

    /*public void sendMessage(Tuple2<String,Integer> tuple2) {
        //System.out.println("send to kafka: "+message);
        producer.send(new ProducerRecord<String,Tuple2<String,Integer>>(topic, tuple2));
    }*/





    public void close(){
        producer.close();
    }


    public static void throttleNanos(long ratePerSecond, long startTimeNanos, long targetDone) {
        // throttle the operations
        double targetPerMs = ratePerSecond / 1000.0;
        long targetOpsTickNs = (long) (1000000 / targetPerMs);
        //long targetOpsTickNs = (long) (100000000 / targetPerMs); 1000/11000
        //long targetOpsTickNs = (long) (100 / targetPerMs);
        if (targetPerMs > 0) {
            // delay until next tick
            long deadline = startTimeNanos + targetDone * targetOpsTickNs;
            //System.out.println("已经发送"+ targetDone+"数据"+"，现在需要停止"+deadline+"ms");
            sleepUntil(deadline);
        }
    }

    public static void main(String[] args) throws IOException {
        String topic = "kdd";
        // String dataPath = "e:\\data\\kddccup.data_10_percent_train_normalized.csv";
        String dataPath = "/Users/kk/Downloads/lcr/data/data123/kddcup99.csv";
        Long rate = 100000L;
        Long dataCount = 1000L;
        long startTime = 0l;
        int numPartitions = 32;
        int timeInterval = 1000;
        if (args.length > 0) runTime = Long.parseLong(args[0]);
        if (args.length > 1) startTime = Long.parseLong(args[1]);
        if (args.length > 2) dataPath = args[2];
        if (args.length > 3) rate = Long.parseLong(args[3]);
        if (args.length > 4) dataCount = Long.parseLong(args[4]);
        if (args.length > 5) topic = args[5].toString();
        if (args.length > 6) numPartitions = Integer.parseInt(args[6]);
        if (args.length > 7) timeInterval = Integer.parseInt(args[7]);
        KmeansProducer KP = new KmeansProducer(topic);// 使用kafka集群中创建好的主题
        KP.init();
        //读取文件(字符流)
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(dataPath)));
        //BufferedReader in = new BufferedReader(new FileReader("d:\\1.txt")));
        //读取数据
        String str = null;
        //int count = 0;
        int count = -1;
        long start = System.currentTimeMillis();
        System.out.println("Waiting spark initialize. StartTime is " + (startTime / 1000) + " seconds later");
        while (System.currentTimeMillis() - start < startTime) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("after " + (System.currentTimeMillis() - start) / 1000.0 + "s, start kafka streaming");

        start = System.currentTimeMillis();
        long processedCount = 0l;
        int part = 0;
        int key = 0;
        int time = 0;
        while (true){
            //写入kafka生产端
            long targetDone = 0;
            long startTimeNanos = System.nanoTime();
            while (targetDone < rate && processedCount < dataCount) {
                str = in.readLine();
                if (str == null) {
//                    break;
                    in = new BufferedReader(new InputStreamReader(new FileInputStream(dataPath)));
                    str = in.readLine();
//                    System.out.println(count);
                }
                count++;
                time = count/timeInterval;
                str = time+"/"+str;
                key = part%numPartitions;
                //System.out.println("Data Partition is" + key);
                part++;
                //KP.sendMessage(new KeyedMessage<Integer,String>(topic,key,str));
                KP.sendMessage(str,key);
                //KP.sendMessage(Tuple2(str,count));
                ++targetDone;
                ++processedCount;
                throttleNanos(rate,startTimeNanos,targetDone);
                if (System.nanoTime() - startTimeNanos >= 1000000000) break;
            }
            if (str == null || System.currentTimeMillis() - start >= KmeansProducer.runTime || processedCount >= dataCount) break;
        }
        System.out.println("Finish sending " + processedCount + " data, spend " + ((System.currentTimeMillis() - start) / 1000.0) + "s");
        //关闭流和kafka
        in.close();
        KP.close();
    }



    public static void sleepUntil(long deadline) {
        while ((System.nanoTime()) < deadline) {
        }
    }
}