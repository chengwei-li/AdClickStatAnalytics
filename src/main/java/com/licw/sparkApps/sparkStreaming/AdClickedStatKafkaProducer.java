package com.licw.sparkApps.sparkStreaming;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;





/**
 * 
 * @author Administrator
 * kafka producer产生数据，模拟用户点击事件
 */
public class AdClickedStatKafkaProducer {
	public static void main(String[] args) {
		
	
	 final Random random = new Random();
     final String[] provinces = new String[]{"Guangdong", "Zhejiang", "Jiangsu", "Fujian"};
     final Map<String, String[]> cities = new HashMap<String, String[]>();
     cities.put("Guangdong", new String[]{"Guangzhou", "Shenzhen", "Dongguan"});
     cities.put("Zhejiang", new String[]{"Hangzhou", "Wenzhou", "Ningbo"});
     cities.put("Jiangsu", new String[]{"Nanjing", "Suzhou", "Wuxi"});
     cities.put("Fujian", new String[]{"Fuzhou", "Xiamen", "Sanming"});

     final String[] ips = new String[] {
             "192.168.112.240",
             "192.168.112.239",
             "192.168.112.245",
             "192.168.112.246",
             "192.168.112.247",
             "192.168.112.248",
             "192.168.112.249",
             "192.168.112.250",
             "192.168.112.251",
             "192.168.112.252",
             "192.168.112.253",
             "192.168.112.254",
     };
     /**
      * Kafka相关的基本配置信息
      */
     Properties kafkaConf = new Properties();
     
     //server地址只能用IP，不能用hostname
     kafkaConf.put("bootstrap.servers", "10.0.40.50:6667");
     kafkaConf.put("acks", "all");
     kafkaConf.put("retries", 0);
     kafkaConf.put("batch.size", 16384);
     kafkaConf.put("linger.ms", 1);
     kafkaConf.put("buffer.memory", 33554432);
     kafkaConf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     kafkaConf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
   //  ProducerConfig producerConfig = new ProducerConfig(kafkaConf);
     final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaConf);
     new Thread(new Runnable() {

         public void run() {
             while(true) {
                 //在线处理广告点击流的基本数据格式：timestamp、ip、userID、adID、province、city
                 Long timestamp = new Date().getTime();
                 String ip = ips[random.nextInt(12)]; //可以采用网络上免费提供的ip库
                 int userID = random.nextInt(10000);
                 int adID = random.nextInt(100);
                 String province = provinces[random.nextInt(4)];
                 String city = cities.get(province)[random.nextInt(3)];
                 String clickedAd = timestamp + "\t" + ip + "\t" + userID + "\t" + adID + "\t" + province + "\t" + city;
                 producer.send(new ProducerRecord<String, String>( "adclick",clickedAd));
                 System.out.println(clickedAd);
                 try {
                     Thread.sleep(50);
                 } catch (InterruptedException e) {
                     // TODO Auto-generated catch block
                     e.printStackTrace();
                 }
             }
         }
     }).start();
 }

}

