＃AdClickStatAnalytics

1。创建topic
 bin/kafka-topics.sh --zookeeper n1.troy.com:2181,n2.troy.com:2181,n3.troy.com:2181 --create --topic adclick --replication-factor 1 --partitions 2
 class AdClickedStatKafkaProducer｛//模拟广告点击事件，基本数据格式timestamp、ip、userID、adID、province、city｝
 2.class AdClickedStreamingStat {
 sparkstreaming+kafka实时分析广告点击事件
 分析广告点击量，TOPN，30分钟间隔的点击趋势，黑名单处理
 }
 3.数据库表设计
 ##reduceByKey 统计用表
 CREATE TABLE adclicked(
timestamp  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,  #点击时间
ip  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,   #访问时的iP
userID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,  # 用户id
adID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,   #广告id
province  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL , #点击广告时所在省份
city  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,   #点击广告时所在城市
clickCount  bigint(255) NULL DEFAULT NULL #点击次数
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
##updateStateBykey 统计用表
CREATE TABLE adclickedclick(
timestamp  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,  #点击时间
ip  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,   #访问时的iP
userID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,  # 用户id
adID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,   #广告id
province  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL , #点击广告时所在省份
city  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,   #点击广告时所在城市
clickCount  bigint(255) NULL DEFAULT NULL #点击次数
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
#黑名单
CREATE TABLE blacklist (
userID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '黑名单编号' ,
PRIMARY KEY (userID)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
#topN 统计表
CREATE TABLE adclickprovincetopn (
timestamp  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
adID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
province  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
clickCount  bigint(255) NULL DEFAULT NULL 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
## 30分钟趋势统计表
CREATE TABLE adclicktrend (
adID  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
clickCount  bigint(255) NULL DEFAULT NULL ,
date  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
hour  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL ,
minute  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci

4.工程打包AdClickStatAnalytics-0.0.1-SNAPSHOT.jar，放入集群目录（这里测试直接放到/opt/）
另外项目插入mysql数据库使用了dbcp连接池 需要引用commons-dbcp-1.4.jar commons-pool-1.6.jar（版本自己定）。也都放入/opt目录
5.AdClickedStatKafkaProducer 执行此类 模拟kafka-producer生产广告点击数据
6. 进入spark2 目录
/bin/spark-submit --class com.licw.sparkApps.sparkStreaming.AdClickedStreamingStat --jars /opt/mysql-connector-java-5.1.39-bin.jar,/opt/commons-dbcp-1.4.jar,/opt/commons-pool-1.6.jar /opt/AdClickStatAnalytics-0.0.1-SNAPSHOT.jar
这里采用的local模式。可自行定义或加如其他参数
