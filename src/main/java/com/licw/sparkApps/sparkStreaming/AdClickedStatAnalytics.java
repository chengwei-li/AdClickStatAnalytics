package com.licw.sparkApps.sparkStreaming;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.licw.sparkApps.Utils.DbcpPool;
import com.licw.sparkApps.Utils.ExecuteCallBack;


import scala.Tuple2;

/**
 * 
 * @author Administrator
 * 修正版本：
 * 直接采用updateStateBykey算子统计点击量，去除了reduceByKey算子
 */
public class AdClickedStatAnalytics {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("AdClickedStreamingStat").setMaster("local[2]");
		String checkpint="hdfs://n1.troy.com:8020/lcwtest/SparkStreaming/Checkpoint_adclick/"; //暂放本地，真实环境一般多放于hdfs
		/**
		 * 第二步 创建SparkStreamingContext
		 * 
		 */
	
		JavaStreamingContext jsc = new JavaStreamingContext(conf,Durations.seconds(5));
		jsc.checkpoint(checkpint);
		//kafka config
		String topics = "adclick";
		Map<String,Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("metadata.broker.list", topics) ;
        kafkaParams.put("bootstrap.servers","10.0.40.50:6667" );
        kafkaParams.put("group.id", "adclick-group3");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //topic list
        Collection<String> topic= Arrays.asList(topics.split(","));
        
        // topic partition
        //Map<TopicPartition,Long> offsets = new HashMap<TopicPartition, Long>();
        //offsets.put(new TopicPartition("adclick", 0), 2L);
        /**
         * 第三步：创建Spark Streaming输入数据来源input Stream：
         * 
         * kafka读过来的数据是ConsumerRecord类型
         */
        JavaInputDStream<ConsumerRecord<String, String>> consumerRecords = 
        KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferBrokers(), 
        		ConsumerStrategies.<String,String>Subscribe(topic, kafkaParams));
        // 转换为pair<String,String>
        JavaPairDStream<String, String> adclickRecords =consumerRecords.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, String>() {

			public Tuple2<String, String> call(ConsumerRecord<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,String>(arg0.key(),arg0.value());
			}
		});
		
        /**
         * 黑名单过滤。采用transform函数
         * 在这里须使用transformToPair，因为kafka读取过来的数据是Pair<String,String>类型
         * 另外过滤后的数据还需要进一步处理，所以读取过来的数据必须是kafka的原始类型
         * 再次说明：每个batch duration中输入的数据实际上是被一个且仅仅是被一个RDD封装的，你可以有多个InputDStream
         * 但其实产生job时这些不同的InputDStream在batach duraction中就相当于spark基于hdfs数据操作的不同文件来源罢了
         */
        JavaPairDStream<String, String> adclickLogs=adclickRecords.transformToPair(new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> arg0) throws Exception {
				/**
				 * 黑名单过滤思路
				 * 1.从数据库中查询黑名单转换成RDD,即新的RDD封装黑名单数据
				 * 2.然后把代表黑名单的RDD与batach duration中的RDD进行join
				 * 准备说是进行leftOuterJoin，即用batch duration中的RDD与黑名单的RDD进行leftOuterJoin，
				 * 如果join结果两者都有内容，则为true
				 * 保存false 的数据
				 * 不能直接join，join拿的结果是只有黑名单中有内容，才能把黑名单和通过流进来的RDD数据进行合并。这肯定不行
				 */
				final List<String>  blackListName  =new ArrayList<String>();
				DbcpPool jdbc = new DbcpPool();
				jdbc.doQuery("select * from blacklist", null, new ExecuteCallBack() {
					
					public void resultCallBack(ResultSet rs) throws Exception {
						while (rs.next()){
							blackListName.add(rs.getString(1)); //查询获取userid
						}
						
					}
				});
				List<Tuple2<String,Boolean>> blackListTuple = new ArrayList<Tuple2<String, Boolean>>();
				for(String name :blackListName){
					blackListTuple.add(new Tuple2<String, Boolean>(name, null));
				}
				//数据来自于数据库查询的黑名单表并且映射成为<String, Boolean>
				List<Tuple2<String,Boolean>> blackListFromDB = blackListTuple;
				//借助上下文创建，不需要再次新建
				JavaSparkContext jsc1 = new JavaSparkContext(arg0.context());
				/**
				 * 黑名单表中只有userID，如果要进行join操作必须是k-v格式
				 * 所以这里需要基于数据表中的数据生成k-v格式的数据集合
				 */
				JavaPairRDD<String,Boolean> blackListRDD = 	jsc1.parallelizePairs(blackListFromDB);
				/**
				 * 进行join操作时肯定是基于userID的，故需要把传入的RDD转换成指定格式RDD
				 */
				JavaPairRDD<String, Tuple2<String, String>> rdd2Pair = arg0.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<String,String>>() {

					public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> arg0) throws Exception {
						//基本数据格式：timestamp、ip、userID、adID、province、city
						String userID = arg0._2.split("\t")[2];
						return new Tuple2<String, Tuple2<String,String>>(userID, arg0);
					}
				});
				JavaPairRDD<String, Tuple2<Tuple2<String, String>,Optional<Boolean>>> joinList=rdd2Pair.leftOuterJoin(blackListRDD);
				JavaPairRDD<String,String> result = joinList.filter(new Function<Tuple2<String,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {

					public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> arg0)
							throws Exception {
						//第2个参数Tuple2的 第2个参数Optional
						 Optional<Boolean> optional = 	arg0._2._2;
						 if(optional.isPresent()&&optional.get()){
							 return false; //存在 
						 }else{
							 return true; //不存在
						 }
						
					}
				}).mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<String, String>,Optional<Boolean>>>,String,String>() {

					public Tuple2<String, String> call(
							Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> arg0) throws Exception {
						//返回Tuple2<String, String> 即我们最终过滤后的想要的数据
						return arg0._2._1; 
					}
				});
				return result;
			}
		});
        
        /**
         * 第四步 像对于RDD一样基于DStream编程，因为DStream是RDD产生的模版
         * 在sparkstreaming发生计算前，其实质是把每个batch中对DStream的操作翻译成对RDD的操作
         * 对初始的DStream执行transformation级别的操作 如map，filter等高级函数来进行具体计算
         * 
         * 把广告数据切分出来，重新组拼成后面我们需要的格式
         * 同一用户计数1
         */
        JavaPairDStream<String, Integer> adclickPairs=  adclickLogs.mapToPair(new PairFunction<Tuple2<String,String>,String, Integer >() {

			public Tuple2<String, Integer> call(Tuple2<String, String> arg0) throws Exception {
				String[] adclickLog = arg0._2.split("\t");
				String time = adclickLog[0];
				String ip = adclickLog[1];
				String userid = adclickLog[2];
				String adid = adclickLog[3];
				String province = adclickLog[4];
				String city = adclickLog[5];
				String adclickRecord =time+"-"+ip+"-"+userid+"-"+adid+"-"+province+"-"+city;
				return new Tuple2<String, Integer>(adclickRecord, 1);
			}
		}) ;

     
		
        /**
         * 广告点击量动态更新，每个updateStateByKey都会在batch duration时间间隔基础上对点击次数进行更新
         * 更新之后我们一般都会持久化到外部存储，比如mysql
         */
        JavaPairDStream<String, Integer> updateStateByKey4AdClicked = adclickPairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
			  /**
			   * 在历史数据基础上进行更新，
			   * values 代表当前key 在当前batch duration时间内出现次数的集合 ｛1，1，1，1，1，1。。。｝
			   * state 代表当前key在之前的所有batch duration时间内 累计的结果，故我们需要在state的基础上不断的加上values
			   */
				Integer	clickedTotalHistory =0;
			  if(state.isPresent()){
				  clickedTotalHistory = state.get();
			  }
			  for(Integer clickCount : values){
				  clickedTotalHistory += clickCount;
			  }
				return  Optional.of(clickedTotalHistory);
			}
		});
        updateStateByKey4AdClicked.print();
        /**
         * 对黑名单写入外部存储库，通过foreachRDD模式得到每一个记录，使用数据库连接池高效的写入数据到mysql
         * 由于传入的参数是Iterator类型的集合，所以为了更高效的处理这里需要采用批量提交
         * 比如一次插入1000个record，就使用insertBatch或updateBatch类型的操作
         * 插入用户的信息只包含：timestamp、adID、province、city
         * 这里需要注意一个问题：可能会有2条记录的key是一样的，此时就需要进行累加更新
         * 在batch里面这个放进去的数据就是当前用户对这个广告点击的次数，如果发现数据库已经存在key的数据，就进行累加。10秒更新一次
         */
        
        updateStateByKey4AdClicked.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			//以下内容跟reduceByKey 后的foreachRDD操作一样
			public void call(JavaPairRDD<String, Integer> arg0) throws Exception {
				arg0.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

					public void call(Iterator<Tuple2<String, Integer>> arg0) throws Exception {
						List<AdClicked> adClickList = new ArrayList<AdClicked>();
						while(arg0.hasNext()){
							Tuple2<String,Integer> tuple = arg0.next();
							String[] clickLog = tuple._1.split("-");
							AdClicked userAdClick =new AdClicked();
							userAdClick.setTimestamp(clickLog[0]);
							userAdClick.setAdID(clickLog[1]);
							userAdClick.setProvince(clickLog[2]);
							userAdClick.setCity(clickLog[3]);
							adClickList.add(userAdClick);
						}
						 final List<AdClicked> adClick2insert = new ArrayList<AdClicked>();
						 final List<AdClicked> adClick2update = new ArrayList<AdClicked>();
						 DbcpPool jdbcUtil =  new DbcpPool();
						// adclickedcount表的字段：timestamp、adID、province、city、clickCount
						for( final AdClicked adClick:adClickList){
							jdbcUtil.doQuery("select clickCount from adclickedcount where timestamp = ? and adID= ? and province=? and city=? ",
									new Object[]{adClick.getTimestamp(),adClick.getAdID(),adClick.getProvince(),adClick.getCity()}, new ExecuteCallBack() {
								
								public void resultCallBack(ResultSet rs) throws Exception {
									if(rs.next()){
										Integer result = rs.getInt(1);
										adClick.setClickedCount(result);
										adClick2update.add(adClick);
									}else{
										adClick2insert.add(adClick);
										adClick.setClickedCount(1);
									}
									
								}
							});
						}
						List<Object[]> insertParams = new ArrayList<Object[]>();
						for(AdClicked clicked : adClick2insert){
							insertParams.add(new Object[]{
									clicked.getTimestamp(),
									clicked.getAdID(),
									clicked.getProvince(),
									clicked.getCity(),
									clicked.getClickedCount()
							});
						}
						jdbcUtil.doBatch("insert into adclickedcount values(?,?,?,?,?)", insertParams);
						
						List<Object[]> updateParams = new ArrayList<Object[]>();
						for(AdClicked clicked : adClick2insert){
							updateParams.add(new Object[]{
									clicked.getClickedCount(),
									clicked.getTimestamp(),
									clicked.getAdID(),
									clicked.getProvince(),
									clicked.getCity()
							});
						}
						jdbcUtil.doBatch("update adclickedcount set clickCount=? where timestamp = ? and adID= ? and province=? and city=? ", updateParams);
					}
				});
				
			}
		});
        /**
		 * 	计算有效点击
		 *  1.复杂情况下一般是采用机器学习训练好模型来进行在线过滤
		 *  2.简单情况下，可以通过每一个batch duration时间内统计的点击次数来判断是不是非法广告点击,
		 *  实际上非法广告点击程序都是会模拟真实的广告点击行为，所以通过batch duration的统计次数判断并不准备和完整，
		 *  可能需要对一天或者1小时的数据进行判断
		 *  3.比在线机器学习退而求之的办法
		 *    如：一段时间内，同一个IP或mac有多少个用户的帐号访问
		 *    如：统计一天内一个用户的点击次数如果一天内点击相同广告的次数超出50次，就列入黑名单
		 *  黑名单有个重要特征：动态生成，所以每一个batch duration时间内都要考虑是否有新的黑名单生成，此时黑名单需要存储起来，比如db、redis等
		 * 
		 *  例如邮件系统中的“黑名单”，可以采用Spark Streaming不断的监控每个用户的操作，如果用户发送邮件的频率超过了设定的值，
         *  可以暂时把用户列入“黑名单”，从而阻止用户过度频繁的发送邮件
		 */
        JavaPairDStream<String, Integer> adclickFilterInBatch = updateStateByKey4AdClicked.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			
			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				if(1<arg0._2){
					return false ; //更新黑名单的库表
				}
				return true ;
			}
		});
      
        
        /**
         * 再次过滤，从数据库中读取数据过滤黑名单
         */
        JavaPairDStream<String, Integer> blackList4History =   adclickFilterInBatch.filter(new Function<Tuple2<String,Integer>, Boolean>() {

			public Boolean call(Tuple2<String, Integer> arg0) throws Exception {
				//提前所有的点击历史数据：timestamp-ip-userID-adID-province-city
				String[] adclickLog = arg0._1.split("-");
				String userID = adclickLog[2];
				String adID = adclickLog[3];
				String time =adclickLog[0];
				/**
				 * 
				 * 根据userID，adID，time去查询用户用户点击广告的数据表获取总的点击次数
				 * 基于点击次数判断是否属于黑名单点击，查询出一个用户对同一个adID的点击次数大于50，则定义为黑名单
				 */
				int clickCountTotalDay = 88 ; //模拟次数
				if(clickCountTotalDay > 50){
					return true ;
				}
				return false;
			}
		});
        /**
         * 由于不同的partition里面可能包含相同的黑名单user，故必须对黑名单的RDD进行去重，对RDD去重即是对partition去重
         * 数据可能出现重复，在一个partition里面重复，但多个partition里面不能保证一个user重复，需要对黑名单的整个rdd进行去重
         */
        JavaDStream<String> uniqueBlackListUserId4History=blackList4History.map(new Function<Tuple2<String,Integer>, String>() {

			public String call(Tuple2<String, Integer> arg0) throws Exception {
				// 获取黑名单的userID
				return arg0._1.split("-")[2];
			}
		}).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

			public JavaRDD<String> call(JavaRDD<String> arg0) throws Exception {
				// 去重
				return arg0.distinct();
			}
		}) ;
        /**
         * 对黑名单写入数据库表，通过foreachRDD模式得到每一个记录，使用数据库连接池高效的写入数据到mysql
         * 由于传入的参数是Iterator类型的集合，所以为了更高效的处理这里需要采用批量提交
         * 比如一次插入1000个record，就使用insertBatch或updateBatch类型的操作
         * 插入用户的信息只包含：userID
         * 直接插入黑名单表即可
         * 
         */
        uniqueBlackListUserId4History.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			public void call(JavaRDD<String> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<String>>() {

					public void call(Iterator<String> arg0) throws Exception {
						List<Object[]> blackList = new ArrayList<Object[]>();
						while(arg0.hasNext()){
							blackList.add(new Object[]{arg0.next()});
							System.out.println("black===>"+arg0.next());
						}
						DbcpPool jdbc = new DbcpPool();
						jdbc.doBatch("insert into blacklist values(?)", blackList);
					}
				});
				
			}
		});
        
        /**
         * TopN计算
         * 计算出每天每个省份排名top5的广告，由于对RDD直接进行计算，需要使用transform算子
         */
        JavaDStream<Row> adClickTop5= updateStateByKey4AdClicked.transform(new Function<JavaPairRDD<String,Integer>, JavaRDD<Row>>() {

			public JavaRDD<Row> call(JavaPairRDD<String, Integer> arg0) throws Exception {
				JavaPairRDD<String, Integer> rddPair = arg0.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

					public Tuple2<String, Integer> call(Tuple2<String, Integer> arg0) throws Exception {
						String[] clicked =arg0._1.split("-");
						String timestamp =clicked[0];
						String adid = clicked[3];
						String province = clicked[4];
						String clickedRecord = timestamp+","+adid+","+province;
						//(String:新组装的广告id记录,Integer:点击次数)
						return new Tuple2<String, Integer>(clickedRecord, arg0._2);
					}
				}) ;
				JavaPairRDD<String, Integer> rddreduce = rddPair.reduceByKey(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer arg0, Integer arg1) throws Exception {
						return arg0+arg1;
					}
				});
				//重新生成一条条 存在timstamp,adID,province,clickcount 的记录，用作统计计算top
				JavaRDD<Row> rddRow = rddreduce.map(new Function<Tuple2<String,Integer>, Row>() {

					public Row call(Tuple2<String, Integer> arg0) throws Exception {
						String[] clickRecord = arg0._1.split(",");
						String timstamp =clickRecord[0];
						String adID=clickRecord[1];
						String province =clickRecord[2];
						
						return RowFactory.create(timstamp,adID,province,arg0._2);
					}
				});
				//动态构造元数据 (另外一直创建DataFrame方式使用case class 反射方式)
				StructType struct =DataTypes.createStructType(Arrays.asList(
						DataTypes.createStructField("timestamp", DataTypes.StringType, true),
						DataTypes.createStructField("adID", DataTypes.StringType, true),
						DataTypes.createStructField("province", DataTypes.StringType, true),
						DataTypes.createStructField("clickCount", DataTypes.IntegerType, true)));
				SQLContext sc1=  new SQLContext(arg0.context());
				//将struct信息映射到rddRow 转换成数据集
				Dataset<Row> clickdf = sc1.createDataFrame(rddRow, struct);
				clickdf.createOrReplaceTempView("clickTopN");
				Dataset<Row> rowrdd =sc1.sql("select timestamp,adID,province,clickCount from "
					+"	clickTopN group by province,timestamp,adID,clickCount order by clickCount desc limit 5") ;
				return rowrdd.toJavaRDD();
			}
		});
        adClickTop5.print();
        // top5 rdd数据 遍历逐条存入外部数据库 用于业务
        adClickTop5.foreachRDD(new VoidFunction<JavaRDD<Row>>() {

			public void call(JavaRDD<Row> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {

					public void call(Iterator<Row> row) throws Exception {
						List<AdClickProvinceTopN> top5 = new ArrayList<AdClickProvinceTopN>(); 
						while (row.hasNext()){
							Row r = row.next() ;
							AdClickProvinceTopN  topN= new AdClickProvinceTopN();
							topN.setTimestamp( r.getString(0));
							topN.setAdID(r.getString(1));
							topN.setProvince( r.getString(2));
							topN.setClickCount(r.getInt(3));
							top5.add(topN);
						}
						DbcpPool jdbc = new DbcpPool();
						//构造删除数据参数
						Set<String> set = new HashSet<String>();
						for(AdClickProvinceTopN str:top5){
							set.add(str.getTimestamp()+"_"+str.getProvince());
						}
						List<Object[]> delParams = new ArrayList<Object[]>();
						for(String del : set){
							String[] delp = del.split("_");
							delParams.add(new Object[]{delp[0],delp[1]});
						}
						//adclickprovincetopn表的字段：timestamp、adID、province、clickedCount
						jdbc.doBatch("delete from adclickprovincetopn where timestamp=? and province=?", delParams);
						
						List<Object[]> insertParams = new ArrayList<Object[]>();
						for(AdClickProvinceTopN top : top5){
							insertParams.add(new Object[]{top.getTimestamp(),top.getAdID(),top.getProvince(),top.getClickCount()});
						}
						jdbc.doBatch("insert into adclickprovincetopn values(?,?,?,?)", insertParams);
					}
				});
				
			}
		});
        /**
         * 计算过去半小时内广告点击趋势，只需得到adID，timestamp
         * 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
         */
        JavaPairDStream<String, Integer> adclick30m=adclickLogs.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {

			public Tuple2<String, Integer> call(Tuple2<String, String> arg0) throws Exception {
				String[] clickLog = arg0._2.split("\t");
				//Todo:后续需要重构代码实现时间戳和分钟的转换提取。此处需要提取出该广告的点击分钟单位
				String time = clickLog[0];
				String adID = clickLog[3];
				return new Tuple2<String, Integer>(adID+"_"+time, 1);
			}
			//增量方式 reduceByKeyAndWindow( reduceFunc, invReduceFunc, windowDuration,slideDuration)
		}).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		},new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0-arg1;
			}
		},Durations.minutes(30),Durations.milliseconds(5000));
        adclick30m.print();
        // 写入数据库
        adclick30m.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {

			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

					public void call(Iterator<Tuple2<String, Integer>> arg0) throws Exception {
						List<AdTrendStat> adTrend = new ArrayList<AdTrendStat>();
						while (arg0.hasNext()){
							Tuple2<String, Integer> clickLog = arg0.next();
							String[] click=clickLog._1.split("_");
							String adID = click[0];
							String time =click[1];
							Integer clickcount =clickLog._2;
							/**
							 * 写如数据库的字段：adID,time,clickcount
							 * 根据趋势绘图需要用到年月日时分秒，这里需要对time再次解析
							 */
							AdTrendStat stat= new AdTrendStat();
							stat.setAdID(adID);
							stat.setClickcount(clickcount);
							stat.set_date(time);
							stat.set_hour(time);
							stat.set_minute(time);
							
							adTrend.add(stat);
						}
						final List<AdTrendStat> insertParams = new ArrayList<AdTrendStat>();
						final List<AdTrendStat> updateParams = new ArrayList<AdTrendStat>();
						DbcpPool jdbc = new DbcpPool();
						for(final AdTrendStat adt:adTrend){
							final AdTrendCountHis adTrendHis = new AdTrendCountHis();
							jdbc.doQuery("select clickCount from adclicktrend where date=? and hour=? and minute=? and adID=? ", 
							    new Object[]{adt.get_date(),adt.get_hour(),adt.get_minute(),adt.getAdID()}, new ExecuteCallBack() {
								
								public void resultCallBack(ResultSet rs) throws Exception {
									if(rs.next()){
										Integer counthis = rs.getInt(1);
										adTrendHis.setCountHis(counthis);
										updateParams.add(adt);
									}else{
										insertParams.add(adt);
									}
									
								}
							});
							
						}
						List<Object[]> inserting =new ArrayList<Object[]>();
						List<Object[]> updating =new ArrayList<Object[]>();
						for(AdTrendStat adt:insertParams){
							inserting.add(new Object[]{
									adt.getAdID(),
									adt.getClickcount(),
									adt.get_date(),
									adt.get_hour(),
									adt.get_minute(),
									
									
							});
						}
						jdbc.doBatch("insert into adclicktrend values(?,?,?,?,?)", inserting);
						for(AdTrendStat adt:updateParams){
							updating.add(new Object[]{
									adt.getClickcount(),
									adt.get_date(),
									adt.get_hour(),
									adt.get_minute(),
									adt.getAdID()
							});
						}
						jdbc.doBatch("update adclicktrend set clickCount=? where date=? and hour=? and minute=? and adID=? ", updating);
					}
				});
				
			}
		});
     
        jsc.start();
        try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        jsc.close();
	}
}
