package com.pzoom.mdsp.logfilling;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.mdsp.util.XmlParser;

/**
 * pull data from kafka and push to hdfs
 * 
 * @author cby
 *
 */
public class Kafka2HDFS {
	
	public static final Logger LOG = LoggerFactory.getLogger(Kafka2HDFS.class);
	private String topic;
	private int partitionNum;
	private String kafkaZkStr;
	private String offsetZkStr;
	private String offsetZkRoot;
	private List<PartitionAndOffset> offsetList;
	private String rootDirOutput;
	private int fileSize;
	private String brokerPath;
	private int fetchInterval;
	
	private KafkaConfig kafkaConfig;
	private ZkState state;
	private ExecutorService executor;
	private List<Future<?>> futerList;
	
	private long startTime;
	private long endtime;
	
	public Kafka2HDFS() {
		offsetList = new ArrayList<PartitionAndOffset>();
		futerList = new ArrayList<Future<?>>();
	}
	
	public class PartitionAndOffset {
		public int id;
		public long startOffset = -1;
		public long endOffset = -1;
		
		public PartitionAndOffset(int id, long startOffset, long endOffset) {
			this.id = id;
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}
		
		@Override
		public String toString() {
			return "[" + id + ":"+ startOffset 
					+ ":" + endOffset + "]";
		}
	}
	
	public void prepare() {
		//parser xml 
		getConfig();
//		StringBuilder sb = new StringBuilder();
//		sb.append("topic: ").append(topic).append(" partitionNum: ").append(partitionNum).append(" apName: ").append(" name").append("\n");
//		sb.append("kafkaZkStr: ").append(kafkaZkStr).append(" brokerPath: ").append(brokerPath).append("\n");
//		sb.append("offsetZkStr: ").append(offsetZkStr).append(" offsetZkRoot: ").append(offsetZkRoot).append("\n");
//		sb.append("rootDirOutput: ").append(rootDirOutput).append(" fileSize: ").append(fileSize).append(" compressor: ").append("\n");
//		sb.append("fetchInterval: ").append(fetchInterval);
//		LOG.info("get config :" + sb.toString() + "offset: " + offsetList.toString());
//		
		init();
		LOG.info("init sucess!!");
	}
	
	private void getConfig() {
		XmlParser config = new XmlParser();
		try {
			config.parse("config.xml");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Properties props = config.getProps();
		topic = props.getProperty("topic");
		partitionNum = Integer.valueOf(props.getProperty("partitonnumber"));
		kafkaZkStr = props.getProperty("kafkazkstr");
		offsetZkStr = props.getProperty("offsetzkstr");
		offsetZkRoot = props.getProperty("offsetzkroot");
		rootDirOutput = props.getProperty("rootdir");//hdfs 根目录
		fileSize = Integer.valueOf(props.getProperty("filesize"));
		brokerPath = props.getProperty("brokerpath");
		String[] segs = props.getProperty("partitionsandoffsets").trim().split(";");
		for (String seg : segs) {
			String[] offset = seg.trim().split(":");
			PartitionAndOffset offsetRange = new PartitionAndOffset(Integer.valueOf(offset[0]),
					Long.valueOf(offset[1]), Long.valueOf(offset[2]));
			offsetList.add(offsetRange);
		}
		fetchInterval = Integer.valueOf(props.getProperty("fetchinterval"));
	}
	
	private void init() {
		state = new ZkState(offsetZkStr);
		String outPathWithtopic = rootDirOutput + "/" + topic;
		kafkaConfig = new KafkaConfig(kafkaZkStr,brokerPath, topic, offsetZkRoot, 
				outPathWithtopic, fileSize, offsetList);
		//创建固定大小的线程池
		executor = Executors.newFixedThreadPool(partitionNum);//one partition one thread 一个分区一个线程 
	}
	
	private void run() {
		for (int i = 0; i < partitionNum; i++) {
			Kafka2HDFSTask task = new Kafka2HDFSTask(kafkaConfig, state, i, partitionNum, fetchInterval);
			Future<?> retFuture = executor.submit(task);
			futerList.add(retFuture);
		}
		LOG.info("create threads sucess!!");

		startTime = System.currentTimeMillis();
	}
	
	private void clean() {
		executor.shutdown();
		LOG.info("shut down thread pool");
		
		try {
			while (true) {		
				boolean flag = true;
				
				for (Future<?> index : futerList) {
					if (!index.isDone()) {
						flag = false;
						break;//not complete
					}
				}

				if (flag) {
					LOG.info("task has completed!!");
					endtime = System.currentTimeMillis();
					LOG.info("task use time: " + (endtime-startTime));
					break;//has completed
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			state.close();
			LOG.info("clean up!!");
		}
	}
	

	public static void main(String[] args) throws IOException {
		File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        new File("./bin/winutils.exe").createNewFile();
    	PropertyConfigurator.configure("log4j.properties");
		Kafka2HDFS task = new Kafka2HDFS();
		task.prepare();
		task.run();
		task.clean();
	}
}
