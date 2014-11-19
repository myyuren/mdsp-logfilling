package com.pzoom.mdsp.logfilling;

import java.util.List;

import com.pzoom.mdsp.logfilling.Kafka2HDFS.PartitionAndOffset;


/**
 * config.xml的参数构建的对象
 * @author chenbaoyu
 *
 */
public class KafkaConfig {

	public final String topic;
	public final String clientId;
	
	public int fetchSizeBytes = 1024 * 1024;
	public int socketTimeoutMs = 10000;
	public int bufferSizeBytes = 1024 * 1024;
	public boolean forceFromStart = false;
	public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
	
	public String brokerZkStr = null;
	public String brokerZkPath = null;
	public String zkRoot = null;
	public long stateUpdateIntervalMs = 1000; //提交 offset的间隔时间为1秒
	
	public String outDir;
	public int fileSize;
	public List<PartitionAndOffset> offsetList;
	public int refreshFreqSecs = 60;
	
	public KafkaConfig(String brokerZkStr,String brokerZkPath,  String topic) {
		this(brokerZkStr,brokerZkPath, topic, kafka.api.OffsetRequest.DefaultClientId());
	}
	
	public KafkaConfig(String brokerZkStr,String brokerZkPath,  String topic, String clientId) {
		this.brokerZkStr = brokerZkStr;
		this.brokerZkPath = brokerZkPath;
		this.topic = topic;
		this.clientId = clientId;
	}
	
	public KafkaConfig(String brokerZkStr,String brokerZkPath, String topic, String zkRoot, 
			String outDir, int fileSize, List offsetList) {
		this(brokerZkStr,brokerZkPath, topic);
		this.brokerZkStr = brokerZkStr;
		this.brokerZkPath = brokerZkPath;
		this.zkRoot = zkRoot;
		this.outDir = outDir;
		this.fileSize = fileSize;
		this.offsetList = offsetList;
	}
}
