package com.pzoom.mdsp.logfilling;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.mdsp.util.ConstData;

/**
 * zk信息管理
 * 
 * @author chenbaoyu
 * 
 */
public class ZkInfoReader {
	public static final Logger LOG = LoggerFactory
			.getLogger(ZkInfoReader.class);

	private KafkaConfig kafkaConfig;
	private int taskIndex;
	private int totalTasks;
	private List<PartitionManager> managerList = new ArrayList<PartitionManager>();
	private Long lastRefreshTime;
	private int refreshFreqMs;
	private ZkState state;

	private CuratorFramework curator;
	private String zkPath;
	private String topic;
	
	public ZkInfoReader(KafkaConfig kafkaConfig, ZkState state,
			int taskIndex, int totalTasks) {
		this.kafkaConfig = kafkaConfig;
		this.taskIndex = taskIndex;
		this.totalTasks = totalTasks;
		this.state = state;

		this.refreshFreqMs = kafkaConfig.refreshFreqSecs * 1000;

		this.zkPath = kafkaConfig.brokerZkPath;
		this.topic = kafkaConfig.topic;

		curator = CuratorFrameworkFactory.newClient(
				kafkaConfig.brokerZkStr, ConstData.SESSION_TIMEOUT, 15000,
				new RetryNTimes((ConstData.RETRY_TIMES),
						ConstData.RETRY_INTERVAL));

		curator.start();

	}

	/**
	 * 获取管理分区
	 * 
	 * @return
	 */
	public List<PartitionManager> getMyManagedPartitions() {
		if (lastRefreshTime == null
				|| (System.currentTimeMillis() - lastRefreshTime) > refreshFreqMs) {
			try {
				LOG.info("Refreshing partition manager connections");
				
				List<Partition> partitions = getPartitions();
				for (int i = taskIndex; i < partitions.size(); i += totalTasks) {
					Partition myPartition = partitions.get(i);
					PartitionManager man = new PartitionManager(state,
							kafkaConfig, myPartition);
					managerList.add(man);
				}

			} catch (Exception e) {
				LOG.info("获取zk分区出错:" + e);
			}
			LOG.info("Finished refreshing");
			lastRefreshTime = System.currentTimeMillis();
		}
		return managerList;
	}
	
	public List<Partition> getPartitions()
	{
		List<Partition> partitions = new LinkedList<Partition>();
		try {
			// 从zk取得partition的数目
			int numPartitionsForTopic = getNumPartitions();
			String brokerInfoPath = brokerPath();
			for (int partition = 0; partition < numPartitionsForTopic; partition++) {
				// 从zk获取partition的leader broker
				int leader = getLeaderFor(partition);
				String path = brokerInfoPath + "/" + leader;
				try {
					byte[] brokerData = curator.getData().forPath(path);
					// 从zk获取broker的host:port
					StringBuilder brokerInfo = new StringBuilder();
					
					Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(brokerData, "UTF-8"));
					brokerInfo.append(value.get("host")).append(":").append(value.get("port"));
					partitions.add(new Partition(brokerInfo.toString(), partition));
				} catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
					LOG.error("Node {} does not exist ", path);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
        return partitions;
	}

	/**
	 * zk 中获取分区数
	 * 
	 * @return
	 */
	private int getNumPartitions() {
		try {
			String topicBrokersPath = partitionPath();
			List<String> children = curator.getChildren().forPath(
					topicBrokersPath);
			return children.size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public String partitionPath() {
		return zkPath + "/topics/" + topic + "/partitions";
	}

	public String brokerPath() {
		return zkPath + "/ids";
	}

	/**
	 * 从zk获取partition的leader broker get
	 * /brokers/topics/distributedTopic/partitions/1/state {
	 * "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1,
	 * "version":1 }
	 * 
	 * @param partition
	 * @return
	 */
	private int getLeaderFor(long partition) {
		try {
			String topicBrokersPath = partitionPath();
			byte[] hostPortData = curator.getData().forPath(
					topicBrokersPath + "/" + partition + "/state");
			@SuppressWarnings("unchecked")
			Map<Object, Object> value = (Map<Object, Object>) JSONValue
					.parse(new String(hostPortData, "UTF-8"));
			Integer leader = ((Number) value.get("leader")).intValue();
			return leader;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		curator.close();
		curator = null;
	}

}
