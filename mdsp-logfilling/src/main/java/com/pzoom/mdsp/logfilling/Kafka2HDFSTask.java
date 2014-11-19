package com.pzoom.mdsp.logfilling;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.mdsp.logfilling.PartitionManager.EmitState;

/**
 * 执行备份的任务线程，一个分区对应一个备份线程
 * @author chenbaoyu
 *
 */
public class Kafka2HDFSTask implements Runnable{
	public static final Logger LOG = LoggerFactory.getLogger(Kafka2HDFSTask.class);
	
	private KafkaConfig kafkaConfig;
	private int taskIndex;
	private int totalTasks;
	private ZkState state;
	private int fetchInterval;
	
	private ZkInfoReader zkCoordinator;
	private long lastUpdateMs = 0;
	private int currPartitionIndex = 0;
	private List<PartitionManager> managers = new ArrayList<PartitionManager>();
	
	public Kafka2HDFSTask(KafkaConfig kafkaConfig, ZkState state, 
			int taskIndex, int totalTasks, int fetchInterval) {
		this.kafkaConfig = kafkaConfig;
		this.taskIndex = taskIndex;
		this.totalTasks = totalTasks;
		this.state = state;
		this.fetchInterval = fetchInterval;
	}
	
	@Override
	public void run() {
		zkCoordinator = new ZkInfoReader(kafkaConfig, state, taskIndex, totalTasks);
		managers = zkCoordinator.getMyManagedPartitions();
		/**
		 * 循环判断，
		 */
		while (true) {
			//间隔指定时间去拉取一次数据，防止过于频繁拉取数据，系统资源浪费
			if (fetchInterval != 0) {
				try {
					Thread.sleep(fetchInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			PartitionManager.EmitState state = null;
			for (int i = 0; i < managers.size(); i++) {
				// in case the number of managers decreased
				//处理当currPartitionIndex的值大于managers.size()的情况
				currPartitionIndex = currPartitionIndex % managers.size();
				state = managers.get(currPartitionIndex).fetchNextMessages();
				currPartitionIndex = (currPartitionIndex + 1) % managers.size();
				if (state != EmitState.NO_EMITTED) {
					break;
				}
			}
			
			if(state==EmitState.NO_EMITTED)
			{
				continue;
			}

			long now = System.currentTimeMillis();
			if ((now - lastUpdateMs) > kafkaConfig.stateUpdateIntervalMs) {
				commit();
			}
			
			boolean allFinished = true;
			for (PartitionManager manager : managers) {
				allFinished &= manager.isDone();
			}
			if (allFinished) {
				commit(); //force commit
				LOG.info("强制提交 taskIndex :" + taskIndex + " has compeleted the job!");
				//跳出循环
				break;
			}

		}
		
	}
	/**
	 * 提交偏移量，循环每个分区。
	 */
    private void commit() {
        lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : managers) {
            manager.commit();
        }
    }
}
