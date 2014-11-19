package com.pzoom.mdsp.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.mdsp.logfilling.KafkaConfig;
import com.pzoom.mdsp.logfilling.Partition;

public class Utils {
	public static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	private static final int NO_OFFSET = -5;
	
	
	enum KafkaError {
	    NO_ERROR,
	    OFFSET_OUT_OF_RANGE,
	    /*
	    INVALID_MESSAGE,
	    UNKNOWN_TOPIC_OR_PARTITION,
	    INVALID_FETCH_SIZE,
	    LEADER_NOT_AVAILABLE,
	    NOT_LEADER_FOR_PARTITION,
	    REQUEST_TIMED_OUT,
	    BROKER_NOT_AVAILABLE,
	    REPLICA_NOT_AVAILABLE,
	    MESSAGE_SIZE_TOO_LARGE,
	    STALE_CONTROLLER_EPOCH,
	    OFFSET_METADATA_TOO_LARGE,*/
	    UNKNOWN;

	    public static KafkaError getError(int errorCode) {
	        if (errorCode < 0 || errorCode >= UNKNOWN.ordinal()) {
	            return UNKNOWN;
	        } else {
	            return values()[errorCode];
	        }
	    }
	}
	
	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, KafkaConfig config) {
		long startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		if (config.forceFromStart) {
			startOffsetTime = config.startOffsetTime;
		}
		return getOffset(consumer, topic, partition, startOffsetTime);
	}

	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, long startOffsetTime) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				startOffsetTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

		long[] offsets = consumer.getOffsetsBefore(request).offsets(topic,
				partition);
		if (offsets.length > 0) {
			return offsets[0];
		} else {
			return NO_OFFSET;
		}

	}

	/**
	 * 拉取kafka 数据
	 * 
	 * @param config
	 * @param consumer
	 * @param partition
	 *            分区
	 * @param offset
	 *            从指定的偏移量开始
	 * @return
	 */
	public ByteBufferMessageSet fetchMessages(KafkaConfig config,
			SimpleConsumer consumer, Partition partition, long offset) {
		ByteBufferMessageSet msgs = null;
		String topic = config.topic;
		int partitionId = partition.partition;
		for (int errors = 0; errors < 2 && msgs == null; errors++) {
			FetchRequestBuilder builder = new FetchRequestBuilder();
			FetchRequest fetchRequest = builder
					.addFetch(topic, partitionId, offset, config.fetchSizeBytes)
					.clientId(config.clientId).build();
			FetchResponse fetchResponse = null;
			try {
				fetchResponse = consumer.fetch(fetchRequest);
			} catch (Exception e) {
				LOG.error("拉取kafka数据出错 fetchMessages："+e);
			}
			if (fetchResponse.hasError()) {//主要处理offset outofrange的case，通过getOffset从earliest或latest读
				KafkaError error = KafkaError.getError(fetchResponse.errorCode(
						topic, partitionId));
				if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)
						&& config.useStartOffsetTimeIfOffsetOutOfRange
						&& errors == 0) {
					long startOffset = getOffset(consumer, topic, partitionId,
							config.startOffsetTime);
					LOG.warn("获取kafka数据 offset out of range错误，当前偏移量为: ["
							+ offset
							+ "]; "
							+ "retrying with default start offset time from configuration. "
							+ "configured start offset time: ["
							+ config.startOffsetTime + "] offset: ["
							+ startOffset + "]");
					offset = startOffset;	
				} else {
					String message = "获取kafka数据 from [" + partition
							+ "] 主题 [" + topic + "]: 出错信息：[" + error + "]";
					LOG.error(message);
				}
			} else {
				msgs = fetchResponse.messageSet(topic, partitionId);
			}
		}
		return msgs;
	}
	public static ByteBuffer getLog(Message msg) {
		return msg.payload();
	}

}
