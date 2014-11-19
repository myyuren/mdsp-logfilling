package com.pzoom.mdsp.logfilling;


import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.pzoom.mdsp.model.SettlementEntity;
import com.pzoom.mdsp.util.ConstData;
import com.pzoom.mdsp.util.FileReaderUtil;
import com.pzoom.mdsp.util.HDFSWriter;
import com.pzoom.mdsp.util.Utilities;
import com.pzoom.mdsp.util.Utils;
import com.pzoom.rpc.serialize.RpcSerializer;

/**
 * 分区管理，初始化kafka的consumer相关的信息，以及管理zookeeper中存储的offset
 *  
 * @author chenbaoyu
 *
 */
public class PartitionManager {
	public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);
	
    static enum EmitState {
        EMITTED_SUCESSFUL,
        EMITTED_FAILED,
        NO_EMITTED
    }
    
    private Long emittedToOffset;
    private Long committedTo;
    private Long oldOffset;
    private Partition partition;
    private KafkaConfig kafkaConfig;
    private SimpleConsumer consumer;
    private ZkState state;
    
    private long startOffset = -1;
    private long endOffset = -1;
    
    private volatile boolean needCheckTime = true;
    private boolean hasDone = false;
   
    private Timer checkDateTimer;
    private String dateString;
    private HDFSWriter writer;
    private long offsetInFile=0L; //kafka数据备份文件中保存的offset
    
    public PartitionManager(ZkState state, KafkaConfig kafkaConfig, Partition partition) {
        this.partition = partition;
        this.kafkaConfig = kafkaConfig;
        this.state = state;
        //get offset to pull 配置文件config.xml中配置的startOffset和endOffset
        startOffset = kafkaConfig.offsetList.get(partition.partition).startOffset;
        endOffset = kafkaConfig.offsetList.get(partition.partition).endOffset;
        LOG.info("partion: " + partition.partition + " startOffset: " + startOffset + " endOffset: " + endOffset);
        init();
    }
    /**
     * 初始化操作，注册消费者，初始化writer,获取oldoffset
     */
    private void init() {
    	//注册消费者,需要根据这个consumer获取offset,并给fetch使用
        String brokerInfo = partition.bokerInfo;
        String[] info = brokerInfo.split(":");
        consumer = new SimpleConsumer(info[0],Integer.parseInt(info[1]), kafkaConfig.socketTimeoutMs, kafkaConfig.bufferSizeBytes, kafkaConfig.clientId);
        checkDateTimer = new Timer();
        //表示在new Date()开始执行，指定毫妙数执行一次
        checkDateTimer.scheduleAtFixedRate(new CheckTime(), new Date(), ConstData.DATE_CHECK_SEC*1000L);
        
        writer = new HDFSWriter(kafkaConfig.outDir, kafkaConfig.fileSize,partition.partition);
        //保存在文件中的偏移量，和数据绑定到一起
        offsetInFile = getOffsetInFile(partition.partition);
        
        Long jsonOffset = null;
        String path = committedPath(); //offset 在zookeeper上的地址
        try {
            Map<Object, Object> json = state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }
        LOG.info("offsetInFile and jsonOffset"+offsetInFile+"---------------------"+jsonOffset);
        if (jsonOffset == null) { // failed to parse JSON?
        	if (-1 != startOffset) {
				committedTo = kafkaConfig.offsetList.get(partition.partition).startOffset;
				LOG.info("No partition information found, using user configuration to determine offset");
			}else {
				//Kafka 中获取offset
				committedTo = Utils.getOffset(consumer, kafkaConfig.topic, partition.partition, kafkaConfig);
	            LOG.info("No partition information found, using configuration to determine offset");
			}
        } else if (kafkaConfig.forceFromStart) { //强制从偏移量开始读取
            committedTo = Utils.getOffset(consumer, kafkaConfig.topic, partition.partition, kafkaConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
        	//offsetInFile 的值应该永远比 jsonOffset 大1 如果不是，说明在保存jsonOffset的时候程序异常中止，所以此时只能以offsetInFile为准
        	if(jsonOffset-offsetInFile!=1&&offsetInFile!=0)
        	{
        		LOG.info("重要警告：文件中最后数据的offsetInFile和zk中保存的jsonOffset值大小不一致，" +
        				"程序已默认从备份文件最后一条数据中的offset开始读取kafka数据，请确认与事实相符（如：机器重启）");
        		jsonOffset = offsetInFile;
        	}
        	if (-1 != startOffset) {
        		committedTo = startOffset;
                LOG.info("Read last commit offset from zookeeper: " + committedTo);
			}else {
				committedTo = jsonOffset;
				LOG.info("refresh last commit offset using user configuration: " + committedTo);
			}
        }

        LOG.info("Starting Kafka " + consumer.host() + ":" + partition.partition + " from offset " + committedTo);
        emittedToOffset = committedTo;
        oldOffset = committedTo;
    }
    
    /**
     * 获取kafka数据备份文件中最后的偏移量，（注意：备份文件的位置默认取当天产生的目录下面，详情看dateString变量）,有问题，暂时不用
     * @return
     */
    public long getOffsetInFile(int partition)
    {
    	//备份文件所有的文件夹路径
    	StringBuilder replicaFileFolder = new StringBuilder();
    	long fileOffset = 0L;
    	List<String> strList = new ArrayList<String>();
    	SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
		dateString = fmt.format(new Date());
		try {
			List<String> folderList = FileReaderUtil.readHDFSListFolder(kafkaConfig.outDir);
			if(folderList.size()>0)
			{
				dateString = folderList.get(folderList.size()-1);
				dateString = dateString.substring(dateString.lastIndexOf("/")+1);
			}
			replicaFileFolder.append(kafkaConfig.outDir).append("/").append(dateString);
			strList = FileReaderUtil.readHDFSListAll(replicaFileFolder.toString(),partition);
			if(strList.size()>0)
			{
				fileOffset = FileReaderUtil.readStreamLastOffset(strList.get(strList.size()-1));
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return fileOffset;
    }
    /**
     * 提交偏移量到zk
     */
    public void commit() {
        LOG.info("Committing offset for " + partition);
        String brokerInfo = partition.bokerInfo;
        String[] info = brokerInfo.split(":");
        if (oldOffset != committedTo) {
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("offset", oldOffset)
                    .put("partition", partition.partition)
                    .put("broker", ImmutableMap.of("host", info[0],
                            "port", info[1]))
                    .put("topic", kafkaConfig.topic).build();
            //JSON格式内容写入zk
            state.writeJSON(committedPath(), data);

            LOG.info("Wrote committed offset to ZK: " + oldOffset);
            committedTo = oldOffset;
        }
        LOG.info("Committed offset " + oldOffset + " for " + partition);
    }
    /**
     * 偏移量在zk中的存储路径
     * @return
     */
    private String committedPath() {
        return kafkaConfig.zkRoot + "/"+ partition.getId();
    }
    
    /**
     * 把下一个zk分区数据把数据写入hdfs 
     * @return
     */
    public EmitState fetchNextMessages() {
    	
    	if (isDone()) {
    		return EmitState.NO_EMITTED;
    	}
    	//拿取kafka数据
    	byte[] fetchBytes = fetchMessages();
    	if(fetchBytes==null)
    	{
    	//	LOG.error("fetch message is null!!");
    		return EmitState.NO_EMITTED;
    	}
    	//写到hdfs上
    	if (!writer.Writer(fetchBytes, dateString)) {
			LOG.error("failed write to hdfs!!");
			return EmitState.EMITTED_FAILED;
		}
		if (hasDone) {
			writer.close();
		}
		oldOffset = emittedToOffset;
		LOG.info("success write data to hdfs offset:"+emittedToOffset);
		
    	return EmitState.EMITTED_SUCESSFUL;
    }
    /**
     * 读取 kafka  更新要提交的偏移量  emittedToOffset
     * 存储时：备份数据以二进制的形式存储在文件中，内容以byte[4]+byte[messages.length]的格式循环存储，前4个字节代表下一个message所占字节长度
     * 读取时：先读取前4个字节，再解析出来前4个字节的int值，这个int值是下一个message的长度，程序再顺序读取这个int值大小的字节。循环！
     * @return
     */
    private byte[] fetchMessages() {
    	Utils utils = new Utils();
        ByteBufferMessageSet msgs = utils.fetchMessages(kafkaConfig, consumer, partition, oldOffset);
        int numMessages = countMessages(msgs);
        if (numMessages > 0) {
            LOG.info("Fetched " + numMessages + " messages from Kafka: " + consumer.host() + ":" + partition.partition);
        }else{
        	LOG.debug("Fetched Failed! from Kafka: " + consumer.host() + ":" + partition.partition);
        	return null;
        }
        
        int bytesLen = countBytesMessages(msgs);
        LOG.info("取到kafka数据字节长 度："+bytesLen);
        ByteBuffer buffers =ByteBuffer.allocate(bytesLen+numMessages*4);
        for (MessageAndOffset msg : msgs) {
        	if (endOffset != -1 && emittedToOffset > endOffset) {
        		LOG.info("has reach the end of request!!");
        		hasDone = true;
        		break;
        	}
        	ByteBuffer payload = Utils.getLog(msg.message());
    		byte[] bytes = new byte[payload.limit()];
    		payload.get(bytes);
    		buffers.put(Utilities.int2Bytes(bytes.length));
    		try {
				SettlementEntity settleEntity = RpcSerializer.deserialize(bytes, SettlementEntity.class);
				settleEntity.setOffset(msg.offset());
				bytes = RpcSerializer.serialize(settleEntity);
				buffers.put(bytes);
    		} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		if (needCheckTime) {
    			SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
				dateString = fmt.format(new Date());
				needCheckTime = false;
			}
    		
            emittedToOffset = msg.nextOffset();
        }
        byte[] fetchBytes = buffers.array();
        buffers.clear();
        Long addMessage = emittedToOffset - oldOffset;
        if (addMessage > 0) {
            LOG.info("Added " + addMessage + " messages from Kafka: " + consumer.host() + ":" + partition.partition + " to internal buffers");
            return fetchBytes;
        }
        return null;
    }
    /**
     * 计算message条数
     * @param messageSet
     * @return
     */
    private int countMessages(ByteBufferMessageSet messageSet) {
        int counter = 0;
        for (MessageAndOffset messageAndOffset : messageSet) {
            counter++;
        }
        return counter;
    }
    
    /**
     * 计算读取到的message字节总长度
     * @param messageSet
     * @return
     */
    private int countBytesMessages(ByteBufferMessageSet messageSet) {
        int counter = 0;
        for (MessageAndOffset messageAndOffset : messageSet) {
        	ByteBuffer payload = Utils.getLog(messageAndOffset.message());
            counter+=payload.limit();
        }
        return counter;
    }
   
    
    public boolean isDone() { 
    	return hasDone;
    }
    
    private class CheckTime extends TimerTask {

		@Override
		public void run() {
			needCheckTime = true;
		}
    	
    }
}
