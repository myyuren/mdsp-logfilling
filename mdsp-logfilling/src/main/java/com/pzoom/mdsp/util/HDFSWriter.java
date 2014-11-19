package com.pzoom.mdsp.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * filePath:
 * 
 * 
 * @author cby
 *
 */
public class HDFSWriter {
	private static final Logger LOG = LoggerFactory.getLogger(HDFSWriter.class);
	
	private final String rootPath;
//	private final String compressor; //不提供压缩操作
	private final long fileSize;
	
	private long counter;
	//private int partitionID;
	private String date = null;
	private int fileIndex = 0;
	private final String fileSuffix;
	private FSDataOutputStream output;
	private Path path;
	
	private static Configuration conf;
	private FileSystem fs;


	public HDFSWriter(String rootPath, int fileSize, int partitionID) {
		this.fileSize = fileSize;
		this.rootPath = rootPath;
		long curTime = System.currentTimeMillis();
		fileSuffix = "/partition_" + partitionID +"_"+curTime+"-part";
		
		String dfsStr = rootPath.substring(0, rootPath.lastIndexOf("/"));
		conf = new Configuration();
		conf.set("fs.defaultFS", dfsStr);
		
		
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			LOG.error("get hdfs FileSystem error: " + e.getMessage());
			e.printStackTrace();
		}
	}
	/**
	 * 程序启动的时候，执行的判断。
	 * @param messageDate
	 * @return
	 * @throws Exception 
	 */
	private boolean checkAndCreatefile(String messageDate){
		
		if (date == null || !date.endsWith(messageDate)) {
			//reset fileIndex
			fileIndex = 0;
			String file = getFileName(messageDate);
			LOG.info("---- checkAndCreatefile file value:"+file+" date Info:"+date+" messageDate info:"+messageDate);
			try {
				if (date != null) {
					output.close();
				}
				path = new Path(file);
				output = fs.create(path);
			} catch (Exception e) {
				LOG.error("create new file for new day error: " + e.getMessage());
				e.printStackTrace();
				return false;
			}
			date = messageDate;
			LOG.info("create new hdfs file: " + file);
		}
		return true;
	}
	
	public boolean Writer(byte[] buffers, String messageDate) {
		if (!checkAndCreatefile(messageDate)) return false;
		if (!write(buffers)) return false;
		if (!checkAndCreatePart()) return false;
		
		return true;
	}
	
	private boolean checkAndCreatePart() {
		try {
		//	FileStatus status = fs.getFileLinkStatus(path);
			long readSize = fs.open(path).available();
			LOG.info("打印出FileStatus信息："+readSize+" size value:"+fileSize);
			if (readSize >= fileSize*1024*1024) {
				
				fileIndex++;
				String file = getFileName(date);
				LOG.info("新建文件："+file);
				output.close();
				path = new Path(file);
				output =fs.create(path);
			}
		} catch (Exception e) {
			LOG.error("get filestatus error: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}
	private boolean write(byte[]  fetchBytes) {
		try {
			LOG.info("-----------write message to hdfs");
			output.write(fetchBytes);
			output.sync();
		} catch (IOException e) {
			LOG.error("write 2 hdfs error! for: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public void close() {
		try {
			output.close();
			fs.close();
		} catch (Exception e) {
			LOG.error("close fs error: " + e.getMessage());
		}
		LOG.info("close fs sucess!!");
	}


	
	private String getFileName(String messageDate) {
		StringBuilder sb = new StringBuilder();
		sb.append(rootPath).append("/").append(messageDate).append(fileSuffix).append(fileIndex);
		LOG.info("-----------file path :"+sb.toString());
	//	sb.append(".").append(compressor);
		return sb.toString();
	}
	
	public static void main(String[] args) {
		String message = "this is for test compressor!!\nhdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txthdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txthdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt\n"
				+ "hdfs://JNCDH5slave-72-58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/test.txt";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.31.72.58:8020");
		//conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		//conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path p = new Path("hdfs://10.31.72.58:8020/user/adrd/kafka2hadoop/countinfo/2014/03/12/partition_0-part0.lzo");
			FileStatus status = fs.getFileStatus(p);
			System.out.println("file lenth = " + status.getLen());
			
			/*fs.createNewFile(p);	
			Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.SnappyCodec");
			CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
			
			FSDataOutputStream output = fs.create(p);
			output.writeUTF(message);
			output.close();*/
			//CompressionOutputStream output = codec.createOutputStream(fs.create(p));
			//output.write(message.getBytes());
			//output.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				
				fs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
}
