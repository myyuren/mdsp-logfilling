package com.pzoom.mdsp.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pzoom.mdsp.model.SettlementEntity;
import com.pzoom.rpc.serialize.RpcSerializer;

public class FileReaderUtil {
	private static Logger LOG = LoggerFactory.getLogger(FileReaderUtil.class);

	/**
	 * 获取最后一行的内容
	 * 
	 * @param uri
	 * @return
	 * @throws IOException
	 */
	public static String readLastLine(String uri) throws IOException {
		long start = System.currentTimeMillis();
		String encoding = "UTF-8";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		String lastLine = null;
		try {
			in = fs.open(new Path(uri));
			InputStreamReader read = new InputStreamReader(in, encoding);
			BufferedReader bufferedReader = new BufferedReader(read);
			String lineTxt = null;
			while ((lineTxt = bufferedReader.readLine()) != null) {
				lastLine = lineTxt;
			}
			long end = System.currentTimeMillis();
			long inteval = end - start;
			LOG.info("the lastLine value is :" + lastLine + " read time :"
					+ inteval);
			read.close();
		} catch (Exception e) {
			LOG.error("读取文件内容出错" + e.getMessage());
		}

		return lastLine;
	}

	/**
	 * 获取二进制最后一条消息记录的内容
	 * 
	 * @param uri
	 * @return
	 * @throws IOException
	 */
	public static long readStreamLastOffset(String uri) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		SettlementEntity settleEntity = null;
		InputStream in = fs.open(new Path(uri));
		BufferedInputStream bis = new BufferedInputStream(in);
		byte[] freflag = new byte[4];
		int len;
		int i=0;
		byte[] content = null;
		long a = System.currentTimeMillis();
		while (bis.read(freflag) != -1) {
			i++;
			len = Utilities.bytes2Int(freflag);
			if(len==0)
			{
				break;
			}
			content = new byte[len];
			bis.read(content);
			
		}
		try {
			settleEntity = RpcSerializer.deserialize(content,
					SettlementEntity.class);
			LOG.info(settleEntity.toString()+i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long b = System.currentTimeMillis();
		long c = b-a;
		LOG.info(c+"毫秒");
		bis.close();
		if (settleEntity != null) {
			return settleEntity.getOffset();
		}
		return 0L;
	}

	/***
	 * 
	 * 读取HDFS某个文件夹的所有 文件
	 * 
	 * **/
	public static List<String> readHDFSListAll(String uri,int partition) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		// 获取日志文件的根目录
		Path listf = new Path(uri);
		// 获取根目录下的所有2级子文件目录
		FileStatus stats[] = fs.listStatus(listf);

		List<String> filePathList = new ArrayList<String>();
		for (int i = 0; i < stats.length; i++) {
			// 获取子目录下的文件路径
			String tempStr = stats[i].getPath().toString();
			if(tempStr.contains("partition_"+partition))
			{
				filePathList.add(tempStr);
			}
		}
		
		Collections.sort(filePathList, new Comparator<String>() {
			public int compare(String arg0, String arg1) {
				return arg0.substring(arg0.lastIndexOf("_") + 1,
						arg0.lastIndexOf("-")).compareTo(
						arg1.substring(arg1.lastIndexOf("_") + 1,
								arg1.lastIndexOf("-")));
			}
		});

		return filePathList;

	}
	
	
	/***
	 * 
	 * 读取指定目录的文件夹并按照文件名排序
	 * 
	 * **/
	public static List<String> readHDFSListFolder(String uri) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		// 获取日志文件的根目录
		Path listf = new Path(uri);
		// 获取根目录下的所有2级子文件目录
		FileStatus stats[] = fs.listStatus(listf);
		
		List<String> filePathList = new ArrayList<String>();
		for (int i = 0; i < stats.length; i++) {
			// 获取子目录下的文件路径
			String tempStr = stats[i].getPath().toString();
			filePathList.add(tempStr);
		}
		Collections.sort(filePathList, new Comparator<String>() {
            public int compare(String arg0, String arg1) {
                return arg0.substring(arg0.lastIndexOf("/")+1).compareTo(arg1.substring(arg1.lastIndexOf("/")+1));
            }
        });
		
		return filePathList;

	}

	public static void main(String[] args) throws Exception {
		File workaround = new File(".");
		System.getProperties().put("hadoop.home.dir",
				workaround.getAbsolutePath());
		new File("./bin").mkdirs();
		new File("./bin/winutils.exe").createNewFile();
		String uri = "hdfs://cby-hadoop:9000/user/hadoop/log_analyze";
		List<String> list = FileReaderUtil.readHDFSListAll(uri,0);
		for (String path : list) {
			System.out.println(path);
		}
	}
}
