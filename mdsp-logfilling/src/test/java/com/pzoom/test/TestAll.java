package com.pzoom.test;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.pzoom.mdsp.model.SettlementEntity;
import com.pzoom.mdsp.util.Utilities;
import com.pzoom.rpc.serialize.RpcSerializer;



public class TestAll {
	public static void main(String[] args) throws Exception {
		File file = new File("e:\\-user-k8test-20140930-partition_1_1412048110378-part0 (1)");  
        FileInputStream fis = new FileInputStream(file);  
        BufferedInputStream bis = new BufferedInputStream(fis);  
//		String uri = "hdfs://10.100.10.186:9000/user/k8test/20140930/partition_1_1412043547319-part0";
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(URI.create(uri), conf);
//		InputStream in = fs.open(new Path(uri));
//		BufferedInputStream bis = new BufferedInputStream(in);
		
		byte[] freflag = new byte[4];    
        int len;  
          
        try {  
        	int i = 0;
            while(bis.read(freflag)!= -1){  
            	len = Utilities.bytes2Int(freflag);
            	if(len>0)
            	{
            		System.out.println("----asdfadf-------------"+i);
            	}else
            	{
            		break;
            	}
                byte[] content = new byte[len];
                i++;
                if(i>100)
                {
                	System.out.println("------------------");
                }
                bis.read(content);
                SettlementEntity settleEntity = RpcSerializer.deserialize(content, SettlementEntity.class);
                System.out.println(settleEntity.toString()+i);
            }  
        } catch (IOException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
        }  
        bis.close();  
	}
}
