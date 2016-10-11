/**
 * 
 */
package zhongwei.kmeans.clustering;

import java.io.ByteArrayInputStream;  
import java.io.ByteArrayOutputStream;  
import java.io.IOException;  
import java.io.OutputStream; 
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zhongwei.kmeans.label.LabelDriver;

/**
* <p>Title: ClusteringDriver.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月9日
* @version 1.0
*/
public class ClusteringDriver extends Configured implements Tool {
	public int k = 10;
	public static String CENTERS_FILEPATH = "";
	public static int DIMENSION = 2;
	public static final String HDFS_PATH = "hdfs://192.168.1.104:9000";

	public int run(String args[]) throws Exception {
		CENTERS_FILEPATH = HDFS_PATH + args[3];//new String("");
		DIMENSION = Integer.valueOf(args[1]);//2;
		
		Configuration conf = new Configuration();
		conf.set("kmeans.centers.filepath", CENTERS_FILEPATH);
		conf.set("kmeans.dimension", String.valueOf(DIMENSION));
		
		Job job = Job.getInstance(conf, "KMeans");
		job.setJarByClass(zhongwei.kmeans.clustering.ClusteringDriver.class);
		job.setMapperClass(ClusteringMapper.class);
		job.setReducerClass(ClusteringReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(HDFS_PATH + "/test/input"));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_PATH + "/test/output"));

		if (!job.waitForCompletion(true))
			return 1;
		
		return 0;
	}

	/*
	 * parm : args[0]-k, args[1]-dimension, args[2]-numIter, args[3]-centersfile
	 */
	public static void main(String[] args) throws Exception {
		ClusteringDriver.init(args);
		
		int res = ToolRunner.run(new Configuration(), new ClusteringDriver(), args);
		
		int numIter = Integer.valueOf(args[2]);
		int i = 0;
		
		while(i < numIter) {
			if(res == 0) {
				ClusteringDriver.renameAndCover(HDFS_PATH + "/test/output", HDFS_PATH + args[3]);
				res = ToolRunner.run(new Configuration(), new ClusteringDriver(), args);
				i++;
				System.out.println("Iterator: " + i);
			}
		}
		
		res = ToolRunner.run(new Configuration(), new LabelDriver(), args);
		if(res == 0) {
			System.out.println("Complete!");
			System.exit(0);
		}
	}
	
	public static void renameAndCover(String src, String dst) throws IOException {
		Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "hadoop,hadoop");   
        FileSystem fs = FileSystem.get(URI.create(src), conf);
        
        Path srcPath = new Path(src + "/part-r-00000");
        Path dstPath = new Path(dst);
        if(fs.exists(dstPath))
        	fs.delete(dstPath, false);
        
        FSDataInputStream in = null;   
        ByteArrayOutputStream out = new ByteArrayOutputStream();  
        
        String str = new String();
        try{
            in = fs.open(srcPath);   
            IOUtils.copyBytes(in,out,50,false);  //用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上   
            str = out.toString().replace('\t', ',');  
        } finally {   
        	IOUtils.closeStream(in);  
        }  
          
        FileSystem filesystem = FileSystem.get(URI.create(dst), conf);
       
        OutputStream out2 = filesystem.create(dstPath);   
        IOUtils.copyBytes(new ByteArrayInputStream(str.getBytes()), out2, 4096,true); //write string 
        
        if(fs.exists(new Path(src)))
        	fs.delete(new Path(src), true);
	}
	
	public static void init(String args[]) throws IOException {
		String[] clist;  
        int k = Integer.valueOf(args[0]);
        String inpath = HDFS_PATH + "/test/input";  //cluster  
        String outpath = HDFS_PATH + args[3];  //center 
        
        StringBuffer string = new StringBuffer("");  
         
        Configuration conf = new Configuration(); //读取hadoop文件系统的配置  
        conf.set("hadoop.job.ugi", "hadoop,hadoop");   
        FileSystem fs = FileSystem.get(URI.create(inpath),conf); //FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统   
        FSDataInputStream in = null;   
        ByteArrayOutputStream out = new ByteArrayOutputStream();  
        try{
            in = fs.open( new Path(inpath) );   
            IOUtils.copyBytes(in,out,50,false);  //用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上   
            clist = out.toString().split("\n");  
        } finally {   
        	IOUtils.closeStream(in);  
        }  
          
        FileSystem filesystem = FileSystem.get(URI.create(outpath), conf);
          
        for(int i=0;i<k;i++)  
        {  
            int j=(int) (Math.random()*100) % clist.length;  
            if(string.toString().contains(clist[j]))  // choose the same one  
            {  
                k++;  
                continue;  
            }  
            string.append(clist[j] + "\n");  
        }  
        OutputStream out2 = filesystem.create(new Path(outpath) );   
        IOUtils.copyBytes(new ByteArrayInputStream(string.toString().getBytes()), out2, 4096,true); //write string  
        //System.out.println(string);  
	}
}
