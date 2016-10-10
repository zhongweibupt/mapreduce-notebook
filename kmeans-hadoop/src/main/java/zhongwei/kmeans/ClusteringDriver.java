/**
 * 
 */
package zhongwei.kmeans;

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
	public String CENTERS_FILEPATH = "";
	public int DIMENSION = 2;

	public int run(String args[]) throws Exception {
		CENTERS_FILEPATH = "hdfs://10.2.43.230:9000/test/centers";//args[3];//new String("");
		DIMENSION = 2;//Integer.valueOf(args[1]);//2;
		
		Configuration conf = new Configuration();
		conf.set("kmeans.centers.filepath", CENTERS_FILEPATH);
		conf.set("kmeans.dimension", String.valueOf(DIMENSION));
		
		Job job = Job.getInstance(conf, "KMeans");
		job.setJarByClass(zhongwei.kmeans.ClusteringDriver.class);
		job.setMapperClass(ClusteringMapper.class);
		job.setReducerClass(ClusteringReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://10.2.43.230:9000/test/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.2.43.230:9000/test/output"));

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
				//TODO : copyfile, output->centers
				ClusteringDriver.renameAndCross("hdfs://10.2.43.230:9000/test/output", args[3]);
				res = ToolRunner.run(new Configuration(), new ClusteringDriver(), args);
				i++;
				System.out.println("Iterator: " + i);
			}
		}
	}
	
	public static void renameAndCross(String src, String dst) throws IOException {
		
	}
	
	public static void init(String args[]) throws IOException {
		String[] clist;  
        int k = Integer.valueOf(10);//args[0]);
        String inpath = "hdfs://10.2.43.230:9000/test/input";  //cluster  
        String outpath = "hdfs://10.2.43.230:9000/test/centers";//args[3];  //center 
        
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
