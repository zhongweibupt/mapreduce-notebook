/**
 * 
 */
package zhongwei.kmeans.label;

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

import zhongwei.kmeans.clustering.ClusteringDriver;

/**
* <p>Title: ClusteringDriver.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月9日
* @version 1.0
*/
public class LabelDriver extends Configured implements Tool {

	public int run(String args[]) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("kmeans.centers.filepath", ClusteringDriver.CENTERS_FILEPATH);
		conf.set("kmeans.dimension", String.valueOf(ClusteringDriver.DIMENSION));
		
		Job job = Job.getInstance(conf, "KMeans");
		job.setJarByClass(zhongwei.kmeans.label.LabelDriver.class);
		job.setMapperClass(LabelMapper.class);
		job.setReducerClass(LabelReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(ClusteringDriver.HDFS_PATH + "/test/input"));
		FileOutputFormat.setOutputPath(job, new Path(ClusteringDriver.HDFS_PATH + "/test/label"));

		if (!job.waitForCompletion(true))
			return 1;
		
		return 0;
	}
}
