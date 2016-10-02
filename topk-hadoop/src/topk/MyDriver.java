/**
 * 
 */
package topk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
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
* <p>Title: MyDriver.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016Äê10ÔÂ2ÈÕ
* @version 1.0
*/public class MyDriver extends Configured implements Tool {
	/* (non-Javadoc)
	 * @see javax.tools.Tool#run(java.io.InputStream, java.io.OutputStream, java.io.OutputStream, java.lang.String[])
	 */
	@Override
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("TopK");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass((Class<? extends Mapper>) MyMapper.class);
		job.setReducerClass((Class<? extends Reducer>) MyReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass((Class<? extends org.apache.hadoop.mapreduce.InputFormat>) TextInputFormat.class);
		job.setOutputFormatClass((Class<? extends org.apache.hadoop.mapreduce.OutputFormat>) TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.119:9000/test/test.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.119:9000/test/output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyDriver(), args);
		System.exit(res);
	}
}
