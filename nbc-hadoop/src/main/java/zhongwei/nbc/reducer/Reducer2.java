/**
 * 
 */
package zhongwei.nbc.reducer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zhongwei.nbc.mapper.SegmentMapper;
import zhongwei.nbc.mapper.SegmentMapper.COUNT;
import zhongwei.nbc.utils.Util;

/**
* <p>Title: Reducer2.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月25日
* @version 1.0
*/
public class Reducer2 extends Reducer<Text, Text, Text, IntWritable> {
	private List<String> stopWordsList;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		stopWordsList = new LinkedList<String>();
		Util.SIZE_ARTICLES = context.getCounter(Util.COUNT.article).getValue();
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		writeStopWords(this.stopWordsList, Util.STOPWORDS_FILE);
		Util.SIZE_WORDTABLE = context.getCounter(Util.COUNT.word).getValue();
	}
	
	public void writeStopWords(List<String> stopWords, String fileNamePath) throws IOException {
		Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "hadoop,hadoop");   
        FileSystem fs = FileSystem.get(URI.create(fileNamePath), conf);
        
		Path dstPath = new Path(fileNamePath);
        if(fs.exists(dstPath))
        	fs.delete(dstPath, false);
        
        OutputStream out = fs.create(dstPath);   
        IOUtils.copyBytes(new ByteArrayInputStream(stopWords.toString().getBytes()), out, 4096,true); //write string 
	}

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		int count = 0;
		for (Text val : values) {
			count++;
		}
		
		if(count < Util.SIZE_ARTICLES * Util.THRESHOLD_STOP) {
			context.write(_key,new IntWritable(count));
			context.getCounter(Util.COUNT.word).increment(1);
		} else {
			stopWordsList.add(_key.toString());
		}
	}
}
