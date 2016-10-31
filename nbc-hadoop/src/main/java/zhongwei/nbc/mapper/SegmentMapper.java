/**
 * 
 */
package zhongwei.nbc.mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zhongwei.nbc.utils.Util;
import zhongwei.nbc.utils.Util.MODE;
import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: SegmentMapper.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月24日
* @version 1.0
*/
public class SegmentMapper extends Mapper<LongWritable, Text, MultiWritableComparable, DoubleWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		
		int index = line.indexOf(',');
		String label = line.substring(0, index);
		String article = line.substring(index + 1, line.length());

		//article.setLabel(label);
		
		List<String> words = (List<String>) Util.segment(article, MODE.STANDARD);
		
		context.getCounter(Util.COUNT.article).increment(1);
		
		for(String word : words) {
			List<String> list = new LinkedList<String>();
			list.add(word);
			list.add(label);
			list.add(String.valueOf(article.hashCode()));
			MultiWritableComparable wc = new MultiWritableComparable(list);
			context.write(wc, new DoubleWritable((double)1));
		}
	}

}
