/**
 * 
 */
package zhongwei.nbc.mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zhongwei.nbc.writable.MultiWritableComparable;

/**
* <p>Title: ClassifyMapper.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class ClassifyMapper extends Mapper<LongWritable, Text, Text, MultiWritableComparable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] values = line.split("[, ]");
		
		String word = new String();
		List<String> list = new LinkedList<String>();
		if(values.length == 3) {
			word = values[0];
			list.add(values[1]);
			list.add(values[2]);
		} else {
			word = values[0];
			list.add(values[1]);
			list.add(values[2]);
			list.add(values[3]);
		}
		
		context.write(new Text(word), new MultiWritableComparable(list));

	}

}
