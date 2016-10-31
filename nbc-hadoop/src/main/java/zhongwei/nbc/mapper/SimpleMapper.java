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

/**
* <p>Title: SimpleMapper.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月27日
* @version 1.0
*/
public class SimpleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] values = line.split("[, ]");
		
		String label = values[0];
		double sumOfIdf = Double.valueOf(values[3]);
		
		context.write(new Text(label), new DoubleWritable(sumOfIdf));
	}
}
