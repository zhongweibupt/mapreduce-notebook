/**
 * 
 */
package topk;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* <p>Title: MyMapper.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016Äê10ÔÂ2ÈÕ
* @version 1.0
*/
public class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public static final int K = 10;
	private Heap<String> heap = new Heap<String>(10, new MinComparator<String>());

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] str = ivalue.toString().split(",");
		heap.add(str[0]);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Text text = new Text();
		for(String str : heap.getData()) {
			if(str != null) {
				text.set(str);
				context.write(NullWritable.get(), text);
			}
		}
	}
}
