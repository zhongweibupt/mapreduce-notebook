/**
 * 
 */
package zhongwei.kmeans.label;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* <p>Title: ClusteringReducer.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月9日
* @version 1.0
*/
public class LabelReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values, key - cluster_id, values - data
		if(!values.iterator().hasNext()) {
			//throw new IllegalStateException("Size of members in " + id + " is empty.");  
			//center = KMeans.chooseCenter(1);
			//return;
		}
		
		for(Text val : values) {
			context.write(_key, val);
		}
	}
}
