/**
 * 
 */
package zhongwei.kmeans;

import java.io.IOException;
import java.util.ArrayList;
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
public class ClusteringReducer extends Reducer<Object, Object, Text, Text> {

	public void reduce(Object _key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
		// process values, key - cluster_id, values - data
		if(!values.iterator().hasNext()) {
			//throw new IllegalStateException("Size of members in " + id + " is empty.");  
			//center = KMeans.chooseCenter(1);
			//return;
		}
		
		Configuration conf = context.getConfiguration();
		
		StringBuffer buf = new StringBuffer();
		
		for (int i = 0; i < Integer.valueOf(conf.get("kmeans.dimension")); i++) {  
            double tmp = 0.0;
            int count = 0;
			for(Object val : values) {
            	tmp += ((Point)val).getData().get(i);
            	count++;
            }
			tmp /= count;
			buf.append(tmp);
			if(i != Integer.valueOf(conf.get("kmeans.dimension"))) {
				buf.append(",");
			}
        }  
		
        Text center = new Text(buf.toString());
        context.write(new Text(((Cluster)_key).getId()), center);
	}
}
