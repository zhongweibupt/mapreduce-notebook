/**
 * 
 */
package zhongwei.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* <p>Title: ClusteringMapper.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月9日
* @version 1.0
*/
public class ClusteringMapper extends Mapper<LongWritable, Text, Object, Object> {
	private List<Point> centers = new ArrayList<Point>();
	private List<Cluster> clusters = new ArrayList<Cluster>();
	
	@Override
	public void setup(Context context) 
			throws IOException, InterruptedException {
		//TODO : 读取centers
		Configuration conf = context.getConfiguration();
		this.centers = KMeans.readCsv(conf.get("kmeans.centers.filepath"), 
				Integer.valueOf(conf.get("kmeans.dimension")));
		
		this.clusters.clear();;
    	
    	for(Point point : centers) {
    		List<Point> members = new LinkedList<Point>();
    		Cluster cluster = new Cluster(point.getId(), point, members);
    		clusters.add(cluster);
    	}
	}

	public void map(LongWritable ikey, Text ivalue, Context context) 
			throws IOException, InterruptedException {
		String[] str = ivalue.toString().split(",");
		
		String id = str[0];
		List<Double> data = new ArrayList<Double>();
		for(int i = 1; i < str.length; i++) {
			data.add(Double.valueOf(str[i]));
		}
		
		Point point = new Point(id, data);
		Cluster cluster = point.chooseCluster(clusters, KMeans.distance);
		context.write(cluster, point);
	}
}
