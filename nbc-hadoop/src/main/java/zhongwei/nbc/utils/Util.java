/**
 * 
 */
package zhongwei.nbc.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
* <p>Title: Util.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月23日
* @version 1.0
*/
public class Util {
	public static enum COUNT{article, word};
	public static enum MODE{STOP, SIMPLE, WHITESPACE, STANDARD}

	public static final double LAPLACE_SMOOTH_PARA = 1.0;

	public static double THRESHOLD_STOP = 0.3;
	public static long SIZE_ARTICLES = Long.MAX_VALUE;
	public static long SIZE_WORDTABLE = 1;
	public static String STOPWORDS_FILE = "";
	
	public static <T> List<String> sortMap(Map<String, T> map, Comparator comparator) {
		List<Entry<String, T>> list = new LinkedList<Entry<String, T>>(map.entrySet());
		
		Collections.sort(list, comparator);
		
		List<String> listSorted = new LinkedList<String>();
		for(Entry<String, T> entry : list) {
			listSorted.add(entry.getKey());
		}
		return listSorted;
	}
	
	public static List<String> segment(String text, MODE mode) throws IOException {
		List<String> wordsList = new LinkedList<String>();
		
		Analyzer analyzer;
		switch(mode) {
		case STOP :
			analyzer = new StopAnalyzer();
			break;
		case SIMPLE :
			analyzer = new SimpleAnalyzer();
			break;
		case WHITESPACE:
			analyzer = new WhitespaceAnalyzer();
			break;
		case STANDARD :
			analyzer = new StandardAnalyzer();
			break;
		default :
			analyzer = new StandardAnalyzer();
			break;
		}
		
		TokenStream stream  = analyzer.tokenStream("", new StringReader(text));
		//Stemming
		stream = new PorterStemFilter(stream);
		stream.reset();
		CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);
        while(stream.incrementToken()){
        	String word = cta.toString();
            wordsList.add(word);
        }
        
		return wordsList;
	}
	
	public static ArrayWritable listToWritable(List<String> list) {
		Text[] array = new Text[list.size()];
		
		int i = 0;
		for(String word : list) {
			array[i++] = new Text(word);
		}
		ArrayWritable aw = new ArrayWritable(Text.class, array);
		
		return aw;
	}
	
	public static List<String> WritableToList(ArrayWritable aw) {
		List<String> list = new LinkedList<String>();
		
		Text[] array= (Text[]) aw.toArray();		
		for(int i = 0; i < array.length; i++) {
			list.add(array[i].toString());
		}
		
		return list;
	}
}
