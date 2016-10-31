/**
 * 
 */
package zhongwei.nbc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import zhongwei.nbc.utils.Util;

/**
* <p>Title: Article.java</p>
* <p>Description: </p>
* <p>Copyright: Copyright (c) 2007</p>
* <p>Company: Zhongwei</p>
* @author Zhongwei
* @date 2016年10月15日
* @version 1.0
*/
public class Article implements Writable {
	public static enum MODE{STOP, SIMPLE, WHITESPACE, STANDARD}; 
	
	private String label;
	
	private int realLength;
	private String textOriginal;
	private List<String> wordsListAll;
	private List<String> wordsVectorSorted;
	private Map<String, Integer> termFrequency;
	
	public Article(String textOriginal) {
		this.realLength = 0;
		this.textOriginal = textOriginal;
		this.wordsListAll = new LinkedList<String>();
		this.wordsVectorSorted = new LinkedList<String>();
		this.termFrequency = new TreeMap<String, Integer>();
	}
	
	@SuppressWarnings("unchecked")
	public void wordsCount() throws IOException {
		this.wordsListAll = segment(this.textOriginal, MODE.STANDARD);
		
		this.termFrequency.clear();
		for(String word : this.wordsListAll) {
			int tmp = this.termFrequency.containsKey(word) ? this.termFrequency.get(word) : 0;
			this.termFrequency.put(word, ++tmp);
		}
		
		this.wordsVectorSorted = WordsTable.sortMap(termFrequency, new Comparator<Object>() {  
            public int compare(Object o1, Object o2) {  
            	Entry<String, Integer> obj1 = (Entry<String, Integer>) o1;  
            	Entry<String, Integer> obj2 = (Entry<String, Integer>) o2;  
                return (obj2.getValue()).compareTo(obj1.getValue());  
                }  
        });
	}
	
	public void filter(List<String> stopWords) {
		this.wordsListAll.removeAll(stopWords);
		this.wordsVectorSorted.removeAll(stopWords);
		for(String word : stopWords) {
			this.termFrequency.remove(word);
		}
		
		this.realLength = this.wordsListAll.size();
	}
	
	private List<String> segment(String text, MODE mode) throws IOException {
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
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public List<String> getWords() {
		return this.wordsVectorSorted;
	}
	
	public boolean containsWord(String word) {
		return this.wordsListAll.contains(word);
	}
	
	public int getFrequencyOfWord(String word) {
		return this.containsWord(word) ? 
				this.termFrequency.get(word) : 0;
	}
	
	public String getText() {
		return this.textOriginal;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public int getLength() {
		return this.realLength;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		wordsListAll.clear();
		wordsVectorSorted.clear();
		termFrequency.clear();
		
		label = in.readUTF();
		realLength = in.readInt();
		textOriginal = in.readUTF();
		
		ArrayWritable aw = new ArrayWritable(Text.class);
		aw.readFields(in);
		wordsListAll = Util.WritableToList(aw);
		
		ArrayWritable awOther = new ArrayWritable(Text.class);
		awOther.readFields(in);
		wordsVectorSorted = Util.WritableToList(awOther);
		
		MapWritable mw = new MapWritable();
		mw.readFields(in);
		
		for(Entry<Writable, Writable> entry : mw.entrySet()) {
			termFrequency.put(((Text) entry.getKey()).toString(), ((IntWritable) entry.getValue()).get());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(label);
		out.writeInt(realLength);
		out.writeUTF(textOriginal);
		
		ArrayWritable aw = Util.listToWritable(wordsListAll);
		aw.write(out);
		
		ArrayWritable awOther = Util.listToWritable(wordsVectorSorted);
		awOther.write(out);
		
		MapWritable mw = new MapWritable();
		for(Map.Entry<String, Integer> entry : termFrequency.entrySet()) {
			mw.put(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		}
		mw.write(out);
	}
}
