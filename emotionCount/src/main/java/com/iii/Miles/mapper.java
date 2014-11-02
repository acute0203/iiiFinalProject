package com.iii.Miles;



import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {

	Text outputKey=new Text();
	Text outputValue=new Text();
	
	@Override
	protected void setup(Context context){
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try{
			int positive=0;
			int negative=0;
			String line=value.toString();
			String tokens[]=line.split(" ");
			String date=tokens[0];
			String voccount=tokens[1];
			HashMap<String,Integer> emotion_dict=new HashMap<String,Integer>();//insert emotion dictionary
			HashMap<String,Integer> today_emotion=new HashMap<String,Integer>();
		   String token2step[]=voccount.split("\t");
		   for(int i=0;i<voccount.length();i++)
			   today_emotion.put(token2step[0], Integer.parseInt(token2step[1]));			
		   @SuppressWarnings({ "unchecked", "rawtypes" })
		HashSet keySet = new HashSet(today_emotion.keySet());
		   @SuppressWarnings("rawtypes")
		Iterator it = keySet.iterator();
		   String kkey;
		   while (it.hasNext()) {
			   kkey = it.next().toString();
			   if (emotion_dict.get(kkey)<0){
				   negative+=emotion_dict.get(kkey)*today_emotion.get(kkey);
				   
			   }
			   else if(emotion_dict.get(kkey)>0){
				   positive+=emotion_dict.get(kkey)*today_emotion.get(kkey);
			   }
			   else
				   continue;
		   }
		   String stringValue = new String(positive+"\t"+negative);
			outputKey.set(date);			
			outputValue.set(stringValue);
			context.write(outputKey, outputValue);
		}catch(Exception e){
			//ignore
		}
	}
	
	@Override
	protected void cleanup(Context context){
	}	
	
}
