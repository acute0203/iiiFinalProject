package com.iii.Miles;



import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class reducer extends Reducer<Text, Text, Text, Text> {

	String previous=null;
	String current=null;
	Text outputKey=new Text();
	Text outputValue=new Text();
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}

	protected void reduce(Text entry, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
				int sumOfPositive=0;
				int sumOfNegative=0;
			       for (Text val : value) {
			    	   String token[]=val.toString().split("\t");
						int positive=Integer.parseInt(token[0]);
						int negative=Integer.parseInt(token[1]);
						sumOfNegative+=negative;
						sumOfPositive+=positive;
			        }	
				String stringValue = new String(sumOfPositive+"\t"+sumOfNegative);
				outputKey.set(entry);
				outputValue.set(stringValue);
				context.write(outputKey, outputValue);
	    	   

		}
	
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
	
}
