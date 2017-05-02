package org.myorg;




import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Count {

	private final static IntWritable EDU = new IntWritable(1);
	private final static IntWritable POL = new IntWritable(2);
	private final static IntWritable SPT = new IntWritable(3);
	private final static IntWritable AGR = new IntWritable(4);
	
	public static int count(String sentence, String keyword) {
		int edu = sentence.indexOf(keyword);
		int times = 0;
		while (edu != -1) {
			times++;
			sentence = sentence.substring(edu + keyword.length());
		    edu = sentence.indexOf(keyword);
		}
		return times;
	}
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				String inputLine = s.toLowerCase();
				String stateName = ((FileSplit) context.getInputSplit()).getPath().getName();
				word.set(stateName);
//				check how many times that "education", "politics", "sports", "agriculture" occurred in this word
				int j = Count.count(inputLine, "education");
				for(int i = 0; i < j; i++) {
					context.write(EDU, word);
				}
				j = Count.count(inputLine, "politics");
				for(int i = 0; i < j; i++) {
					context.write(POL, word);
				}
				j = Count.count(inputLine, "sports");
				for(int i = 0; i < j; i++) {
					context.write(SPT, word);
				}
				j = Count.count(inputLine, "agriculture");
				for(int i = 0; i < j; i++) {
					context.write(AGR, word);
				}
			}
		}
	}
	

	public static class IntSumReducer extends
	Reducer<IntWritable, Text, Text, IntWritable> {
		Map<Integer, Map<String, Integer>> propertyMap = new HashMap<Integer, Map<String, Integer>>();
		
		public void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
			int sum = 0;
			Map<String, Integer> stateMap = null;
			Integer propertyCode = key.get();
			if(propertyMap.get(propertyCode) == null) {
				stateMap = new HashMap<String, Integer>();
				propertyMap.put(propertyCode, stateMap);
			} else {
				stateMap = propertyMap.get(propertyCode);
			}
			for (Text val : values) {
				String stateName = val.toString();
				
				if(stateMap.get(stateName) == null) {
					sum++;
					stateMap.put(stateName, new Integer(1));
				} else {
					sum++;
					stateMap.put(stateName, stateMap.get(stateName)+1);
				}
			}
			
			Text proTx = new Text();
			if(propertyCode == EDU.get()) {
				proTx.set("education	");
			} else if(propertyCode == POL.get()) {
				proTx.set("politics	");
			} else if(propertyCode == SPT.get()) {
				proTx.set("sports	");
			} else if(propertyCode == AGR.get()) {
				proTx.set("agriculture	");
			} 
			System.out.println(proTx.toString()+"total:"+sum);
			context.write(proTx, new IntWritable(sum));
			Map<String, Integer> sortedMapDesc = sortByComparator(stateMap, false);
			Iterator stateIterator = sortedMapDesc.entrySet().iterator();
			int i = 1;
			while(stateIterator.hasNext()) // &&i<=3
	        {
				Entry<String, Integer> entry = (Map.Entry)stateIterator.next();
	            System.out.println("No."+(i)+"  " + entry.getKey() + " : "+ entry.getValue());
				context.write(new Text("No."+(i)+"  " + entry.getKey() + " : "), new IntWritable(entry.getValue()));
	            i++;
	        }
		}
	}
	
	public static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order)
    {
        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());
        // sort the list by values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());
                }
            }
        });
        // keep order with LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Count.class);
		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));

		
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}