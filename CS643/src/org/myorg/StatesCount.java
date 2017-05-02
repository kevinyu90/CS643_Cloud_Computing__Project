
package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class StatesCount {
	
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
	
	
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    

    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  
			String inputLine = itr.nextToken().toLowerCase();
			String stateName = ((FileSplit) context.getInputSplit()).getPath().getName();
			word.set(stateName);
			int j = StatesCount.count(inputLine, "education");
			for(int i = 0; i < j; i++) {
				context.write(word, new Text("education"));
			}
			j = StatesCount.count(inputLine, "politics");
			for(int i = 0; i < j; i++) {
				context.write(word, new Text("politics"));
			}
			j = StatesCount.count(inputLine, "sports");
			for(int i = 0; i < j; i++) {
				context.write(word, new Text("sports"));
			}
			j = StatesCount.count(inputLine, "agriculture");
			for(int i = 0; i < j; i++) {
				context.write(word, new Text("agriculture"));
			}


			
      }
    }
  }
  
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,IntWritable> {
	  private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {

		
    	int eduSum = 0;
    	int polSum = 0;
    	int sptSum = 0;
    	int argSum = 0;
    	int max = 0;
    	
    	String flag = null;
    	for (Text val : values){
    		if(val.equals(new Text("education"))){
    			eduSum++;
    		} 
    		if(val.equals(new Text("politics"))){
    			polSum++;
    		} 
    		if(val.equals(new Text("sports"))){
    			sptSum++;
    		} 
    		if(val.equals(new Text("agriculture"))){
    			argSum++;
    		} 

    	}
    	HashMap<String, Integer> map = new HashMap<>();
    	map.put("education", eduSum);
    	map.put("politics", polSum);
    	map.put("sports", sptSum);
    	map.put("agriculture", argSum);
    	List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
    	Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>() {   
    	    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {      
    	        return (o2.getValue() - o1.getValue()); 
//    	        return (o1.getKey()).toString().compareTo(o2.getKey());
    	    }
    	}); 
    	
    	
    	max = Math.max(eduSum, Math.max(polSum, Math.max(sptSum, argSum)));
    	if(max == eduSum){
			flag = "education";
		}
    	if(max == polSum){
			flag = "politics";
		}
    	if(max == sptSum){
			flag = "sports";
		}
    	if(max == argSum){
			flag = "agriculture";
		}
    	result.set(eduSum);
    	context.write(new Text(key+" education"), result);
    	result.set(polSum);
    	context.write(new Text(key+" politics: "), result);
    	result.set(sptSum);
    	context.write(new Text(key+" sports: "), result);
    	result.set(argSum);
    	context.write(new Text(key+" agriculture: "), result);
    	result.set(max);
    	context.write(new Text(key+" dominant: "+flag), result);
    	context.write(new Text(key+" Ranking: "+infoIds.get(0).getKey()+">"+infoIds.get(1).getKey()+">"+infoIds.get(2).getKey()+">"+infoIds.get(3).getKey()), null);
    	
//    	int sum = 0;
//        for (IntWritable val : values) {
//          sum += val.get();
//        }
//        result.set(sum);
//        context.write(key, result);
    	
    }
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(StatesCount.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(Combiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
   
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
