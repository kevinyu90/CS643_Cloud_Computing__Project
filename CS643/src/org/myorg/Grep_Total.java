package org.myorg;


import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
 
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Grep_Total {
    
    public static class Map extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Pattern pattern;
        private int group;
        
        public void configure(JobConf job) {
            pattern = Pattern.compile(job.get("mapred.mapper.regex"),Pattern.CASE_INSENSITIVE);//make it case insensitive
            group = job.getInt("mapred.mapper.regex.group", 0);
        }
        
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
        
	    String line = value.toString();
            String line1 = line.replaceAll("<[^>]+>","");//eliminate the content inside the html tags and extract the real text of the htm pages                
            Matcher matcher = pattern.matcher(line1);
            while (matcher.find()) {
                    String str = matcher.group(group).toString();
                    str = str.toLowerCase();//convert to lowercase for counting
                    output.collect(new Text(str), one);            
            }
        }
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
            return;
        }
        JobConf conf = new JobConf(Grep_Total.class);
        conf.setJobName("Grep_Total");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.set("mapred.mapper.regex", args[2]);
        if (args.length == 4) {
            conf.set("mapred.mapper.regex.group", args[3]);
        }
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
