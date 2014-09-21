

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// different with traditional word count, in this case, user can specufy the list of words interested in
public class WCDistributedCache {
 
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // distributed cache
    private Set<String> patterns = new HashSet<String>(); // hashset contains all words user interested in
    
    public void setup(Context context) throws IOException, FileNotFoundException {
    	Path[] patternsFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	for (Path path:patternsFile){// only one pattern file case was tested
			BufferedReader fis = new BufferedReader(new FileReader(path.toString()));
			String line = null;
			// save all unique user words into hashset; later the mapper only count words in this set
			while ((line = fis.readLine()) != null) {
				 StringTokenizer itr = new StringTokenizer(line);
			      while (itr.hasMoreTokens()) {
			    	  patterns.add(itr.nextToken());
			      }
			}
			
    	}
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	String currWord = itr.nextToken();
    	// 1. if user didn't specify any pattern, count all words as traditional single word count
    	// 2. if user specify some non-empty word lists, only count those in list
    	if (patterns.isEmpty() || patterns.contains(currWord)){
    		word.set(currWord);
            context.write(word, one);
    	}
      }
    }
  }
  
  
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length < 3) {
      System.err.println("Usage: wordcount <in> <out> [patternFiles]");
      System.exit(2);
    }
    
    Job job = new Job(conf, "word count distributed cache");
    job.setJarByClass(WCDistributedCache.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    
    // user can specify pattenfiles which contain all the word pattern user interested
    if (otherArgs.length == 4){
    	DistributedCache.addCacheFile(new Path(otherArgs[3]).toUri(), job.getConfiguration());
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}