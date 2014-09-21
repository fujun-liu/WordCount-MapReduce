

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDouble {
  // mapper, difference with traditional word count: this mapper treat two consecutive words as single element
  public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // easy implementation: treat two consecutive words with one space between as a new string
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String prev = "";
      while (itr.hasMoreTokens()) {
    	String curr = itr.nextToken();
    	if (!prev.isEmpty()){
    		word.set(prev + " " + curr);
            context.write(word, one);
    	}
    	prev = curr;
      }
    }
  }
  
  // reducer, the same as single word count
  public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
	// read configuration, can optionally be specified by resource file
    Configuration conf = new Configuration(); 
    // use GenericOptionsParser to handle some hadoop predefined commands
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    if (otherArgs.length < 3) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCountDouble.class);
    
    job.setMapperClass(WordCountMapper.class); // set
    job.setCombinerClass(WordCountReducer.class); // combiner class is same as reduce
    job.setReducerClass(WordCountReducer.class); // reducer class
    
    // specify numreduce tasks
    /*if (otherArgs.length == 3){
    	job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
    }*/
    
    // set output format, this should be consist with reduce out
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[1])); // set input path
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); // set output path
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}