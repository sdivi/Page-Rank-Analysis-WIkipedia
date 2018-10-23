/*SAILOKESH DIVI - 800888466 */

package org.myorg;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class DocWordCount extends Configured implements Tool {
  
  private static final Logger LOG = Logger.getLogger(DocWordCount.class);
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new DocWordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
  //  job.waitForCompletion(true);
    return job.waitForCompletion(true) ? 0 : 1;
    
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String delimeter ="####"; //String delimeter to separate the filename
    private boolean caseSensitive = false;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    //map function
    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	//Filesplit to get the filename
    	
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = delimeter+fileSplit.getPath().getName(); 
		
      String line = lineText.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty()) {
            continue;
          }
          word = word + filename;
          currentWord = new Text(word);
          context.write(currentWord,one);
        }         
      }
  }

  //Reduce function
  
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) { 
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
    
}
