/*SAILOKESH DIVI - 800888466*/

package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class TFIDF extends Configured implements Tool {
	
  private static final String OUTPUT_PATH = "intermediate_output";	 //intermediate output path 
  private static final Logger LOG = Logger.getLogger(TFIDF.class);
  private static long fileCount;
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TFIDF(), args);
    System.exit(res);
  }

  //Job1 to calculate the intermediate WF output. 
  
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);   //reducer not used as combiner as the key is modified
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.waitForCompletion(true);
    
    Configuration conf = job.getConfiguration();
    Configuration test = new Configuration(); //creating new configuration to pass no of files
    String dirs = conf.get("mapred.input.dir");
    String[] arrDirs = dirs.split(",");
    int numDirs = arrDirs.length;
    FileSystem fs = FileSystem.get(conf);
    Path pt = new Path(dirs);
    ContentSummary cs = fs.getContentSummary(pt);
    fileCount = cs.getFileCount();
    test.setLong("filecount",fileCount);
    System.out.println("**********"+fileCount);
    //System.out.println("**********"+counter);
    

   // job2 to calculate the TF-IDF 
    
    Job job2 = Job.getInstance(test, "wordcountIF");
    job2.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    job2.setMapperClass(MapIF.class);
    //job2.setCombinerClass(ReduceIF.class);
    job2.setReducerClass(ReduceIF.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    return job2.waitForCompletion(true) ? 0 : 1;

  }

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static DoubleWritable one = new DoubleWritable(1);
    private Text word = new Text();
    private String delimeter ="####";
    private boolean caseSensitive = false;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
    	//To get the filename 
    	
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

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      double WF=0;
      for (DoubleWritable count : counts) {
    	 // WF(t,d) = 1 + log(TF(t,d)) 
        sum += count.get();
      }
    // Write WF to context
      context.write(word, new DoubleWritable((1+ Math.log10(sum))));
    }
  }
  
  public static class MapIF extends Mapper<LongWritable, Text, Text, Text>{
	  
	  //  private final static DoubleWritable one = new DoubleWritable(1);
	    private Text word = new Text();
	    private String delimeter ="####";
	    private boolean caseSensitive = false;
	 
	    protected void setup(Mapper.Context context)
	      throws IOException,
	        InterruptedException {
	      Configuration config = context.getConfiguration();
	      this.caseSensitive = config.getBoolean("wordcountIF.case.sensitive", false);
	    }

	@Override
	protected void map(LongWritable offset, Text lineText, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
   	
      String line = lineText.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }

      String[] inputline = line.split("\\s");   //split the line with space
      String[] word_file = inputline[0].trim().split("####");  //then split with the delimeter.
      String value = word_file[1]+"="+inputline[1];            //append it to the WF
      context.write(new Text(word_file[0]),new Text(value));
                
      }
	    
  }
  
  public static class ReduceIF extends Reducer<Text, Text,Text,Text>{

		@Override
		protected void reduce(Text word, Iterable<Text> counts, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double sum = 0;
		    double IWF=0;
		    int counter=0;
		    HashMap<String,String> hm = new HashMap();
		    long totalfiles =Long.parseLong( context.getConfiguration().get("filecount"));
		      
			for(Text count:counts){
				
				String[] Fileparameters = count.toString().trim().split("=");
				String filename= Fileparameters[0];
				String WF = Fileparameters[1];
				hm.put(filename,WF);
				counter ++;
			}
			
			//System.out.println("------"+counter);
			for (Entry<String, String> entry: hm.entrySet())
			
			{
			 String finalword = word.toString()+"####"+entry.getKey();
			 String WF =entry.getValue();
			 IWF = Math.log10((double)totalfiles/counter);
			 
			 //TF-IDF(t, d) =  WF(t,d) * IDF(t)
			 Double TFIDF = (Double.parseDouble(WF)) * IWF;
			 context.write(new Text(finalword), new Text(TFIDF.toString()));
			}
				
						
				
			}
			
		}
	  
	  
	  
	  
	  
  }
  
  
