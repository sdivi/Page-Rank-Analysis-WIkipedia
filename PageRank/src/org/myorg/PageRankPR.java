package org.myorg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRankPR {

	// WRITE output directory name
	public static final String OUTPUT = "output";

	// cleanupmapper : reads the output file after 10 iterations

	public static class CleanUpMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] keyTitleRank = line.split("\001");
				String[] filenameRank = keyTitleRank[0].split("\\|\\|");
				DoubleWritable prank = new DoubleWritable((Double.parseDouble(filenameRank[1])));
				// pagrank values are given as key and filenames as value
				context.write(prank, new Text(filenameRank[0]));
			} catch (Exception e) {
				System.out.println("Exception : " + e);
			}
		}
	}

	public static class CleanUpReducer extends Reducer<DoubleWritable, Text, Text, Text> {
		private static int counter = 100;
		int j = 10;
		String out = OUTPUT;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());

			// deletes all intermediate files
			while (j >= 0) {
				fs.delete(new Path(out + j), true);
				j--;
			}

			super.cleanup(context);
		}

		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// reads only top 10 pagerank pages
			for (Text t : values) {
				if (counter > 0) {
					counter--;
					System.out.println("counter : " + counter);
					context.write(new Text(key + ""), t);
				}
			}

		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// reading xml file using SAXbuilder
			// "||" is delimiter used to seperate filename and pagerank
			// "\001" is delimiter used to seperate (filename,pagerank) and list
			// of files
			// "\\~" is delimiter used to seperate each file in the list

			StringBuilder sb = new StringBuilder();
			String xmlstring = value.toString();

			String doctitle = null;
			String textinter = null;
			String text = null;
			if (!xmlstring.isEmpty()) {
				System.out.println("inside the loop");
				Pattern titleregex = Pattern.compile("<title>(.*?)</title>");
				Matcher titlem = titleregex.matcher(xmlstring);
				while (titlem.find()) {
					doctitle = titlem.group(1).trim();
					System.out.println(doctitle);
				}
				Pattern revregex = Pattern.compile("<revision>(.*?)</revision>");
				Matcher revm = revregex.matcher(xmlstring);
				Pattern textregex = Pattern.compile("<text(.*?)>(.*?)</text>");
				while (revm.find()) {
					System.out.println("inside the loop revm");
					System.out.println(revm.group(1));
					textinter = revm.group(1).trim();
				}

				Matcher textm = textregex.matcher(textinter);
				while (textm.find()) {
					System.out.println("inside the loop test");
					text = textm.group(2);
					System.out.println(text);
				}
				if (text != null) {
					Pattern p = Pattern.compile("\\[\\[(.*?)\\]\\]");
					Matcher m = p.matcher(text); // get a matcher object

					while (m.find()) {

						sb.append(m.group(1) + "\\~");

					}
				}
				context.write(new Text(doctitle + "||" + "0.15" + "\001"), new Text(sb.toString()));

			}

		}

	}

	public static class PageRankPRMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// reads output file from tokenizermapper
			if (value.toString() != null || value.toString().isEmpty() == false) {
				String[] keyTitle = value.toString().split("\001");
				// System.out.println(" title "+keyTitle[1]);

				String[] filePageRank = keyTitle[0].split("\\|\\|");
				System.out.println(" keyTitle  " + keyTitle[1]);
				System.out.println("running.......");
				if (keyTitle[1].trim().length() > 0) {

					StringTokenizer tokenizer = new StringTokenizer(keyTitle[1], "\\~");

					int count = tokenizer.countTokens();
					while (tokenizer.hasMoreTokens()) {
						// emits pagerank for each file in the list
						double prank = (Double.parseDouble(filePageRank[1]) / new Double(count));

						word.set(tokenizer.nextToken().trim());
						if (prank != 0.0) {
							context.write(word, new Text(prank + ""));
						}
					}

				}

				context.write(new Text(filePageRank[0]), new Text(keyTitle[1]));

			}
		}
	}

	public static class PageRankPRReducer extends Reducer<Text, Text, Text, Text> {
		public static final double DAMPING_FACTOR = 0.85;

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			double pagerank = 0.00;

			try {
				for (Text val : values) {
					// checks if the value is list or pageranks
					if (val.toString().contains("\\~") && val.toString().length() > 1) {

						sb.append(val.toString());
						System.out.println(sb.toString());

					} else {
						if (val.toString().length() > 1)
							pagerank += Double.parseDouble(val.toString().trim());
					}
				}

				pagerank = 1 - DAMPING_FACTOR + (DAMPING_FACTOR * pagerank);
				context.write(new Text(key + "||" + pagerank + "\001"), new Text(sb.toString().trim()));

			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(IntWritable.class);
		}

		// comparator for descending order of pagerank

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

			return v1.compareTo(v2) * (-1);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		int i = 0;
		// String output = "out";
		String output = OUTPUT;

		// reading xml file job

		Job job = new Job(conf, "WikiMR");
		job.setJarByClass(PageRankPR.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true));

		// calculating pagerank in 10 iterations

		while (i < 10) {
			Job job1 = new Job(conf, "PageRankPR");
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			job1.setMapperClass(PageRankPRMapper.class);
			job1.setReducerClass(PageRankPRReducer.class);
			job1.setJarByClass(PageRankPR.class);
			FileInputFormat.setInputPaths(job1, new Path(output + i));
			FileOutputFormat.setOutputPath(job1, new Path(output + (i + 1)));
			i++;
			System.out.println(job1.waitForCompletion(true));

		}

		// sorting and cleanup process

		Job job2 = new Job(conf, "Cleanup");
		job2.setMapOutputValueClass(Text.class);
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setSortComparatorClass(IntComparator.class);
		job2.setJarByClass(PageRankPR.class);
		FileInputFormat.setInputPaths(job2, new Path(output + i));
		FileOutputFormat.setOutputPath(job2, new Path(output + (i + 1)));
		job2.setMapperClass(CleanUpMapper.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(CleanUpReducer.class);
		System.out.println(job2.waitForCompletion(true));

	}
}
