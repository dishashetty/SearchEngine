//DISHA KARUNAKAR SHETTY
//dshetty1@uncc.edu
package training;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( TermFrequency.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new TermFrequency(), args);  //initializing the run function after getting instance of the class

		System .exit(res);
	}



	public int run( String[] args) throws  Exception {
		Job job  = Job .getInstance(getConf(), " wordcount "); //creating job
		job.setJarByClass( this .getClass()); //creating jar

		FileInputFormat.addInputPaths(job, args[0]); //input file path in HDFS

		FileOutputFormat.setOutputPath(job,  new Path(args[ 1])); //output file path in HDFS
		job.setMapperClass( Map .class); //initialization of mapper class to execute map function
		job.setReducerClass( Reduce .class); //initialization of reducer class to execute map function
		job.setOutputKeyClass( Text .class); //text object to output key i.e the word being counted(key-value pair)
		job.setOutputValueClass( DoubleWritable .class); // value represents number of times that word appears(key-value pair)

		return job.waitForCompletion( true)  ? 0 : 1; //launching the job and waiting for it to finish- if true returns progress
	}

	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
		private final static DoubleWritable one  = new DoubleWritable( 1); // defining global variable
		private Text word  = new Text(); // text object to store each word after parsing
		private String fileName = new String(); //converting text object to string
		String delimeter=new String("#####");

		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*"); //creates regular expression pattern to parse each line of input on word boundaries


		public void map( LongWritable offset,  Text lineText,  Context context) //invokes map method once for each key value pair from your input source
				throws  IOException,  InterruptedException {


			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName(); //to get name of the input file which contains the word

			//  currentWord=newText(word+"######"+filename);

			String line  = lineText.toString().toLowerCase(); //converting text object to string
			Text currentWord  = new Text(); //currentWord- variable to capture individual words from input string

			for ( String word  : WORD_BOUNDARY .split(line)) { //regular expression pattern to split line into individual words based on word boundaries

				if (word.isEmpty()) {
					continue;
				}
				String full= word.toString()+delimeter+filename;
				currentWord  = new Text(full);

				context.write(currentWord,one); //Assigning default value 1 to each word

			}
		}
	}

	public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context) //reducer function to give overall count of word from all mappers
				throws IOException,  InterruptedException {
			int sum  = 0;
			double LogResult=0;
			for ( DoubleWritable count : counts) {
				sum  += count.get();


			}
			LogResult=(Double)(1+(Math.log(sum)/Math.log(10))); //calculating term frequency for each word
			context.write(word,  new DoubleWritable(LogResult)); // writing result to reducer context
		}
	}
}



