//DISHA KARUNAKAR SHETTY
//dshetty1@uncc.edu

package training;

import java.io.IOException;
//import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.File;
import org.apache.hadoop.conf.Configuration;

public class TFIDF extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TFIDF.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TermFrequency(), args);
        int res = ToolRunner.run(new TFIDF(), args); //initializing the run function after getting instance of the class
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        int temp = new File(args[0]).listFiles().length;
        conf.setInt("n", temp);
        Job job = Job.getInstance(conf, " Job"); //creating job
        job.setJarByClass(this.getClass()); //creating jar
        FileInputFormat.addInputPaths(job, args[1]); //input file path in HDFS for TFIDF
        FileOutputFormat.setOutputPath(job, new Path(args[2])); //output file path in HDFS
        job.setMapperClass(Map.class); //initialization of mapper class to execute map function
        job.setReducerClass(Reduce.class); //initialization of reducer class to execute map function
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class); //text object to output key i.e the word being counted(key-value pair)
        job.setOutputValueClass(DoubleWritable.class);  // value represents number of times that word appears(key-value pair)
        return job.waitForCompletion(true) ? 0 : 1; //launching the job and waiting for it to finish- if true returns progress
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable offset, Text lineText, Context context) //invokes map method once for each key value pair from your input source
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int set = conf.getInt("n", 0);
            conf.setInt("n", set);
            context.getInputSplit();
            String eachLine = lineText.toString();
            String[] split_delimiter = eachLine.split("#####");
            String[] split_tab = split_delimiter[1].split("\t");
            context.write(new Text(split_delimiter[0]), new Text(split_tab[0]
                    + "=" + split_tab[1]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> { 

        public void reduce( Text word,  Iterable<Text> file,  Context context)
             throws IOException,  InterruptedException {
              String fileList="";
           // concatenating all file names for a single word with delimeter ','
              for(Text list:file){
                  if(fileList.equals(""))
                	  fileList=fileList+list.toString();
                  else
                	  fileList=fileList+","+list.toString();
              }
       int counter=0;
       // getting number of files from configuration object
              Configuration conf=context.getConfiguration();
              int n=conf.getInt("n",0);
              
              //using counter to count number of files for corresponding word
              for(int i=0;i<fileList.toString().length();i++){ 
                  if(fileList.charAt(i)==','){
                      counter++;
                  }
              }
              counter=counter+1;double tfidf=0;
              //performing TFIDF calculations
              if(counter>1){  
                  String[] arr2=fileList.toString().split(",");
                  for(int j=0;j<arr2.length;j++){
                      String[] arr1=arr2[j].split("=");
                      tfidf=Math.log10(1+(n/arr2.length))*Double.parseDouble(arr1[1]);DoubleWritable DoubleForm=new DoubleWritable(tfidf);
                      String new_word=word+"#####"+arr1[0];Text textForm=new Text(new_word);
                      context.write(textForm,DoubleForm);}
                  }
                  else{
                     
                      String[] arr=fileList.toString().split("=");
                      tfidf=Math.log10(1+(n))*Double.parseDouble(arr[1]);DoubleWritable x=new DoubleWritable(tfidf);
                      String new_word=word+"#####"+arr[0];Text textForm=new Text(new_word);
                      context.write(textForm,x);
                  }
              }
           
           
           
             
          }
    }