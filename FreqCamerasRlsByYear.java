package Project1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreqCamerasRlsByYear
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
			
	        Job job = Job.getInstance(conf, "FreqCamerasRlsByYear");
	        job.setJarByClass(FreqCamerasRlsByYear.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(FreqCamerasRlsByYearMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		                	    
 		    job.setNumReduceTasks(1);
 		    
	 	    job.setReducerClass(FreqCamerasRlsByYearReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class FreqCamerasRlsByYearMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	
	private int bad_record_count = 0;
	
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	  
	  		 
	 if(!line[1].equals("Release date") && !line[1].equals("DOUBLE") && !line[1].equals("")) 		 
	    {
			
			int Year = Integer.parseInt(line[1].trim());
			if(Year>=1994 && Year <=2007)
			{
			  String Yr=Integer.toString(Year); //Converting Year to String to store as Text
			  
			  context.write(new Text(Yr),new IntWritable(1));
		    }	   
			else
			{
				bad_record_count++;
			}
   
        }
  }
  
   public void cleanup(Context context) throws IOException, InterruptedException
   {	
       context.write(new Text("INVALID RECORDS COUNT"), new IntWritable(bad_record_count));
	   
   }

   

  
} 

class FreqCamerasRlsByYearReducer extends Reducer <Text,IntWritable,Text,Text> 
{
	public void setup(Context context) throws IOException, InterruptedException
	{
		String header1 = "YEAR "+"\t"+" NO OF CAMERAS";
		String header2 = "";
        context.write(new Text(header1), new Text(header2)); //Used header2 as a filler here
	}
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {        
    
	    int Num_Of_Cameras=0;			 
		 
       for (IntWritable val:values)
       {      	   
    	    Num_Of_Cameras+= val.get();
		   
		   	   
	   }
	   String count=Integer.toString(Num_Of_Cameras);
	   context.write(new Text(key), new Text(count));  
       
  }  
            
}
	   
	
    