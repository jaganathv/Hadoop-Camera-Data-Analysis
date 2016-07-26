package Project1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreqCamerasWithinDPIRange
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
			
	        Job job = Job.getInstance(conf, "FreqCamerasWithinDPIRange");
	        job.setJarByClass(FreqCamerasWithinDPIRange.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(FreqCamerasWithinDPIRangeMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		                	    
 		    job.setNumReduceTasks(0);
 		    
	 	    //job.setReducerClass(FreqCamerasWithinDPIRangeReducer.class); //Reducer Not needed for this computation 
	 	    
	 	    //job.setOutputKeyClass(Text.class);
		   // job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class FreqCamerasWithinDPIRangeMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	
	private int count_of_cameras = 0;
	
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	  
	  		 
	 if(!line[2].equals("Max resolution") && !line[2].equals("DOUBLE") && !line[2].equals("") &&
	     !line[12].equals("Price") && !line[12].equals("DOUBLE") && !line[12].equals(""))
  		 
	    {
			Double Price = Double.parseDouble(line[12].trim());
			Double resolution = Double.parseDouble(line[2].trim());
			
			int ret = Double.compare(Price,1000.00);
			if(ret>0)
			{
				if(resolution>=1600.00 && resolution<=3000.00)
				{
					++count_of_cameras;
				}
			}
			   
        }
  }
  
   public void cleanup(Context context) throws IOException, InterruptedException
   {	
       context.write(new Text("COUNT OF CAMERAS WITH MAXIMUM DPI RANGE BETWEEN 1600 & 3000 COSTING MORE THAN $ 1000"), new Text(""));
	   String header1="\t\t\t";
	   String header2=Integer.toString(count_of_cameras);
       context.write(new Text(header1), new Text(header2));	   
   }
 
} 

    