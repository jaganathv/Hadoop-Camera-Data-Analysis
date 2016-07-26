package Project1;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestCameraByPriceAndResolution
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
			
	        Job job = Job.getInstance(conf, "BestCameraByPriceAndResolution");
	        job.setJarByClass(BestCameraByPriceAndResolution.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(BestCameraByPriceAndResolutionMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(DoublePair.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    job.setSortComparatorClass(BestCameraByPriceAndResSortComp.class);
		    job.setGroupingComparatorClass(BestCameraByPriceAndResGroupComp.class);
		    job.setPartitionerClass(BestCameraByPriceAndResPtnr.class);
		    
 		    job.setNumReduceTasks(1);
	 	    job.setReducerClass(BestCameraByPriceAndResolutionReducer.class); //Reducer Not needed for this computation 
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class BestCameraByPriceAndResolutionMapper extends Mapper<LongWritable, Text, DoublePair, Text>
{
		
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	  
	 		 
	 if(!line[0].equals("Model") && !line[0].equals("STRING") && !line[0].equals("") &&
	    !line[1].equals("Release date") && !line[1].equals("DOUBLE") && !line[1].equals("") && 
		!line[2].equals("Max resolution") && !line[2].equals("DOUBLE") && !line[2].equals("") &&
		!line[4].equals("Effective pixels") && !line[4].equals("DOUBLE") && !line[4].equals("") && 
		!line[11].equals("Dimensions") && !line[11].equals("DOUBLE") && !line[11].equals("") &&
		!line[12].equals("Price") && !line[12].equals("DOUBLE") && !line[12].equals(""))
	    {
			
			int release_date=Integer.parseInt(line[1].trim());
			Double price=Double.parseDouble(line[12].trim());
			if((release_date>=2005 && release_date<=2007) &&
			  (price>=800.00) && (price<=1500.00))
			  {
				  //Storing Max_Resolution, Dimensions, Model, Release_Date
				  String value_map=line[2].trim()+";"+line[11].trim()+";"+line[0].trim()+";"+line[1].trim();
				  Double pixels = Double.parseDouble(line[4].trim());
				  context.write(new DoublePair(new Double(price),new Double(pixels)), new Text(value_map));
			  }	
			   
        }
  }   
} 


class BestCameraByPriceAndResolutionReducer extends Reducer <DoublePair, Text, Text, Text> 
{   private boolean header_written=false;
		
    public void reduce(DoublePair key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {         
    	if(!header_written)
    	{
    		//WRITE HEADER ONCE
    		context.write(new Text("Price||  Pixel || Maximum Resolution||  Dimensions|| Camera-Model ||Year-Released"), new Text(""));
    		header_written=true;
    	}
		Double max_resolution = 0.0;
		Double max_dimension=0.0;
		//
		String val_reducer="";
		 
		 for(Text val:values)
	       {    
	    	
	    	    String pixel_red = Double.toString(key.getSecond().get());
	    	    String val_red[] = val.toString().split(";");
			    Double val_max_res = Double.parseDouble(val_red[0].trim());
			    Double val_max_dim = Double.parseDouble(val_red[1].trim());
				
				if(val_max_res>max_resolution) 
				{
				    //Double val_max_dim=Double.parseDouble(val_red[1].trim());
				    val_reducer=pixel_red+"||"+val_max_res+"||"+val_max_dim+"||"+val_red[2].trim()+"||"+val_red[3].trim();
					max_resolution=val_max_res;
					max_dimension=val_max_dim;
				}
				else if(val_max_res==max_resolution)
				{        
	                                				
					
					
					if(val_max_dim>max_dimension)
					{
					
						val_reducer=pixel_red+"||"+max_resolution+"||"+val_max_dim+"||"+val_red[2].trim()+"||"+val_red[3].trim();
						max_dimension=val_max_dim;
					}
						
						
					
				}		
						   
			   	   
	       }
		 
       
       
       String price_red=Double.toString(key.getFirst().get());       
	   context.write(new Text(price_red+"||"), new Text(val_reducer));
	   //CODE TO LEAVE A BLANK LINE
	   context.write(new Text(""), new Text(""));
	   
	}  
            
}
    