package Project1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BestCameraByPriceAndResPtnr extends Partitioner<DoublePair, Text> {
	  
    @Override
    public int getPartition(DoublePair key, Text val, int numPartitions) 
    {
    	Double key1 = key.getFirst().get();
    	System.out.println("PRINTING FROM PARTITIONER");
    	
    	//System.out.println();
    	
    	int hash1 = key1.intValue() & Integer.MAX_VALUE;
    	System.out.println("Key:"+ key1.intValue());
    	System.out.println("hash_value:"+hash1);
    	//System.out.println();       		   	
        int partition = hash1 % numPartitions;
        System.out.println("Partition_Number:"+partition);
        return partition;
    }
}

