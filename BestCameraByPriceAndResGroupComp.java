package Project1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BestCameraByPriceAndResGroupComp extends WritableComparator {
	protected BestCameraByPriceAndResGroupComp() 
    {
        super(DoublePair.class,true);
    }   
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) 
    {
        DoublePair k1 = (DoublePair)w1;
        DoublePair k2 = (DoublePair)w2;
        System.out.println("PRINTING FROM GROUPING COMPARATOR");
        
        Double val1 = k1.getFirst().get();
        System.out.println("val1:"+val1);
        Double val2 = k2.getFirst().get();
        System.out.println("val2:"+val2);
      
        int result = Double.compare(val1,val2); //.compareTo(val2);
        System.out.println("RESULT OF COMPARISON:"+result);
         
		return result;
       }	        
    }


