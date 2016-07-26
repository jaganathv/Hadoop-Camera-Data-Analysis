package Project1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class BestCameraByPriceAndResSortComp extends WritableComparator
{
	protected BestCameraByPriceAndResSortComp() 
	{
        super(DoublePair.class,true);
    }  
	
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) 
    {
        DoublePair k1 = (DoublePair)w1;
        DoublePair k2 = (DoublePair)w2;        
                         
        Double val1 = k1.getFirst().get();
        
        System.out.println("PRINTING FROM SORTING COMPARATOR");
        System.out.println("val1:"+val1);
        Double val2 = k2.getFirst().get();
        System.out.println("val2:"+val2);
        Double val3 = k1.getSecond().get();
        System.out.println("val3:"+val3);
        Double val4 = k2.getSecond().get();
        System.out.println("val4:"+val4);
       // Perform grouping only based on Mean
        int result = Double.compare(val1, val2);//val1.compareTo(val2);
        System.out.println("RETURN CD FROM COMPARISON 1:"+result);
        if(0==result)
        {
        	result=Double.compare(val3,val4);//.compareTo(val4);
        }
         System.out.println("RETURN CD FROM COMPARISON 2:"+result);
		return result;
    }

}
