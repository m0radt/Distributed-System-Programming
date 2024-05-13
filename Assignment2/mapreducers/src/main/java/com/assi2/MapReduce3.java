package com.assi2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

  
public class MapReduce3 { 
 
public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
	long N ;
	double zero = 0.0;
	@Override
    public void setup(Context context)  throws IOException, InterruptedException {
		N=Long.valueOf(context.getConfiguration().get("N"));
    }
	public static double Pdel(double Nr0, double Nr1, double Tr01,double Tr10, double N) {
		double res = 0.0;
		try {
			double p1 = Tr01 + Tr10;
			double p2 = Nr0 + Nr1;
			double m1 = N*p2;
			res= p1 / m1;
			if (Double.isInfinite(res) || Double.isNaN(res))
				return 0.0;
			return res;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;

	}
    


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        String[] splits = value.toString().split("\t");
        String w1 = splits[0].split(",")[0]; 
		String w2 = splits[0].split(",")[1];
        String w3 = splits[0].split(",")[2];
        Double Nr0 = Double.valueOf(splits[1].split(",")[0]) ;
        Double Nr1 = Double.valueOf(splits[1].split(",")[1]);
        Double Tr01 = Double.valueOf(splits[1].split(",")[2]);
        Double Tr10 = Double.valueOf(splits[1].split(",")[3]);
		context.write(new Text(w1 + " " + w2 ), new Text(Pdel(Nr0, Nr1, Tr01,Tr10, N)+"@"+ w3));
    }
    
  }
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {



	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] tmpKey = key.toString().split(" ");
			String w1 = tmpKey[0];
			String w2 = tmpKey[1];
			//
			List<myPair> list = new ArrayList<myPair>();

			for(Text value : values){
				String[] tmpVal = value.toString().split("@");
				Double res = Double.valueOf(tmpVal[0]);
				String w3 = tmpVal[1];
				list.add(new myPair(res,w3));
			}
			list.sort(new Comparator<myPair>() {// reverse sort
				@Override
				public int compare(myPair p1, myPair p2) {
					return p2.getFirst().compareTo(p1.getFirst());
				}
			});
			for(myPair p : list){
				context.write(new Text(w1 + " " + w2 + " " + p.getSecond()), new Text(String.valueOf(p.getFirst())));
			}
	    }
	    
	 }
 
  public static class CombinerClass 
     extends Reducer<Text,Text,Text,Text> {

	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            
        }
	    
	 }

  public static class PartitionerClass extends Partitioner<Text,Text> {
	  
	  @Override
      public int getPartition(Text key, Text count, int numReducers) {
        int tmpres = key.hashCode() % numReducers;
        if(tmpres < 0) {
        	return -1 * tmpres;
        }
        return tmpres;
      }
    
    }
}