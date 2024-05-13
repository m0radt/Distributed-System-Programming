package com.assi2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

  
public class MapReduce2 { 
 
public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
    


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        String[] splits = value.toString().split("\t");
        String w1 = splits[0].split(",")[0]; 
		String w2 = splits[0].split(",")[1];
        String w3 = splits[0].split(",")[2];
        String groupNum = splits[0].split(",")[3]; 
		String countNTotal = splits[1];
        String count = countNTotal.split(",")[0];
        String total = countNTotal.split(",")[1];
        if(groupNum.equals("2") || Integer.parseInt(total) - Integer.parseInt(count) == 0) {
        	context.write(new Text(total+ "," + groupNum), new Text("3gramtotal@" + w1 + "," + w2 + "," + w3)); 
        }
        int rest = Integer.parseInt(total) - Integer.parseInt(count);
        context.write(new Text(count+ "," + groupNum), new Text("count@" + count +"@" +rest)); 

        

    }
    
}
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        Iterable<Text> part1IterableText = null ;
        Text part1keyText = null ;
        int Nr0 ;
        int Nr1 ;
        int b = 0;

	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
	    	b++;
            if(part1IterableText == null && (key.toString().split(",")[1]).equals("1")){//part 1
            	
                part1IterableText = new ArrayList<>();
                for (Text value : values){
                	Text tmp = new Text(value);
                	((ArrayList<Text>) part1IterableText).add(tmp);
                }

                
                part1keyText = key;

            }
            else if (part1IterableText == null && (key.toString().split(",")[1]).equals("2")) {// part 2 there is no part 1
            	List<Text> list = new ArrayList<Text>();
                for (Text value : values){
                	Text tmp = new Text(value);
                	list.add(tmp);
                }
                    Nr0 = 0 ;
                    Nr1 = 0;
                    int Tr01 = 0;
                    int Tr10 = 0;
                    String[] keytmp = key.toString().split(",");
                    int r = Integer.parseInt(keytmp[0]);
                    for (Text value : list){
                        String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("count")) {
                        	int count = Integer.parseInt(valtmp[1]);
                        	int rest = Integer.parseInt(valtmp[2]);
                            Nr1 = Nr1 + count;
                            Tr01 = Tr01 + rest;
                        }
                    }
                    for (Text value : list){
                    	String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("3gramtotal")) {
                        	String threeGram = valtmp[1];
                        	context.write(new Text(threeGram), new Text(Nr0+ "," + Nr1 + "," + Tr01 + "," + Tr10));
                        }
                    	
                    }

                
            } else if(part1IterableText != null && part1keyText.toString().split(",", 2)[0].equals(key.toString().split(",", 2)[0])){// same 
            	List<Text> list = new ArrayList<Text>();
                for (Text value : values){
                	Text tmp = new Text(value);
                	list.add(tmp);
                }
                    Nr0 = 0 ;
                    Nr1 = 0;
                    int Tr01 = 0;
                    int Tr10 = 0;
                    String[] keytmp = key.toString().split(",");
                    int r = Integer.parseInt(keytmp[0]);
                    for (Text value : list){
                        String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("count")) {
                        	int count = Integer.parseInt(valtmp[1]);
                        	int rest = Integer.parseInt(valtmp[2]);
                            Nr1 = Nr1 + count;
                            Tr01 = Tr01 + rest;
                        }
                    }
                    for (Text value : part1IterableText){
                    	String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("count")) {
                        	int count = Integer.parseInt(valtmp[1]);
                        	int rest = Integer.parseInt(valtmp[2]);
                            Nr0 = Nr0 + count;
                            Tr10 = Tr10 + rest;
                        }
                    }
                    for (Text value : list){
                    	String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("3gramtotal")) {
                        	String threeGram = valtmp[1];
                        	context.write(new Text(threeGram), new Text(Nr0+ "," + Nr1 + "," + Tr01 + "," + Tr10));
                        }
                    	
                    }
                    for (Text value : part1IterableText){
                    	String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("3gramtotal")) {
                        	String threeGram = valtmp[1];
                        	context.write(new Text(threeGram), new Text(Nr0+ "," + Nr1 + "," + Tr01 + "," + Tr10));
                        }

                    }
                    part1IterableText = null;
                    part1keyText = null;
                }else{
                	Nr0 = 0 ;
                    Nr1 = 0;
                    int Tr01 = 0;
                    int Tr10 = 0;
                    String[] keytmp = part1keyText.toString().split(",");
                    int r = Integer.parseInt(keytmp[0]);
                    for (Text value : part1IterableText){
                    	String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("count")) {
                        	int count = Integer.parseInt(valtmp[1]);
                        	int rest = Integer.parseInt(valtmp[2]);
                            Nr0 = Nr0 + count;
                            Tr10 = Tr10 + rest;
                        }
                    }         

                    for (Text value : part1IterableText){
                    	String[] valtmp = value.toString().split("@");
                        if(valtmp[0].equals("3gramtotal")) {
                        	String threeGram = valtmp[1];
                        	context.write(new Text(threeGram), new Text(Nr0+ "," + Nr1 + "," + Tr01 + "," + Tr10));
                        }

                    }
                    part1IterableText = null;
                    part1keyText = null;
                    this.reduce(key, values, context);

                }
        }
	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    	if(part1IterableText != null) {
	    		Nr0 = 0 ;
                Nr1 = 0;
                int Tr01 = 0;
                int Tr10 = 0;
                String[] keytmp = part1keyText.toString().split(",");
                int r = Integer.parseInt(keytmp[0]);
                for (Text value : part1IterableText){
                	String[] valtmp = value.toString().split("@");
                    if(valtmp[0].equals("count")) {
                    	int count = Integer.parseInt(valtmp[1]);
                    	int rest = Integer.parseInt(valtmp[2]);
                        Nr0 = Nr0 + count;
                        Tr10 = Tr10 + rest;
                    }
                }         

                for (Text value : part1IterableText){
                	String[] valtmp = value.toString().split("@");
                    if(valtmp[0].equals("3gramtotal")) {
                    	String threeGram = valtmp[1];
                    	context.write(new Text(threeGram), new Text(Nr0+ "," + Nr1 + "," + Tr01 + "," + Tr10));
                    }

                }
                part1IterableText = null;
                part1keyText = null;
	    	}
	    }


                
	}

	    

 
public static class CombinerClass 
     extends Reducer<Text,Text,Text,Text> {

	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {}
	    
	 }

  public static class PartitionerClass extends Partitioner<Text,Text> {
	  
      @Override
      public int getPartition(Text key, Text count, int numReducers) {
        // to make sure every r times 3grams goes to the same reducer
        //return key.toString().split(",")[0].hashCode() % numReducers;
        String [] split = key.toString().split(",");
        int tmpres = split[0].hashCode() % numReducers;
        if(tmpres < 0) {
        	return -1 * tmpres;
        }
        return tmpres;
      }
    
    }
}