package com.assi3;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Counter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;




import com.amazonaws.services.s3.AmazonS3Client;

public class step2 {

    public static class MapperClass extends Mapper<Object, Text, NounsPair, Triple> {
        HashMap<String, String> hashMap ;

        @Override
        public void setup(Context context) throws IOException {
            


            this.hashMap = new HashMap<>();
            Configuration conf = context.getConfiguration();
            String accessKey = conf.get("accessKey");
            String secretKey = conf.get("secretKey");
            String sessionToken = conf.get("sessionToken");
            String bucketName = "assi3bucket";

            AWSCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
            Region region = Region.getRegion(Regions.US_EAST_1);
            AmazonS3 s3Client = new AmazonS3Client(credentials);
            s3Client.setRegion(region);

            
            try {
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, "input/hypernym.txt"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.equals("")) {
                        continue;
                    }
                    String[] splits = line.split("\t"); //split the line
                    NounsPair key = new NounsPair(splits[0],splits[1]);
                    if (!hashMap.containsKey(key.toString())) {
                        if(splits[2].trim().equalsIgnoreCase("True")){
                            hashMap.put(key.toString(),"True");
                        }
                        else if(splits[2].trim().equalsIgnoreCase("False")){
                            hashMap.put(key.toString(),"False");
                        }
                    }
                }
                reader.close();

            }
             catch (IOException e) {
                System.err.println(e.getMessage());
            }

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] splitted = value.toString().split("\t");
                String[] words = splitted[0].split("/");
                NounsPair pair = new NounsPair(words[0],words[1]);
                if (hashMap.containsKey(pair.toString())) {
                    context.write(pair, new Triple(words[2], splitted[1], hashMap.get(pair.toString())));//value:pattern,count,{T,F}
                    context.write(new NounsPair("*",words[2]), new Triple());//*,pattern,

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    public static class PartitionerClass extends Partitioner<NounsPair, Triple> {
        // ensure that keys with same key are directed to the same reducer
        @Override
        public int getPartition(NounsPair key, Triple v, int numPartitions) {
            return Math.abs(key.first.hashCode() + key.second.hashCode()) % numPartitions;
        }
    }


    public static class ReducerClass extends Reducer<NounsPair, Triple, Text, Text> {
        List<String> patternList; 
        Integer NPmin;
        private Counter patternsCounter;

        @Override
        public void setup(Context context) throws IOException {

            
            patternList=new ArrayList<String>();
            NPmin=Integer.valueOf(context.getConfiguration().get("NPmin"));
            patternsCounter = context.getCounter(step1.UpdateCount.Patterns);

        }



        public void reduce(NounsPair key, Iterable<Triple> vs, Context context) throws IOException, InterruptedException {
            LinkedList<String> values = new LinkedList<>();
            int len = 0;
            if(key.first.equals("*") ){
                patternsCounter.increment(1);	//inc pattern counter

            }else{
                for(Triple value : vs){
                    values.add(value.toString());
                    len ++;
                    if(!patternList.contains(value.first)){
                        patternList.add(value.first);
                    }
                    
                    try {
                        context.write(new Text("test@"+key.first + "@"+ key.second+"@"+value.third), new Text(patternList.indexOf(value.first) + "|"+ value.second));
                    } catch (Exception e) {
                        context.write(new Text("error happend with pair <"+key+">"), new Text("Triple <"+value+">"));
                        e.printStackTrace();
                        return;
                    }
                }
                if (len >= NPmin ){ 
                    for (String value: values) {
                        
                        try {
                            String[] splittedValue = value.split("/");
                            context.write(new Text("train@"+key.first + "@"+ key.second+"@"+splittedValue[2]+"@"), new Text(patternList.indexOf(splittedValue[0]) + "|"+ splittedValue[1]));
                        } catch (Exception e) {
                            context.write(new Text("error happend with pair <"+key+">"), new Text());
                            e.printStackTrace();
                            return;
                        }
                    }
                }
            }    
        }
    }
}