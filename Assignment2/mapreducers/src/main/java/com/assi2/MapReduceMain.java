package com.assi2;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MapReduceMain 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        String output1 = args[2] + "MapReduce1Output";
        Configuration conf1 = new Configuration();
        System.out.println("Configuring MapReducer 1");
        
        Job job = Job.getInstance(conf1, "MapReduce1");
        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class,
        MapReduce1.MapperClass.class);
        job.setJarByClass(MapReduce1.class);
        job.setMapperClass(MapReduce1.MapperClass.class);
        job.setPartitionerClass(MapReduce1.PartitionerClass.class);
        job.setCombinerClass(MapReduce1.CombinerClass.class);
        job.setReducerClass(MapReduce1.ReducerClass.class);
        job.setNumReduceTasks(32);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(output1));
        System.out.println("Launching MapReducer 1");
        if (job.waitForCompletion(true)) {
            System.out.println("MapReducer 1 finished");
        } else {
            System.out.println("MapReducer 1 failed ");
        }
        Configuration conf3 = new Configuration();
        CounterGroup jobCounters;
        jobCounters = job.getCounters().getGroup("NCounter");
        for (Counter counter : jobCounters){
        	//System.out.println("Passing " + counter.getName() + " with value " + counter.getValue() + " to MapReducer 2");
            conf3.set("N", "" + counter.getValue());
        }

        System.out.println();
        String output2 = args[2] + "MapReduce2Output";
        System.out.println("output2 = " + output2);
        Configuration conf2 = new Configuration();
        
            
        System.out.println("Configuring MapReducer 2");
        Job job2 = Job.getInstance(conf2, "MapReduce2");
        job2.setJarByClass(MapReduce2.class);
        job2.setMapperClass(MapReduce2.MapperClass.class);
        job2.setPartitionerClass(MapReduce2.PartitionerClass.class);
        job2.setReducerClass(MapReduce2.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(32);
        FileInputFormat.setInputPaths(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.out.println("Launching MapReducer 2");
        if (job2.waitForCompletion(true)) {
            System.out.println("MapReducer 2 finished");
        } else {
            System.out.println("MapReducer 2 failed ");
        }
        
        System.out.println();
        String output3 = args[2] + "MapReduce3Output";
        System.out.println("Configuring MapReducer 3");

        Job job3 = Job.getInstance(conf3, "MapReduce3");
        job3.setJarByClass(MapReduce3.class);
        job3.setMapperClass(MapReduce3.MapperClass.class);
        job3.setReducerClass(MapReduce3.ReducerClass.class);
        job3.setPartitionerClass(MapReduce3.PartitionerClass.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setNumReduceTasks(32);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(output2));
        FileOutputFormat.setOutputPath(job3, new Path(output3));
        System.out.println("Launching MapReducer 3");
        job3.waitForCompletion(true);
        System.out.println("All MapReducers are done");
    }
}
