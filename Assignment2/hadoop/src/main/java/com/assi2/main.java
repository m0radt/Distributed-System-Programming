package com.assi2;
import com.amazonaws.auth.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.regions.Regions;


import java.io.File;
import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;


public class main 
{
    private static String accessKey;
	private static String secretKey;
	private static String sessionToken;
	
	public static void loadCredentials(String path){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            line = reader.readLine();
            accessKey = line.split("=", 2)[1];
            line = reader.readLine();
            secretKey = line.split("=", 2)[1];
            line = reader.readLine();
            if (line!=null) 
            	sessionToken = line.split("=", 2)[1];
            reader.close();
        }
        catch (IOException e)
        {
            System.out.println(e);
        }

    }
    public static void main(String[] args)
    {
        String bucketName = "assi2buckett";
        loadCredentials(System.getProperty("user.home") + File.separator + ".aws" + File.separator + "credentials");

        AWSCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClient.builder()
        .withRegion(Regions.US_EAST_1)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
        .withJar("s3n://"+bucketName+"/Assi2Jar/mapreducers-1.0-SNAPSHOT.jar") // This should be a full map reduce application.
        .withMainClass("/com/assi2/MapReduceMain.class")
        
        //.withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data/", "s3n://"+bucketName+"/outputheb/");
        .withArgs("s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data/", "s3n://"+bucketName+"/output/");
        StepConfig stepConfig = new StepConfig()
        .withName("mapreducers")
        .withHadoopJarStep(hadoopJarStep)
        .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
        .withInstanceCount(9)
        .withMasterInstanceType(InstanceType.M4Large.toString())
        .withSlaveInstanceType(InstanceType.M4Large.toString())
        .withHadoopVersion("3.3.4").withEc2KeyName("vockey")
        .withKeepJobFlowAliveWhenNoSteps(false)
        .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
        .withName("Google Bigrams collocation extract")
        .withInstances(instances)
        .withSteps(stepConfig)
        .withLogUri("s3n://"+bucketName+"/logs/")
        .withServiceRole("EMR_DefaultRole")
        .withJobFlowRole("EMR_EC2_DefaultRole").withReleaseLabel("emr-5.0.0");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}

