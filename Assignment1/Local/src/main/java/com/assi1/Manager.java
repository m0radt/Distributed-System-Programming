package com.assi1;

import com.google.gson.Gson;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {

	   static boolean terminate = false;

	    public static void main(String[] args) throws FileNotFoundException {
	    	System.out.println("Starting Manager");
			Ec2Utils.instanceCredintials();
			SqsUtils.instanceCredintials();
			S3Utils.instanceCredintials();

	    	if(!SqsUtils.isSqsOpened(Names.manager2WorkersQueue)){
				SqsUtils.createSqsQueue(Names.manager2WorkersQueue);
			}
	        
	        ExecutorService pool = Executors.newFixedThreadPool(4);
	        
        	Ec2Utils.startEc2s("Worker", Scripts.initWorker, 1);
	        System.out.println("Manager Created 1st Worker");
	        
	        while(true){
	            List<Message> inbox;
	            if (terminate) {
	                pool.shutdown();
	                while (!pool.isTerminated()) {
	                    try {
							Thread.sleep(6000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
	                }
	                
	                //delete all resources
	                SqsUtils.deleteQueue(Names.local2ManagerQueue);
	                SqsUtils.deleteQueue(Names.manager2WorkersQueue);
	                Ec2Utils.terminateAll("Worker");
	                Ec2Utils.terminateAll("Manager");
					S3Utils.deleteBucketObjects(Names.jarBucket);
					S3Utils.deleteBucket(Names.jarBucket);
	                break;
	            }

				
	            inbox = SqsUtils.receiveMessages(Names.local2ManagerQueue,1);
	            if (inbox != null && inbox.size() != 0) {
					System.out.println("recieved message from local");
	                SqsMsg sqsmessage = new Gson().fromJson(inbox.get(0).body(), SqsMsg.class);
	                terminate = sqsmessage.terminate;
	                
	                ManagerTask task = new ManagerTask(sqsmessage);
	                SqsUtils.deleteMessage(inbox.get(0), Names.local2ManagerQueue); 
	                pool.execute(task);
	            }
	        }
	    }

}
