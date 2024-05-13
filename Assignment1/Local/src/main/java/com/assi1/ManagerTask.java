package com.assi1;

import com.google.gson.Gson;
import software.amazon.awssdk.services.sqs.model.Message;


import java.util.List;


public class ManagerTask implements Runnable {
	public volatile int currWorkersNum = 1;
	public final int MaxWorkers = 18;
	final SqsMsg message;
	final String worker2ManagerQueue = Names.worker2ManagerQueue + System.currentTimeMillis();

	public ManagerTask(SqsMsg msg) {
		this.message = msg;
	}

	public void sendWork(String url) {
		SqsMsg msg = new SqsMsg(worker2ManagerQueue, url);
		SqsUtils.sendMessage(msg, SqsUtils.getQueueUrl(Names.manager2WorkersQueue));
	}
    private synchronized void MoreWorkers(int workersRequir) {
		if (workersRequir > MaxWorkers) {
			if ((MaxWorkers - currWorkersNum) > 0) {
				Ec2Utils.startEc2s("Worker", Scripts.initWorker, (MaxWorkers - currWorkersNum));
				currWorkersNum = MaxWorkers;
			}
		} else if (workersRequir > currWorkersNum) {
			Ec2Utils.startEc2s("Worker", Scripts.initWorker, workersRequir - currWorkersNum);
			currWorkersNum = workersRequir;
		}
		return;
	}

	public void run() {
		String result = "";
		S3Utils.instanceCredintials();
		
		System.out.println("bucket name is :   "+ message.body);
		String InputFromLocal = S3Utils.getObject("inputFile", message.body);
		String[] urls = InputFromLocal.split("\n");
		int urldNum = urls.length;
		if(!SqsUtils.isSqsOpened(worker2ManagerQueue)){
			SqsUtils.createSqsQueue(worker2ManagerQueue);
		}

		for (String url : urls) {
			sendWork(url);
			System.out.println("Processing ==> " + url);
		}

		int workersNum = ((int) Math.round((urldNum/1.0) / message.workerMessageRatio));
		if (workersNum > 1) {
			MoreWorkers(workersNum);
		}

		int finishedJobs = 0;
		System.out.println(finishedJobs);
		SqsMsg tempMsg;
		System.out.println("waiting for messages from workers...");


		while (finishedJobs < urldNum) {
			List<Message> messages = SqsUtils.receiveMessages(worker2ManagerQueue);
			if (messages.size() != 0) {
				for (Message msg : messages) {
					System.out.println("Manager receieved message: " + msg.body());
					tempMsg = new Gson().fromJson(msg.body(), SqsMsg.class);
					result = result + tempMsg.body;
					result = result + "\0";


					finishedJobs++;
					System.out.println(finishedJobs);
					SqsUtils.deleteMessage(msg,worker2ManagerQueue);
				}
			}
			
			if (messages.size() == 0) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Jop is finished");
		System.out.println("n");


		
		System.out.println("File sent to bucket: " + message.body);
		System.out.println("uploading the results : " );
		System.out.println(result);
		System.out.println("to : " +  message.body);
		
		System.out.println("uploading finished");
		S3Utils.putObject(result, "outputFile.txt", message.body);

		System.out.println("Manager finished a task");
		SqsMsg doneMsg = new SqsMsg("finished");
		SqsUtils.sendMessage(doneMsg, SqsUtils.getQueueUrl(message.replyQueue));
		SqsUtils.deleteQueue(worker2ManagerQueue);
	}
	
}