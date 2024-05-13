package com.assi1;

import java.io.IOException;

import java.lang.Thread;  

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.sqs.model.Message;

public class Local {
	

	  public static void main(String[] args) throws IOException {
			S3Utils.localCredintials();
			Ec2Utils.localCredintials();
			SqsUtils.localCredintials();

			String  local2ManagerBucket  = Names.local2ManagerBucket + System.currentTimeMillis();

			final String manager2LocalReplayQueue = Names.manager2LocalQueue+ System.currentTimeMillis();

		    String JarPath = Paths.get(System.getProperty("user.dir"))  + File.separator;
		    File file = new File(args[0]);
		    Path path = file.toPath();
			Boolean terminate = false;

			
			//   check terminate
			if (args.length == 4 && args[3].equalsIgnoreCase("terminate")) {
				terminate = true;
			}

			if (!S3Utils.checkIfBucketExistsAndHasAccessToIt(Names.jarBucket)) 
            	S3Utils.createBucket(Names.jarBucket);

			//First we start by uploading the MOST UPDATED jar file for the Manager/Worker to run
			System.out.println("Uploading assignment JAR"); 
			S3Utils.putObjectPublic(Paths.get(JarPath + "Local-1.0-SNAPSHOT-jar-with-dependencies.jar"), "Assi1Jar",Names.jarBucket); // Upload Jar to S3
			System.out.println("Upload Done!");
        

			//Uploads the input file to S3
			System.out.println("Uploading the file you gave to S3"); 
		    S3Utils.createBucket(local2ManagerBucket );
		    S3Utils.putObjectPublic(path, "inputFile", local2ManagerBucket );
			System.out.println("Upload Done!"); 
 



			// Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node.
			Ec2Utils.startEc2IfNotExist("Manager", Scripts.initManager);

			//Sends a message to an SQS queue, stating the location of the file on S3
			SqsUtils.createSqsQueue(manager2LocalReplayQueue);
			if(!SqsUtils.isSqsOpened(Names.local2ManagerQueue)){
				SqsUtils.createSqsQueue(Names.local2ManagerQueue);
			}

			
			SqsMsg locationMsg = new SqsMsg(manager2LocalReplayQueue, local2ManagerBucket,Integer.parseInt(args[2]), terminate);

			System.out.println("sending message to manger"); 
			SqsUtils.sendMessage(locationMsg, SqsUtils.getQueueUrl(Names.local2ManagerQueue));
			System.out.println("we sent message to manger through " + Names.local2ManagerQueue + " queue" ); 

			//Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.

		    FileWriter fw;
            ResponseInputStream workersOutputStream = null;
       		Writer writer;
			BufferedReader reader = null;
			System.out.println("now we are waiting for output ...");
			while (true) {
				try {
					List<Message> messages = SqsUtils.receiveMessages(manager2LocalReplayQueue);
					if (messages.size() != 0) {
						System.out.println("we are reading the output fileF");

						SqsUtils.deleteMessage(messages.get(0), manager2LocalReplayQueue);
						workersOutputStream = S3Utils.getObjectS3("outputFile.txt", local2ManagerBucket);
						
						reader = new BufferedReader(new InputStreamReader(workersOutputStream));
						fw = new FileWriter(args[1]);
						writer = new BufferedWriter(fw);
						writer.write("<!DOCTYPE html>\n");
						writer.write("<html>\n");
						writer.write("<head>\n<title>OCR</title>\n</head>\n");
						writer.write("<body>\n");
						String Line;
						String[] splits = {};
						String str = "";
						while ((Line = readTillNullTerminate(reader)) != null) {
							splits = Line.split(" ",2);	
							str = splits[0] + "\n" + splits[1];
							writer.write("<p>" + str + "</p>");
							writer.write('\n');
						}
						writer.write("</body>\n");
						writer.write("</html>");
						writer.close();
						
						//delete our queue & buckets to not waste resources
						SqsUtils.deleteQueue(manager2LocalReplayQueue);
						S3Utils.deleteBucket(local2ManagerBucket);
						return;
					}
					Thread.sleep(10000);
				} catch (Exception ignored) {
					;
				}
			}
				

		}
		public static String readTillNullTerminate(BufferedReader buff) throws IOException{
			int c = buff.read();
			// if EOF
			if (c == -1){
				return null;
			}
			StringBuilder builder = new StringBuilder("");
			// Check if new line or EOF
			while (c != -1 && c != 0){
				builder.append((char) c);
				c = buff.read();
			}
			return builder.toString();
		}

}
