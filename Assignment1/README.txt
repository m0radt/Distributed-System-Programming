morad taya   Omer presler

Execution Instructions:
	1. Compile the helper then Local projects by running this command: "mvn clean install package"
	2. Go to your Home directory and Create a directory called ".aws" and
		create a  text file called "credentials"
	3. Open "credentials" and paste into it the AWS credentials from ur lab session.
	4. Put the input file in the same directory with the JAR file
	5. From Terminal run: "java -jar yourjar.jar inputFileName outputFileName n [terminate]"



	- AMI: ami-0b0dcb5067f052a63 (Amazon Linux 64bit)
	- Instance type: T2.Micro
	- we used n = 5 in the example we run
	- Execution time: 7:42 min 


code flow :
	1. Local starts Manager instance if not started yet
	2. Local checks/creates the persistant buckets (JAR bucket, Results bucket) if they exists, has access
	3. Local sends SQS message to the Manager notifying it about a new job
	4. Local now awaits for a "finished" message from Manager
	5. Manager creates proper buckets/queues for communications
	6. Manager initaites a proper number of Workers
	7. Manager creates an SQS message for every line in the input & sends to workers
	8. Manager now awaits for the results from workers
	9. Workers try to read messages from proper queues
	10. Workers finds a job, downloads a file, apply OCR on it, uploads results to S3 and notifies Manager about the results
	11. Manager gathers the results for the same Local in one file and uploads it to S3
	12. Manager notifies Local about finishing the job & the location of the results file in S3
	13. Local downloads the results file, extracts the needed info to create the HTML file

Q&A:no i didn't send credentials in plain text, i used IAMInstanceProfile LabInstanceProfile that exist in the lap. i maked sure before any instance run code to provide credintial by using InstanceProfileCredentialsProvider
	-Did you think for more than 2 minutes about security? Do not send your credentials in plain text!
	==>no i didn't send credentials in plain text, i used IAMInstanceProfile LabInstanceProfile that exist in the lap. i maked sure before any instance run code to provide credintial by using 
	   InstanceProfileCredentialsProvider.

	-Did you think about scalability? Will your program work properly when 1 million clients connected at the same time?
	==> Yes it will work, (disregard the 19 instances limit for lab users :P) not much threads in our app, synchronized as it should, no waiting times (other than waiting for results)
		minimal use of buckets/queues, so IMO it should work perfectly too

	-What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system?
	==> We have taken in regards many things: including errors while: dowloading the files, applying Ocr, sending messages, IO exceptions, and we covered all aspects of this matter.
		Plus our Manager regularly checks on the # of Workers to make sure they match the job, so if any node dies it will be replaced, if it dies mid-job, another Worker
		will take over the job instead . and we delete message from local to manager queue after we complete from a job so there is no undone jobs.

	-Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!
	==> Manager uses a threadpool for execution, and the Workers has 2 threads: one applyocr on  image and other is created to ChangeVisibility of the recieved message & finally deletes it.
		I think we choose good places to use Threads in the task.

	-Did you run more than one client at the same time? Be sure they work properly, and finish properly, and your results are correct
	==> Yes we did, both finished, checked the output files also.

	-Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.
	==> Yes of course we did

	-Did you manage the termination process? Be sure all is closed once requested!
	==> Every Class (Manager/Worker/Local) that creates a resource (Instance/Bucket/Queue) was made sure to close it when done.
	
	-Did you take in mind the system limitations that we are using? Be sure to use it to its fullest!
	==> 19 instances limit was taken into account, all Workers are made sure to work to the full potential also.

	-Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!
	==> NO, our manager handles incoming messages from Locals by splitting them into tasks for workers and finally replying to Locals with the results.

	-Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?
	==> Yes we do, other than Locals waiting for results, our system is always busy and we always thrived to use it to the fullest. 
