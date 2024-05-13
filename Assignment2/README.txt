morad taya 	user: asus
Omer presler       user:omer

Running the project:
	1-  Use cmd and navigate to the 2 projects and run this command for both "mvn clean compile package"
	2-  The jar file "mapreducers-1.0-SNAPSHOT.jar" from "mapreducers" project should be uploaded to your S3 bucket
	3-  Go to your Home directory  create directory ".aws" and
	    create "credentials" file in it and fill with proper credentials
	4-  Run the jar from the "hadoop" project locally on your PC 
	5-  make sure to run the project using java 8 (java 17 doesn't work)

you can access the final output at s3n://assi2buckett/output/MapReduce3Output/
to get the result run the following command "aws s3 cp s3://assi2buckett/output/MapReduce3Output/ <folderName> --recursive --region us-east-1"
 
