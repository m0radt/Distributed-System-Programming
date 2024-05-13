package com.assi1;


public class Scripts{

    static String initInstanceBash =
            		"#!/bin/bash\n" +
                    "aws s3api get-object --bucket " + Names.jarBucket+ " --key Assi1Jar Local-1.0-SNAPSHOT-jar-with-dependencies.jar\n"+
                    "sudo yum install java-1.8.0 -y\n";

                

    static String initManager = initInstanceBash + "java -jar Local-1.0-SNAPSHOT-jar-with-dependencies.jar Manager\n";

    static String initWorker = initInstanceBash + "sudo yum install git -y\n" +
    "git clone https://github.com/tesseract-ocr/tessdata.git\n"+
    "sudo amazon-linux-extras install epel -y\n"+
    "sudo yum install tesseract -y\n"+
    "export LD_LIBRARY_PATH=/usr/local/lib\n"+
    "source ~/.bash_profile \n" + 
    "sudo ldconfig\n"+
    	"java -jar Local-1.0-SNAPSHOT-jar-with-dependencies.jar Worker\n";
}
    

