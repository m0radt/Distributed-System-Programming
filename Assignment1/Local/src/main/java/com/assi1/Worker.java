package com.assi1;


import com.google.gson.Gson;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;


public class Worker {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Worker");
        Ec2Utils.instanceCredintials();
		SqsUtils.instanceCredintials();
		S3Utils.instanceCredintials();
        Thread visibilityThread = null;
        heartbeat heartbeat = null;
        
        while (true) {
            System.out.println("Worker checking for incoming messages from ");
            
            List<Message> messages = SqsUtils.receiveMessages(Names.manager2WorkersQueue, 1);
            if (messages != null && messages.size() > 0) {
                try {
                    heartbeat = new heartbeat(messages.get(0), Names.manager2WorkersQueue);
                    visibilityThread = new Thread(heartbeat);
                    visibilityThread.start();
                    
                    //construct the received message
                    SqsMsg msg = new Gson().fromJson(messages.get(0).body(), SqsMsg.class);
                    String url = msg.body;
                    System.out.println("worker received the following url : " + url);
                    
                    //apply ocr on the image url
                    System.out.println("apply OCR on the image");
                    String ocrRes;
                    String ocrTestRes = testImage(url);
                    //System.out.println("the image is valid : "+ ocrTestRes);
                    if(ocrTestRes.equals("true")){
                        ocrRes = url + " " + applyOCR(url);
                    }
                    else{
                        ocrRes = url + " " + ocrTestRes;
                    }

                    
                    //send the results back to manager
                    System.out.println("OCR result is :\n" + ocrRes + "\n");
                    SqsMsg toSendBack = new SqsMsg(ocrRes); 
                    
                    SqsUtils.sendMessage(toSendBack, SqsUtils.getQueueUrl(msg.replyQueue));
                    SqsUtils.deleteMessage(messages.get(0), Names.manager2WorkersQueue);
                    heartbeat.terminate = true;
                } catch (Exception e) {
                    System.out.println("Something went wrong in Worker: " + e);
                    e.printStackTrace();
                    if (visibilityThread != null)
                        visibilityThread.interrupt();
                }
            }else {
                Thread.sleep(10000);
            }
        }
    }
    

public static String applyOCR(String imageURL) throws IOException, TesseractException {
    String result = "";
    URL url = new URL(imageURL);
    BufferedImage img = ImageIO.read(url);
    Tesseract instance = new Tesseract();
    String tessdataFolder ="/tessdata";
    instance.setDatapath(tessdataFolder);
    result = instance.doOCR(img);

    return result;
}
public static String testImage(String url){  
    try {  
        BufferedImage image = ImageIO.read(new URL(url));  
        //BufferedImage image = ImageIO.read(new URL("http://someimage.jpg"));  
        if(image != null){  
            return "true";
        } else{
            return " no registered ImageReader claims to be able to read the stream";
        }

    } catch (MalformedURLException e) {  
        return "URL error with image";  
        
    } catch (IOException e) {  
        return "IO error with image";  
    }  
}

 }