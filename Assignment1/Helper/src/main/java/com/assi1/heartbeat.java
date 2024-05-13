package com.assi1;

import software.amazon.awssdk.services.sqs.model.Message;

public class heartbeat implements Runnable{
    private Message msg;
    public boolean terminate = false;
    private String queueInUse;

    public heartbeat(Message msg, String queueName) {
        this.queueInUse = queueName;
        this.msg = msg;
    }

    public void run() {
        while (true) {
            if (terminate) {
                SqsUtils.deleteMessage(msg, queueInUse);
                break;
            } else {
            	SqsUtils.changeVisibilityTime(SqsUtils.getQueueUrl(queueInUse), this.msg, 20);
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
}
