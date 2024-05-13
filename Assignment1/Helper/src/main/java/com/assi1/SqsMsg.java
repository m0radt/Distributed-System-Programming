package com.assi1;
import com.google.gson.Gson;

public class SqsMsg {
    public String replyQueue;
    public String body;
    public Boolean terminate;
    public int workerMessageRatio ;
    public SqsMsg(String replyQueue, String body, Boolean terminate)
    {
        this.replyQueue = replyQueue;
        this.terminate=terminate;
        this.body=body;
    }
    public SqsMsg(String replyQueue, String body, int workerMessageRatio,Boolean terminate)
    {
        this.replyQueue = replyQueue;
        this.terminate=terminate;
        this.body=body;
        this.workerMessageRatio = workerMessageRatio;
    }
    public SqsMsg(String replyQueue, String body)
    {
        this.replyQueue = replyQueue;
        this.body=body;
        this.terminate =false;
    }
    public SqsMsg(String body)
    {
        this.replyQueue = "";
        this.body=body;
        this.terminate =false;
    }

    public String toString()
    {
        return new Gson().toJson(this);
    }

    
}
