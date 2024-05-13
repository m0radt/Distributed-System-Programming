package com.assi3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class NounsPair implements WritableComparable<NounsPair> {
    public String first;
    public String second;

    NounsPair(String first,String second)
    {
        this.first = first;
        this.second = second;
    }

    NounsPair(){

    }

    @Override
    public String toString() {
        return first + "," +second;
    }

   public int compareTo(NounsPair other){
    int cmp = this.first.compareTo(other.first);
    if (cmp == 0)
        cmp = this.second.compareTo(other.second);
    return cmp;
   }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeUTF(second);


    }

    public void readFields(DataInput dataInput) throws IOException {
        this.first=dataInput.readUTF();
        this.second=dataInput.readUTF();

    }
}