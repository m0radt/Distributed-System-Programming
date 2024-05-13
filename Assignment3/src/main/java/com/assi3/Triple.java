package com.assi3;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



    public class Triple implements WritableComparable<Triple> {
        public String first;
        public String second;
        public String third;
        Triple(String first,String second, String third)
        {
            this.first=first;
            this.second=second;
            this.third=third;
        }

        Triple(){
            this.first="";
            this.second="";
            this.third="";

        }

        @Override
        public String toString() {
            return first +"/"+ second +"/"+ third;
        }

        public int compareTo(Triple other) {
            int x=this.first.compareTo(other.first);
            int y=this.second.compareTo(other.second);
            int z=this.third.compareTo(other.third);


            if(x!=0)
            {
                return x;
            }
            if(y!=0)
            {
                return y;
            }
            if (z!=0)
            {
                return z;
            }
            return 0;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(first);
            dataOutput.writeUTF(second);
            dataOutput.writeUTF(third);

        }

        public void readFields(DataInput dataInput) throws IOException {
            this.first=dataInput.readUTF();
            this.second=dataInput.readUTF();
            this.third=dataInput.readUTF();
        }
    }

