package com.assi2;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


import java.util.Arrays;
import java.util.HashSet;

  
public class MapReduce1 { 
 
public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {

	String[] symbols = { "\"", "!", "#", "*", "+", ",", "`", "/","\\", "-", "@" , " " ,"(",")",":",";","%",".","&","'","{","}","[","]","$","#","_","0","1","2","3","4","5","6","7","8","9",">","<","?","~","^"};
              
    HashSet<String> stopWords = new HashSet<>(Arrays.asList(new String[] { "a", "about", "above", "across", "after", "afterwards", "again", "against", "all",
    "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst",
    "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway",
    "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes",
    "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between",
    "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer",
    "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during",
    "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even",
    "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
    "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front",
    "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
    "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how",
    "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its",
    "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
    "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
    "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
    "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
    "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own",
    "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
    "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty",
    "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such",
    "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence",
    "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
    "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together",
    "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon",
    "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever",
    "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which",
    "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
    "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves" }));
    


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        try {
            String[] splits = value.toString().split("\t");
            if (splits[0].split(" ").length == 3) {
                String w1 = splits[0].split(" ")[0]; 
                String w2 = splits[0].split(" ")[1];
                String w3 = splits[0].split(" ")[2];
                String count = splits[2];
                boolean containsSymbol = false;
                boolean containsbanWords = false;

                for (String symbol : symbols) {
                    if (w1.contains(symbol) || w2.contains(symbol) || w3.contains(symbol)) {
                        containsSymbol = true;
                        break;
                    }
                }
                for (String banWord : stopWords) {
                    if (w1.equalsIgnoreCase(banWord) || w2.equalsIgnoreCase(banWord)|| w3.equalsIgnoreCase(banWord)) {
                        containsbanWords = true;
                        break;
                    }
                }
                if(!containsSymbol && !containsbanWords ){
                    int groupNum = (int) ( Math.random() * 2 + 1); // will return either 1 or 2
                    context.write(new Text( w1 + "," + w2 + "," + w3+ "," + groupNum), new Text(count)); 
                    context.write(new Text( w1 + "," + w2 + "," + w3+ "," + "*"), new Text(count)); 
                    context.getCounter("NCounter", "N").increment(Integer.parseInt(count));	//inc N counter

                }

            }
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
  }
 
  public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        int total = 0;


	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int sum =0 ;
            if (key.toString().contains("*")) {	//<w1,w2,w3,*>
                total = 0;
                for (Text val : values)
                    total = total + Integer.parseInt(val.toString());

            } else {							//<w1,w2,w3,groupNum>
                for (Text value : values){
                    sum = sum + Integer.parseInt(value.toString());
                }  
                context.write(key, new Text(String.valueOf(sum + "," + total)));
            }
            
	    }
	    
	 }
 
  public static class CombinerClass 
     extends Reducer<Text,Text,Text,Text> {


	    @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int sum =0 ;
            for (Text value : values) {
                sum = sum + Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
	    }
	    

	 }

  public static class PartitionerClass extends Partitioner<Text,Text> {
	  
      @Override
      public int getPartition(Text key, Text count, int numReducers) {
        String [] split = key.toString().split(",");
        Text tmp = new Text(split[0] +","+ split[1] +","+ split[2]);
        int tmpres = tmp.hashCode() % numReducers;
        if(tmpres < 0) {
        	return -1 * tmpres;
        }
        return tmpres;

      }
    
    }
}