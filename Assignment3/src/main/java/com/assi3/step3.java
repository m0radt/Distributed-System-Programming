package com.assi3;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import weka.classifiers.Classifier;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.functions.Logistic;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Attribute;
import weka.core.DenseInstance;




public class step3 {
    public static class MapperClass extends Mapper<Object, Text,Triple, Pair2> {



        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] splitted = value.toString().split("\t");
                String[] splitedKey = splitted[0].split("@");
                String[] splitedValue= splitted[1].split("|");
                String word1 = splitedKey[1];
                String word2 = splitedKey[2];
                int patternIndx = Integer.parseInt(splitedValue[0]);
                int count = Integer.parseInt(splitedValue[1]);

                Pair2 pair = new Pair2(patternIndx,count);
                Triple triple;
                if(splitedKey[0].equals("train")){// train
                    triple = new Triple("#", word1 +","+word2, splitedKey[3]);
                    context.write(triple, pair);
                }
                else if(splitedKey[0].equals("test")){// test
                    triple = new Triple("*",word1 +","+word2, splitedKey[3]);
                    context.write(triple, pair);
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    public static class PartitionerClass extends Partitioner<Triple, Pair2> {
        @Override
        public int getPartition(Triple key, Pair2 v, int numPartitions) {
            return Math.abs(key.first.hashCode() + key.second.hashCode()) % numPartitions;
        }
    }


    public static class ReducerClass extends Reducer< Triple, Pair2, Text, Text> {
        int patternsNum;
        boolean trainMode ;
        Classifier classifier ;

        Instances data;

        ArrayList<Attribute> attributes;
        Attribute hypernym;

        @Override
        public void setup(Context context) throws IOException {
           patternsNum = Integer.valueOf(context.getConfiguration().get("pattern"));

            trainMode = true;
            
            classifier = new Logistic();
            // Create attributes for the data

            hypernym = new Attribute("hypernym", Arrays.asList(new String[]{"False", "True"}));
            

            attributes = new ArrayList<>(patternsNum + 1);
            

            for(int i = 1; i <= patternsNum; i++){
                attributes.add(new Attribute("pattern" + Integer.toString(i)));
            }
            attributes.add(hypernym);

           data = new Instances("data", attributes, 0);


        }



        public void reduce(Triple key, Iterable<Pair2> vs, Context context) throws IOException, InterruptedException {
            int patterns[] = new int[patternsNum];
            for(int i = 0; i < patterns.length; i++) {
                patterns[i]=0;
            }
            for (Pair2 value : vs) {
                patterns[value.key]=value.value;
            }
            String[] words = key.second.split(",");
            String word1 = words[0];
            String word2 = words[1];    
            String isHyp = key.third;
            if(key.first.equals("*") && trainMode){
                // train the classifier  
                trainMode = false;
                // Set the class index
                data.setClassIndex(data.numAttributes() - 1);
                // Creating the classifier model

                
                try {
                    // Evaluating the performance of the model
                    Evaluation eval = new Evaluation(data);
        
                    eval.crossValidateModel(classifier, data, 10, new Random(1));
        
                    classifier.buildClassifier(data);
                    context.write(new Text("accurecy : "), new Text(eval.pctCorrect()+""));
                    context.write(new Text("precision :"), new Text("["+eval.precision(0) + ", "+eval.precision(1) + "]"));
                    context.write(new Text("recall :"), new Text("["+eval.recall(0) + ", "+eval.recall(1) + "]"));
                    context.write(new Text("fMeasure :"), new Text("["+eval.fMeasure(0) + ", "+eval.fMeasure(1) + "]"));
        
                } catch (Exception e) {
                    context.write(new Text("*reduce error acquired while training the data"), new Text());
                    e.printStackTrace();
                }


            }

            if (trainMode){
                Instance example = new DenseInstance(patternsNum + 1);
                
                for(int i = 0; i < patternsNum; i++) {
                    example.setValue(attributes.get(i), patterns[i]);
                }
                example.setValue(hypernym, isHyp);
                data.add(example);
            }else{
                Instance instanceToClassify = new DenseInstance(data.numAttributes());
                instanceToClassify.setDataset(data);


                for(int j = 0; j < patternsNum; j++){
                    instanceToClassify.setValue(j,patterns[j]); // Set value of patterns attribute
                }
                instanceToClassify.setValue(patternsNum,isHyp);

                // Classify instance
                try {
                    double predictedClass = classifier.classifyInstance(instanceToClassify);
                    String predictedClassLabel = data.classAttribute().value((int) predictedClass);
                    //context.write(new Text(word1 + ", " + word2), new Text("predicted as: " + predictedClassLabel+  " actual value is: " + isHyp));
                    context.write(new Text(word1 + ", " + word2), new Text("the value that was calculated is: "+isHyp + "the value that was predicted was: " + predictedClassLabel));
                } catch (Exception e) {
                    context.write(new Text("*reduce error acquired while testing the data"), new Text(e.getMessage()));
                    e.printStackTrace();
                }
            }
            
        }
    }

   
}