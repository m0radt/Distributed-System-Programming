package com.assi3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

//first step is to find the path
public class step1 {
    public static enum UpdateCount {
        Patterns
    }



    public static class MapperClass extends Mapper<LongWritable, Text, Triple, Text> {
        Stemmer stemmer ;
        @Override
        public void setup(Context context) throws IOException {
            this.stemmer = new Stemmer();
        }


        // helper methods
        public static void collect_nodes(ArrayList<TreeNode<WordData>> TreeNode_lst, String[] nodes) {
            for (int i = 0; i < nodes.length; i++) {
                String[] node = nodes[i].split("/"); //cease/VB/ccomp/0
                try {
                    String word = node[0]; //cease
                    String type = node[1]; //VB
                    int parentid = Integer.valueOf(node[3]); //ccomp
                    TreeNode_lst.add(i, new TreeNode<WordData>(new WordData(word, type, parentid, i)));
                } catch (Exception e) {
                    return;
                }
            }
        }

        public static void add_parent(ArrayList<TreeNode<WordData>> TreeNode_lst, String[] nodes) {
            for (int i = 0; i < nodes.length; i++) {
                TreeNode<WordData> tmp = TreeNode_lst.get(i);
                if (tmp.data.parentid > 0) {
                    tmp.addParent(TreeNode_lst.get(tmp.data.parentid - 1));
                }
            }
        }

        public static int[] count_root_nouns(ArrayList<TreeNode<WordData>> TreeNode_lst, String[] nodes, ArrayList<Integer> nouns_indexs) {
            int noun_counter = 0;
            int root_counter = 0;
            int root_index = -1;
            for (int i = 0; i < nodes.length; i++) {
                if (TreeNode_lst.get(i).parent == null) {
                    root_counter++;
                    root_index = i;
                }
                if (TreeNode_lst.get(i).data.is_noun()) {
                    nouns_indexs.add(i);
                    noun_counter++;
                }
            }
            return new int[]{noun_counter, root_counter, root_index};
        }

        public static List<Pair<Integer, Integer>> create_pairs(ArrayList<Integer> nouns_indexs) {
            List<Pair<Integer, Integer>> nounspairs = new ArrayList<Pair<Integer, Integer>>();
            for (int i = 0; i < nouns_indexs.size(); i++) {
                for (int j = i + 1; j < nouns_indexs.size(); j++) {
                    if (i != j) {
                        nounspairs.add(new Pair<Integer, Integer>(nouns_indexs.get(i), nouns_indexs.get(j)));
                    }
                }
            }
            return nounspairs;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) {
            try {
                // head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
                // counts_by_year: year<comma>count
                String regex = "^[a-zA-Z\\s]+$";
                Pattern pattern = Pattern.compile(regex);
                String[] line = value.toString().split("\t");
                String[] nodes = line[1].split("\\s+"); // '\\s+' multiple whitespace
                // extract nodes of syntatic- ngrams ->
                // nodes: cease/VB/ccomp/0  for/IN/prep/1  some/DT/det/4  time/NN/pobj/2

                ArrayList<TreeNode<WordData>> TreeNode_lst = new ArrayList<TreeNode<WordData>>();
                ArrayList<Integer> nouns_indexs = new ArrayList<Integer>();

                collect_nodes(TreeNode_lst, nodes); // add the nodes to the tree
                add_parent(TreeNode_lst, nodes); // add parents to treenode

                int[] res = count_root_nouns(TreeNode_lst, nodes, nouns_indexs);
                int noun_counter = res[0];

                int root_index = res[2];

                List<Pair<Integer, Integer>> nounspairs = create_pairs(nouns_indexs);


                if (TreeNode_lst.get(root_index).data.is_verb() && noun_counter >= 2) {
                    for (Pair<Integer, Integer> p : nounspairs) {
                        // init visited
                        for (int i = 0; i < nodes.length; i++) {
                            TreeNode_lst.get(i).visited = false;
                        }
                        Integer key1 = p.key;
                        Integer key2 = p.value;
                        TreeNode<WordData> goal = TreeNode_lst.get(key2);
                        TreeNode<WordData> start = TreeNode_lst.get(key1);
                        String path = searchPath(start, goal, null, false, root_index);
                        if (path != null) {
                            String path1 = "X " + path.trim() + " Y"; //delete right and left white spaces.
                            String path2 = "Y " + path.trim() + " X";

                            if (pattern.matcher(start.data.word).matches() && pattern.matcher(goal.data.word).matches()
                                    && pattern.matcher(path).matches()) {
                                String stemmedStartData = stemmer.stem(start.data.toString());
                                String stemmedGoalData = stemmer.stem(goal.data.toString());
                                context.write(new Triple(stemmedStartData,stemmedGoalData,path1),new Text(line[2])); // write the path it with the count
                                context.write(new Triple(stemmedGoalData,stemmedStartData,path2),new Text(line[2]));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("exception = " + value);
                e.printStackTrace();
            }
        }

        // searching path
        public static String searchPath(TreeNode<WordData> start, TreeNode<WordData> goal, String acc, boolean head, int headid) {
            if (start == null || start.visited) {
                return null;
            }
            start.visited = true;
            if (start.data.id == goal.data.id) {
                if (head) {
                    return acc;
                } else {
                    return null;
                }
            }
            if (start.data.id == headid) {
                head = true;
            }
            if (!head) {
                if (acc == null) {
                    return searchPath(start.parent, goal, "", false, headid);
                } else {
                    return searchPath(start.parent, goal, acc + " " + start.data.word, false, headid);
                }
            } else {
                for (TreeNode<WordData> n : start.children) {
                    String helper;
                    if (acc == null) {
                        helper = searchPath(n, goal, "", true, headid);
                    } else {
                        helper = searchPath(n, goal, acc + " " + start.data.word, true, headid);
                    }
                    if (helper != null) {
                        return helper;
                    }
                }
            }
            return null;
        }
    }
    public static class CombinerClass 
    extends Reducer<Triple,Text,Triple,Text> {


       @Override
       public void reduce(Triple key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            int counter = 0;
            for (Text value : values) {
                counter += Long.valueOf(value.toString());
            }
            context.write(key, new Text(String.valueOf(counter)));
        }
       

    }

    public static class PartitionerClass extends Partitioner<Triple, Text> {
        // ensure that keys with same key are directed to the same reducer
        @Override

        public int getPartition(Triple key, Text v, int numPartitions) {
            return Math.abs(key.first.hashCode() + key.second.hashCode()) % numPartitions;
        }
    }



    public static class ReducerClass extends Reducer<Triple, Text, Text, Text> {
        @Override
        public void reduce(Triple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int counter = 0;
            for (Text value : values) {
                counter += Long.valueOf(value.toString());
            }
            context.write(new Text(key.toString()), new Text(String.valueOf(counter)));
        }
    }



     public static void main(String[] args) throws Exception {
        String output1 = args[2] + "Step1Output";
        Configuration conf1 = new Configuration();
        System.out.println("Configuring Step 1");

        Job job = Job.getInstance(conf1, "Step1");
        MultipleInputs.addInputPath(job, new Path(args[1] + "biarcs0.txt"), TextInputFormat.class);
        //MultipleInputs.addInputPath(job, new Path(args[1] + "biarcs1.txt"), TextInputFormat.class);
        //MultipleInputs.addInputPath(job, new Path(args[1] + "biarcs2.txt"), TextInputFormat.class);
        //MultipleInputs.addInputPath(job, new Path(args[1] + "biarcs3.txt"), TextInputFormat.class);
        //MultipleInputs.addInputPath(job, new Path(args[1] + "biarcs4.txt"), TextInputFormat.class);

        job.setJarByClass(step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(32);
        job.setMapOutputKeyClass(Triple.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(output1));
        System.out.println("Launching Step 1");
        if (job.waitForCompletion(true)) {
            System.out.println("Step 1 finished");
        } else {
            System.out.println("Step 1 failed ");
        }

        System.out.println();
        String output2 = args[2] + "Step2Output";
        System.out.println("output2 = " + output2);
        Configuration conf2 = new Configuration();
        conf2.set("NPmin", args[3]);
        conf2.set("accessKey", args[4]);
        conf2.set("secretKey", args[5]);
        conf2.set("sessionToken", args[6]);
        

        System.out.println("Configuring Step 2");
        Job job2 = Job.getInstance(conf2, "Step2");
        job2.setJarByClass(step2.class);
        job2.setMapperClass(step2.MapperClass.class);
        job2.setPartitionerClass(step2.PartitionerClass.class);
        job2.setReducerClass(step2.ReducerClass.class);
        job2.setMapOutputKeyClass(NounsPair.class);
        job2.setMapOutputValueClass(Triple.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.out.println("Launching Step 2");
        if (job2.waitForCompletion(true)) {
            System.out.println("Step 2 finished");
        } else {
            System.out.println("Step 2 failed ");
        }
        Counters counters = job2.getCounters();
        long totalpatterns = counters.findCounter(UpdateCount.Patterns).getValue();
        System.out.println("Displaying just the value " + totalpatterns);
        
        System.out.println();
        String output3 = args[2] + "Step3Output";
        System.out.println("output3 = " + output3);
        Configuration conf3 = new Configuration();
        conf3.set("pattern", String.valueOf(totalpatterns));
        System.out.println("Configuring Step 3");
        Job job3 = Job.getInstance(conf3, "Step3");
        job3.setJarByClass(step3.class);
        job3.setMapperClass(step3.MapperClass.class);
        job3.setPartitionerClass(step3.PartitionerClass.class);
        job3.setReducerClass(step3.ReducerClass.class);
        job3.setMapOutputKeyClass(Triple.class);
        job3.setMapOutputValueClass(Pair2.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job3, new Path(output2));
        FileOutputFormat.setOutputPath(job3, new Path(output3));
        System.out.println("Launching Step 3");
        if (job3.waitForCompletion(true)) {
            System.out.println("Step 3 finished");
        } else {
            System.out.println("Step 3 failed ");
        }

        
    }
}