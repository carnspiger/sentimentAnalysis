package com.hadoop.cye;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


/**
 * Created by caitlin.ye on 5/7/17.
 * BigData - Hadoop IntelliJ Maven - pay to see
 * dependencies? target: .gitignore, Data.iml, pom.xml, sentiment-visualization.tar
 */
public class sentimentAnalysis {


    public static Map<String, String> emotionLibrary = new HashMap<>();

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("dictionary", args[2]); //why 2???

        Job job = Job.getInstance(configuration);
        job.setJarByClass(Map.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //export
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //ensures finishes job
        job.waitForCompletion(true);

    }

    //Object, Text (1st two, key-val) is input ,   last two output (val-key)
    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void setup(Context context) throws IOException { //context puts data into hdfs, results in intermediate
            //initialize mapper
            Configuration configuration = context.getConfiguration();   //setup heap size
            String dicName = configuration.get("dictionary", ""); // getting emotion category, dictionary name of text file

            BufferedReader br = new BufferedReader(new FileReader(dicName));
            String line = br.readLine();

            while (line != null) {
                String[] word_feeling = line.split("\\t");
                emotionLibrary.put(word_feeling[0], word_feeling[1]);
                line = br.readLine();
            }
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //read some data -> value
            //split into words
            //look up in sentimentLibrary -> done
            //write out result key-value -> done

            String[] words = value.toString().split("//s+"); //regex: ?
            for (String word : words) {
                //word is emotion category library, lookup using HashMap
                if (emotionLibrary.containsKey(word.trim().toLowerCase())) {
                    //where to write to -> hard disk
                    //intermediate context
                    context.write(new Text(emotionLibrary.get(word.toLowerCase().trim())), new IntWritable(1)); //value = how many times appears   what???
                }
            }
        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //combine data from mapper -> done
            //sum count for each sentiment (pos, neg, neutral)
            //positive
            //value = <1,1,1,1,........>
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));

        }
    }
}


//mac run Auto Complete: hadoop-jar
//sentiment analysis project on intelliJ, article sentiment analysis ie: PerfectPositiveThinking
//TODO - finish creating run method: https://github.com/kite-sdk/kite/wiki/Hadoop-MapReduce-Tutorial


