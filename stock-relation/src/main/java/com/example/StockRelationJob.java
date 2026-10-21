package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockRelationJob {

    // ===== 任务 1 的 Mapper =====
    public static abstract class TaggedMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        protected abstract String tag();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;
            outKey.set(parts[0] + "\t" + parts[1]);
            outValue.set(tag());
            context.write(outKey, outValue);
        }
    }

    public static class AMapper extends TaggedMapper {
        @Override
        protected String tag() { return "A"; }
    }

    public static class BMapper extends TaggedMapper {
        @Override
        protected String tag() { return "B"; }
    }

    // ===== 任务 1 的 Reducer =====
    public static class RelationReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean hasA = false;
            boolean hasB = false;
            for (Text value : values) {
                if ("A".equals(value.toString())) hasA = true;
                if ("B".equals(value.toString())) hasB = true;
            }
            String relation;
            if (hasA && hasB) relation = "BOTH";
            else if (hasA) relation = "A_ONLY";
            else if (hasB) relation = "B_ONLY";
            else return;
            
            outValue.set(relation);
            context.write(key, outValue);
        }
    }

    // ===== 任务 2 的 Mapper =====
    public static class StatMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\\s+");
            if (parts.length < 3) return;
            outKey.set(parts[0]);
            outValue.set(parts[2]);
            context.write(outKey, outValue);
        }
    }

    // ===== 任务 2 的 Reducer =====
    public static class StatReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int both = 0, aOnly = 0, bOnly = 0;
            for (Text value : values) {
                String rel = value.toString();
                if ("BOTH".equals(rel)) both++;
                else if ("A_ONLY".equals(rel)) aOnly++;
                else if ("B_ONLY".equals(rel)) bOnly++;
            }
            int union = both + aOnly + bOnly;
            outValue.set(both + "\t" + aOnly + "\t" + bOnly + "\t" + union);
            context.write(key, outValue);
        }
    }

    // ===== 主函数 =====
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: StockRelationJob <A.txt> <B.txt> <output_task1> <output_task2>");
            System.exit(2);
        }

        Path aPath = new Path(args[0]);
        Path bPath = new Path(args[1]);
        Path out1 = new Path(args[2]);
        Path out2 = new Path(args[3]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out1)) fs.delete(out1, true);
        if (fs.exists(out2)) fs.delete(out2, true);

        // --- Job 1 ---
        Job job1 = Job.getInstance(conf, "task1-stock-relation");
        job1.setJarByClass(StockRelationJob.class);
        MultipleInputs.addInputPath(job1, aPath, TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(job1, bPath, TextInputFormat.class, BMapper.class);
        job1.setReducerClass(RelationReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(1); // 保证全局有序
        FileOutputFormat.setOutputPath(job1, out1);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // --- Job 2 ---
        Job job2 = Job.getInstance(conf, "task2-stock-stat");
        job2.setJarByClass(StockRelationJob.class);
        job2.setMapperClass(StatMapper.class);
        job2.setReducerClass(StatReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1); // 保证全局有序
        FileInputFormat.addInputPath(job2, out1);
        FileOutputFormat.setOutputPath(job2, out2);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

