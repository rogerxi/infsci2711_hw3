package edu.pitt.sis.infsci2711.recommendfriend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RecommendFriend extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new RecommendFriend(),
				args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "RecommendFriend");
		job.setJarByClass(RecommendFriend.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FriendCountWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, FriendCountWritable> {

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			String user = line[0];
			if (line.length == 2) {
				for (String friend : line[1].trim().split(",")) {
					if (friend.isEmpty())
						continue;
					word.set(user);
					context.write(word,
							new FriendCountWritable(Long.valueOf(friend), -1L));
					for (String friend2 : line[1].trim().split(",")) {
						if (!friend.equals(friend2)) {
							if (friend2.isEmpty())
								continue;
							word.set(friend);
							context.write(
									word,
									new FriendCountWritable(Long
											.valueOf(friend2), 1L));
						}
					}
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, FriendCountWritable, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<FriendCountWritable> values,
				Context context) throws IOException, InterruptedException {
			String recommendations = "";
			HashMap<Long, Long> recommendFriends = new HashMap<Long, Long>();
			for (FriendCountWritable val : values) {
				if (recommendFriends.containsKey(val.user)) {
					if (recommendFriends.get(val.user) == -1L) {
						continue;
					} else {
						recommendFriends.put(val.user,
								recommendFriends.get(val.user) + val.count);
					}
				} else {
					recommendFriends.put(val.user, val.count);
				}
			}
			ValueComparator bvc = new ValueComparator(recommendFriends);
			TreeMap<Long, Long> sortedMap = new TreeMap<Long, Long>(bvc);
			sortedMap.putAll(recommendFriends);
			int i = 0;
			System.out.println(sortedMap);
			for (java.util.Map.Entry<Long, Long> entry : sortedMap.entrySet()) {
				Long key_data = entry.getKey();
				Long value = entry.getValue();
				if (value == -1L)
					break;
				if (recommendations == "")
					recommendations = String.valueOf(key_data);
				else
					recommendations = recommendations + ","
							+ String.valueOf(key_data);
				i = i + 1;
				if (i >= 10)
					break;
			}
			context.write(key, new Text(recommendations));
		}
	}

	static public class FriendCountWritable implements Writable {
		public Long user;
		public Long count;

		public FriendCountWritable() {
			this(-1L, -1L);
		}

		public FriendCountWritable(Long user, Long count) {
			this.user = user;
			this.count = count;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(user);
			out.writeLong(count);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			user = in.readLong();
			count = in.readLong();
		}

		@Override
		public String toString() {
			return String.valueOf(user) + " " + String.valueOf(count);
		}
	}

	static class ValueComparator implements Comparator<Long> {
		java.util.Map<Long, Long> base;

		public ValueComparator(java.util.Map<Long, Long> base) {
			this.base = base;
		}

		public int compare(Long a, Long b) {
			if (base.get(a) > base.get(b)) {
				return -1;
			} else if (base.get(a) == base.get(b)) {
				if (a < b)
					return -1;
				else
					return 1;
			} else {
				return 1;
			}
		}
	}
}