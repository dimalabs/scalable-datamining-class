package de.tuberlin.dima.aim3.assignment3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class VectorizeSentencesMapper extends Mapper<LongWritable, Text, IntWritable, SparseVector> {
   //TODO IMPLEMENT ME
}
    