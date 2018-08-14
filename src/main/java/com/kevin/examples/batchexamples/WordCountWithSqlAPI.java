package com.kevin.examples.batchexamples;

import com.kevin.examples.model.WC;
import common.Tokenizer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * @author Kevin.Zhang
 * @description
 * @date 2018-08-14 17:07
 */
public class WordCountWithSqlAPI {
    public static void main(String[] args) throws Exception {
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<String> text = env.readTextFile("hdfs://192.168.207.254:9000/data/a.txt");

        DataSet<WC> input = text.flatMap(new Tokenizer());

        // register the DataSet as table "WordCount"
        tEnv.registerDataSet("WordCount", input, "word, frequency");

        // run a SQL query on the Table and retrieve the result as a new Table
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

        DataSet<WC> result = tEnv.toDataSet(table, WC.class);

        result.print();
    }
}

