package com.kevin.examples.batchexamples;

import com.kevin.examples.model.WC;
import common.Tokenizer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Kevin.Zhang
 * @description
 * @date 2018-08-14 18:02
 */
public class WordCountWithTableAPI {
    public static void main(String[] args) throws Exception {
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        //DataSet<WordCountWithTableAPI.WC> input = getStaticData(env);
        DataSet<WC> input = getHDFSData(env);

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word,frequency.sum as frequency");

        DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);

        result.print();
    }

    private static DataSet<WC> getStaticData(ExecutionEnvironment env){
        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Hello", 1));

        return input;
    }

    private static DataSet<WC> getHDFSData(ExecutionEnvironment env){
        DataSet<String> text = env.readTextFile("hdfs://192.168.207.254:9000/data/a.txt");

        DataSet<WC> input = text.flatMap(new Tokenizer());

        return input;
    }

}
