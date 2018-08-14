package common;

import com.kevin.examples.model.WC;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author Kevin.Zhang
 * @description
 * @date 2018-08-14 18:22
 */
public class Tokenizer implements FlatMapFunction<String, WC> {
    @Override
    public void flatMap(String value, Collector<WC> out) {
        // normalize and split the line
        String[] tokens = value.toLowerCase().split("\\W+");

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new WC(token, 1));
            }
        }
    }
}
