package com.kevin.examples.model;

/**
 * @author Kevin.Zhang
 * @description
 * @date 2018-08-14 18:20
 */
public class WC {
    public String word;
    public long frequency;

    // public constructor to make it a Flink POJO
    public WC() {}

    public WC(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WC " + word + " " + frequency;
    }
}
