package com.haozhuo.bigdata.dataetl.ansj;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import java.io.Serializable;
import java.util.List;

public class AnsjUtils implements Serializable {
    public static String tokensWithoutNature(String str, String split) {
        return ToAnalysis.parse(str).recognition(StopWords.getInstance()).toStringWithOutNature(split);
    }
    public static List<Term> tokens(String str) {
        return ToAnalysis.parse(str).recognition(StopWords.getInstance()).getTerms();
    }

    public static String tokensWithoutNature(String str) {
        return tokensWithoutNature(str, ",");
    }
}
