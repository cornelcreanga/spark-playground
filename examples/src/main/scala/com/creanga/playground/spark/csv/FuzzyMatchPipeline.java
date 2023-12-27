package com.creanga.playground.spark.csv;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.size;

public class FuzzyMatchPipeline<K> {

    Pipeline pipeline;
    Dataset<K> first;
    Dataset<K> second;

    public FuzzyMatchPipeline(Dataset<K> first, Dataset<K> second) {
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("name")
                .setOutputCol("tokens");
        StopWordsRemover stopWordsRemover = new StopWordsRemover()
                .setStopWords(new String[]{"inc.","ltd","limited","incorporated","llc","limited","|"})
                .setInputCol("tokens")
                .setOutputCol("stop");
        SQLTransformer sqlTransformer = new SQLTransformer()
                .setStatement("SELECT *, concat_ws(' ', stop) concat FROM __THIS__");
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("concat")
                .setPattern("")
                .setOutputCol("char")
                .setMinTokenLength(1);
        NGram nGram = new NGram()
                .setN(2)
                .setInputCol("char")
                .setOutputCol("ngram");

        HashingTF hashingTF = new HashingTF()
                .setInputCol("ngram")
                .setOutputCol("vector");

        MinHashLSH minHashLSH = new MinHashLSH()
                .setInputCol("vector")
                .setOutputCol("lsh")
                .setNumHashTables(3);

        pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, stopWordsRemover, sqlTransformer, regexTokenizer, nGram, hashingTF, minHashLSH});
        this.first = first;
        this.second = second;
    }

    public Dataset<Row> join(){
        PipelineModel model = pipeline.fit(first);
        Dataset<Row> firstTransformed = model.transform(first);
        firstTransformed = firstTransformed.filter(size(firstTransformed.col("ngram")).gt(0));

        Dataset<Row> secondTransformed = model.transform(second);
        secondTransformed = secondTransformed.filter(size(secondTransformed.col("ngram")).gt(0));

        Transformer[] transformer = model.stages();
        MinHashLSHModel minHashLSHModel= ((MinHashLSHModel)transformer[transformer.length-1]);

        return minHashLSHModel.approxSimilarityJoin(firstTransformed, secondTransformed, 0.5, "jaccardDist").toDF();
    }
}
