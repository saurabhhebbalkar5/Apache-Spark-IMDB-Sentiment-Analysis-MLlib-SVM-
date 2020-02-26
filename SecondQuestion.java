package saurabh_assignment3;

/**
 *
 * @author Saurabh
 */
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

public class SecondQuestion {

    public static void main(String[] args) throws Exception {

        //Setting up Spark configuration
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
                .setAppName("SecondQuestion")
                .setMaster("local[4]").set("spark.executor.memory", "1g");

        //Initializing Spark Context
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        //Second Question First Part
        //Reading the data from the IMDB file
        JavaRDD<String> lines = ctx.textFile("F:\\NUIG -AI\\3. Large Scale Data Analytics\\Assignment 3\\sentiment labelled sentences\\imdb_labelled.txt", 1);

        //Hashing transformer to convert terms into vectors
        final HashingTF tf = new HashingTF(10000);

        //First splitting the sentences and its label
        //Second converting the sentences in to vectors
        JavaRDD<LabeledPoint> cleanedData = lines.map((String s) -> {
            String[] breakLine = s.split("\t");
            return new LabeledPoint(Double.parseDouble(breakLine[1]), tf.transform(Arrays.asList(breakLine[0].split(" "))));
        });

        //Splitting the 60% Training Data and 40% Test Data
        JavaRDD<LabeledPoint> trainData = cleanedData.sample(false, 0.6, 11L);
        trainData.cache();
        JavaRDD<LabeledPoint> testData = cleanedData.subtract(trainData);

        //Creating SVM model with training data 
        SVMModel model = SVMWithSGD.train(trainData.rdd(), 1000);

        //Storing the term vectors from test data
        JavaRDD<Vector> td = testData.map(a -> a.features());

        //Predicting the test data and printing the labels
        td.foreach(b -> System.out.println(model.predict(b)));

        //Second Question Second Part
        //Calculating area under ROC / accuracy % 
        JavaPairRDD<Object, Object> predictionAndLabels = testData.mapToPair(p
                -> new Tuple2<>(model.predict(p.features()), p.label()));

        BinaryClassificationMetrics metrics
                = new BinaryClassificationMetrics(predictionAndLabels.rdd());
        System.out.println("\n\nSecond Question B Part: \n"
                + "Area under ROC = " + metrics.areaUnderROC() + "\n\n");

        ctx.stop();
        ctx.close();

    }

}
