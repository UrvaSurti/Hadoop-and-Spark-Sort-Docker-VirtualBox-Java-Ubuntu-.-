import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkSort {

    final int ZERO = 0;
    final int ONE = 1;

    public static void main(String[] args) {
       
		SparkConf sc = new SparkConf().setAppName("Spark Sort");
		JavaSparkContext sparkContext = new JavaSparkContext(sc);
		String input = args[ZERO];
        String output = args[ONE];
        JavaRDD<String> textFile = sparkContext.textFile(input);
		
        PairFunction<String, String, String> keyValuePairs =
               new PairFunction<String, String, String>() {
                    public Tuple2<String, String > call(String x) throws Exception{
                        return new Tuple2(x.substring(ZERO,10), x.substring(11,98)); 
                    }
                };
				

        JavaPairRDD<String, String> pairs = textFile.mapToPair(keyValuePairs).sortByKey(true);
        pairs.map(x -> x._1 + " " + x._2 + "\r").coalesce(ONE).saveAsTextFile(output); 
    }
}