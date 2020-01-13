package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TopicSaver2Test {

    private static List<String> buff;

    @BeforeAll
    static void init() {
        buff = new ArrayList<>(5);
        buff.addAll(Arrays.asList(
                "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-12\"}",
                "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-13\"}",
                "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-14\"}",
                "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-15\"}",
                "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-16\"}"
        ));
    }

    @Test
    void saveToHdfs() {
        SparkSession spark = SparkSession.builder()
                .master("local[*]") //.master("yarn")
                .appName("topic2-saver-app")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd = sparkContext.parallelize(buff);
        Dataset<Row> ds = spark.read().json(rdd);

        ds.show();
        spark.stop();
    }

}
