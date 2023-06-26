package transaction;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.*;
import static  org.apache.spark.sql.functions.*;



public final class MostValueableCustomers {
    public static void main(String[] args) throws Exception {

        String filepath = "src/main/resources/application.conf";
        String env = "dev";
        ConfigHandler configHandler = new ConfigHandler(filepath, env);
        Config config = configHandler.getConfig();

        SparkSession spark = new SparkSessionHandler(config.getConfig("SparkSession")).getSparkSession();
        InputStreamHandler inputStreamHandler = new InputStreamHandler(spark);

        MemoryStream<String> inputMemStreamDF = inputStreamHandler.getInputMemStreamDF();
        Dataset<Row> dataFrameWithSchema = new ApplySchema(inputMemStreamDF).getDFWithScheme();


        Dataset<Row> aggregatedWindowedDF = new WindowHandler(dataFrameWithSchema).getAggregatedWindowedDF();

        StreamingQuery query = aggregatedWindowedDF
                .writeStream()
                .trigger(Trigger.ProcessingTime("2 minutes"))
                .outputMode(OutputMode.Append())
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) -> {
                    batchDF.groupBy(
                                col("window"),
                                col("customerId")
                            )
                            .agg(sum(col("amount")).as("totalAmount"))
                            .withColumn("rank", row_number().over(Window.partitionBy(col("window.start"), col("window.end")).orderBy(desc("totalAmount"))))
                            .filter(col("rank").leq(3))
                            .select(col("window.start").as("windowStart"), col("window.end").as("windowEnd"), col("customerId"), col("totalAmount"))
                            .show();
                })
                .start();

        query.awaitTermination();

    }

}