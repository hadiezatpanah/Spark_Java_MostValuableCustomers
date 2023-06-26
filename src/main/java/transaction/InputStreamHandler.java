package transaction;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;


public class InputStreamHandler {
    private MemoryStream<String> inputMemStreamDF;

    public InputStreamHandler(SparkSession sparkSession) {
        inputMemStreamDF = new MemoryStream<String>(42, sparkSession.sqlContext(), null, Encoders.STRING());
        DataGenerator dataGenerator = new DataGenerator(inputMemStreamDF);
        dataGenerator.startGeneratingData();
    }


    public MemoryStream<String> getInputMemStreamDF() {
        return inputMemStreamDF;
    }

}
