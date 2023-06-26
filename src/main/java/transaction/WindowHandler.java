package transaction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static  org.apache.spark.sql.functions.*;

public class WindowHandler {
    private Dataset<Row> aggregatedWindowedDF;
    public WindowHandler (Dataset<Row> inputStream) {
        aggregatedWindowedDF = inputStream
            .withWatermark("transactionTime", "1 minute")
            .select(
                window(col("transactionTime"), "2 minutes"),
                col("customerId"),
                col("amount")
                );
    }

    public Dataset<Row> getAggregatedWindowedDF() {
        return aggregatedWindowedDF;
    }
}
