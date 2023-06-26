package transaction;

import com.typesafe.config.Config;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.types.StructType;
import static  org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class ApplySchema {
    Dataset<Row> DFWithScheme;

    public ApplySchema(MemoryStream inputMemStreamDF) {
        DFWithScheme = inputMemStreamDF.toDF()
                .selectExpr(
                        "cast(split(value,',')[0] as string) as customerId",
                        "cast(split(value,',')[1] as string) as productId",
                        "cast(split(value,',')[2] as string) as amount",
                        "to_timestamp(cast(split(value,',')[3] as string), 'yyyy-MM-dd HH:mm:ss') as transactionTime"
                );
    }

    public Dataset<Row> getDFWithScheme() {
        return DFWithScheme;
    }
}
