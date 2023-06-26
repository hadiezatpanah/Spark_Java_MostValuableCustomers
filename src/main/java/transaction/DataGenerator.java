package transaction;

import org.apache.spark.sql.execution.streaming.MemoryStream;
import scala.collection.JavaConverters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DataGenerator implements Runnable {
    private final MemoryStream<String> memStream;
    private final Thread dataGenerationThread;

    public DataGenerator(MemoryStream<String> memStream) {
        this.memStream = memStream;
        this.dataGenerationThread = new Thread(this);
    }

    public void startGeneratingData() {
        dataGenerationThread.start();
    }

    @Override
    public void run() {
        Random rand = new Random();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.now();

        while (true) {

            String customerId = "Customer" + rand.nextInt(10);
            String productId = "Product" + rand.nextInt(5);
            double amount = rand.nextDouble() * 100;

            time = time.plusSeconds(1);
            String transactionTime = time.format(formatter);

            String data = String.format("%s,%s,%.2f,%s", customerId, productId, amount, transactionTime);
            memStream.addData(JavaConverters.asScalaBuffer(Collections.singletonList(data)).toSeq());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}

