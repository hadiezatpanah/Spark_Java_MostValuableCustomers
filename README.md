# Gradle Spark Configuration and MemoryStream as Streaming Source

## Context
This Spark Java project serves as a demonstration of Gradle Spark configuration, specifically focusing on utilizing the MemoryStream class as the streaming source. While there are existing use cases on the internet showcasing the use of MemoryStream in Spark using Scala, this project showcases its implementation in Java. The project is about identifying the most valuable customers within each time window of a streaming transaction data. Additionally, the project highlights Spark's watermarking solution, which effectively handles late data, and leverages its windowing capabilities. Through this project, users can gain practical insights into employing MemoryStream and explore its application within a Java-based Spark environment.

## Datasets

The schema of input stream is as below:

| Column                | Data Type    | Description                          |
|-----------------------|--------------|--------------------------------------|
| customerId            | string       | Identifier of the customer            |
| productId             | string       | Identifier of the product             |
| amount                | string       | Transaction amount                    |
| transactionTime       | timestamp    | Timestamp of the transaction          |



## Requirements

The requirement is to identify the top three most valuable customers based on their transaction amounts within specific time windows. The objective is to analyze customer transactions and determine the customers who have made the highest monetary contributions during predefined time intervals.

The solution should include the following key components:

+ Streaming Data Source: The system should ingest streaming data that includes customer information, product details, transaction amounts, and transaction timestamps.
+ Windowing: The system should partition the data into specific time windows, such as hourly, daily, or weekly intervals, to analyze customer transactions within those windows.
+ Aggregation: Within each time window, the system should aggregate the transaction amounts for each customer, calculating their total monetary contributions.
+ Ranking: Based on the aggregated amounts, the system should rank the customers and identify the top three customers with the highest transaction amounts within each time window.
+ Output: The system should provide an output that displays the time window, customer IDs, and corresponding total transaction amounts for the top three customers.
The desired output in end of each time window would be:

|     Column      | Data Type |               Description               |
|-----------------|-----------|-----------------------------------------|
| windowStart     | timestamp | Start time of the window                 |
| windowEnd       | timestamp | End time of the window                   |
| customerId      | string    | Identifier of the customer               |
| totalAmount     | double    | Total transaction amount within the window|


The solution should be designed to handle streaming data in real-time, apply window-based computations, and provide timely and accurate results. Additionally, the system should be scalable, fault-tolerant, and capable of handling high-volume and high-velocity data streams.





# Solution
## Description
This solution implements a data processing pipeline to identify the most valuable customers from a stream of random transaction data. The pipeline utilizes Spark's streaming capabilities and applies windowing and watermarking techniques to handle late data effectively.

Random transaction data is generated and added to a MemoryStream, which serves as the streaming source. The transactions include customer IDs, product IDs, transaction amounts, and transaction times.

MemoryStream is a Spark Streaming source that allows developers to simulate real-time data streams by generating data and feeding it into Spark for processing. It is particularly useful in development and testing environments where you don't have access to a live streaming source like Kafka or when you want to quickly prototype and experiment with Spark Streaming applications. First, a MemoryStream object is created, and then data is pushed into it using either a random generator or static insertion. Afterward, the appropriate schema is applied, resulting in the creation of a final DataFrame. 

```
inputMemStreamDF = new MemoryStream<String>(42, sparkSession.sqlContext(), null, Encoders.STRING());
inputMemStreamDF.addData(JavaConverters.asScalaBuffer(Arrays.asList(
    "Customer1,Product5,45,2023-06-01 03:44:00",
    "Customer2,Product2,20,2023-06-02 10:15:30",
    "Customer3,Product8,70,2023-06-03 16:20:45"
)).toSeq());

inputMemStreamDF
    .toDF()
    .selectExpr(
            "cast(split(value,',')[0] as string) as customerId",
            "cast(split(value,',')[1] as string) as productId",
            "cast(split(value,',')[2] as string) as amount",
            "to_timestamp(cast(split(value,',')[3] as string), 'yyyy-MM-dd HH:mm:ss') as transactionTime"
    );
```
MemoryStream simplifies the development and testing process by generating and processing streaming data within Spark, eliminating the need for external dependencies. MemoryStream enables rapid iteration and debugging as data ingestion and processing occur in-memory, providing near-instant feedback loops. It allows developers to have control and predictability over the data by generating custom datasets and simulating specific scenarios. MemoryStream reduces setup and maintenance overhead by eliminating the need for additional infrastructure components. It seamlessly integrates with Spark's ecosystem, leveraging its full streaming capabilities and rich set of APIs and functions. However, it is important to note that MemoryStream may not be suitable for production deployments handling high-volume, real-time data. For production-grade streaming applications, dedicated streaming sources like Kafka provide scalability, fault-tolerance, and advanced features. Overall, MemoryStream is a convenient tool for rapid prototyping, iterative development, and debugging in a controlled and simplified environment within the Spark framework.

Within each window, defined by a specific time duration, the solution aggregates the transaction data to calculate the total amount spent by each customer. This aggregation helps identify the most valuable customers based on their overall transaction amounts within each window.

To handle potential delays in the arrival of data, watermarking is applied, allowing the system to discard late data beyond a specified threshold. This ensures that the analysis focuses on timely and accurate information.

The processed results, which include the window start and end times, customer IDs, and total transaction amounts, are then outputted using a foreachBatch sink. Within each batch, the solution identifies the most valuable customers within the window by applying window functions and ranking the customers based on their total transaction amounts.

This solution provides businesses with insights into their most valuable customers within specific time windows, enabling them to make data-driven decisions and optimize their customer-centric strategies.

## Gradle Configuration

To properly configure the project dependencies, ensure that you have the following dependencies specified in your Gradle build file (build.gradle). This configuration can serve as a template for any Spark Java development:

```
dependencies {
    implementation group: 'com.oracle.ojdbc', name: 'ojdbc8', version: '19.3.0.0'
    implementation 'org.apache.kafka:kafka-clients:3.4.0'
    implementation 'org.apache.logging.log4j:log4j:2.19.0'

    // APACHE SPARK stuff ------------------
    // Scala version (2.12) and spark version (3.3.0) must be the same between different libraries
    implementation 'org.apache.spark:spark-core_2.12:3.3.0'
    implementation 'org.apache.spark:spark-sql_2.12:3.3.0'
    implementation 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
    implementation 'org.apache.spark:spark-avro_2.12:3.3.0'
    implementation('org.codehaus.janino:commons-compiler') {
        version {
            strictly '3.0.16'
        }
    }
    implementation('org.codehaus.janino:janino') {
        version {
            strictly '3.0.16'
        }
    }
    // APACHE SPARK stuff ------------------

    implementation 'com.typesafe:config:1.4.1'
}

```

This configuration can be used as a starting point for your Spark Java projects. Simply modify or add additional dependencies as per your specific requirements.

## Version Compatibility

Java| Spark|gradle     
--- | --- | ---
1.8.0_321| 3.3.0| 6.7

## Configuration
Adjust the configuration settings in `application.conf` to customize the streaming window duration, output file path, or other relevant parameters.

## Contributing
Contributions are welcome! If you have any ideas, suggestions, or bug fixes, feel free to submit a pull request.

## License
This project is licensed under the MIT License.

## Contact
For any inquiries or support, please contact `hadi.ezatpanah@gmail.com`.

This is just a template, so make sure to customize it with the appropriate details specific to your project.

## Author

👤 **Hadi Ezatpanah**

- Github: [@hadiezatpanah](https://github.com/hadiezatpanah)

## Version History
* 0.1
    * Initial Release
