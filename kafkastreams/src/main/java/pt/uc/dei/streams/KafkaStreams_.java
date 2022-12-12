package pt.uc.dei.streams;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONObject;

public class KafkaStreams_ {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Wrong arguments. Please run the class as follows:");
            System.err.println(KafkaStreams_.class.getName() + " StandardWeather-topic WeatherAlerts-topic");
            System.exit(1);
        }
        String topicStandardWeather = args[0].toString();
        String topicWeatherAlerts = args[1].toString();

        String topicResults = "topicResults";
        System.out.println("================================================================================");
        System.out.println( "/workspace/kafkastreams/outputs/teste.txt");
        writeToFile("/workspace/kafkastreams/outputs/teste.txt","TESTEEEEEE");


        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines_standard_weather = builder.stream(topicStandardWeather);
        KStream<String, String> lines_weather_alerts = builder.stream(topicWeatherAlerts);
        writeToFile("/workspace/kafkastreams/outputs/inputs.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);
        writeToFile("/workspace/kafkastreams/outputs/ex1.txt",lines_standard_weather);

        lines_standard_weather
        .peek((key, value) -> writeToFile("/workspace/kafkastreams/outputs/ex1.txt","[lines_standard_weather] key: " + key + " value: " + value));
        lines_weather_alerts
        .peek((key, value) ->writeToFile("/workspace/kafkastreams/outputs/ex1.txt","[lines_weather_alerts] key: " + key + " value: " + value));

        lines_standard_weather
                .peek((key, value) -> System.out.println("[lines_standard_weather] key: " + key + " value: " + value));
        lines_weather_alerts
                .peek((key, value) -> System.out.println("[lines_weather_alerts] key: " + key + " value: " + value));
        // Exercicios
        // 1. Count temperature readings of standard weather events per weather station.
        KTable<String, Long> countTemperaturePerWs = lines_standard_weather.groupByKey()
                .count();

        writeToFile("/workspace/kafkastreams/outputs/ex1.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);
        countTemperaturePerWs.toStream()
                .peek((key, value) -> {
                        String output = "1. key: " + key + " value: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex1.txt",output);
                        System.out.println("1. key: " + key + " value: " + value);
                }
                        )
                .to(topicResults);

        // 2. Count temperature readings of standard weather events per location.
        writeToFile("/workspace/kafkastreams/outputs/ex2.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        lines_standard_weather.map((key, value) -> {
            String location = getLocation(value);
            return new KeyValue<>(location, key);
        })
                .groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> {
                        String output = "2. key: " + key + " value: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex2.txt", output);

                        System.out.println(output);
                })
                .to(topicResults);

        writeToFile("/workspace/kafkastreams/outputs/ex3.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);
        // 3. Get minimum and maximum temperature per weather station.
        KTable<String, Double> Max_temperaturePerWs = lines_standard_weather.map((key, value) -> {
            Double temperature = getTemperature(value);
            return new KeyValue<>(key, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) > 0 ? a : b);

        Max_temperaturePerWs.toStream()
                .peek((key, value) -> {
                        
                        String output = "3. Max ==== WS: " + key + " maximum: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex3.txt", output);
                        System.out.println(output);}
                        )
                .to(topicResults);

        KTable<String, Double> Min_temperaturePerWs = lines_standard_weather.map((key, value) -> {
            Double temperature = getTemperature(value);
            return new KeyValue<>(key, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) < 0 ? a : b);

        Min_temperaturePerWs.toStream()
                .peek((key, value) -> {
                        String output = "3. Min ==== WS: " + key + " maximum: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex3.txt", output);
                        System.out.println(output);}
                        )
                .to(topicResults);

        // 4. Get minimum and maximum temperature per location (Students should compute
        // these values in Fahrenheit).
        writeToFile("/workspace/kafkastreams/outputs/ex4.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        KTable<String, Double> Max_temperaturePerL = lines_standard_weather.map((key, value) -> {
            String location = getLocation(value);
            Double temperature = getTemperature(value);
            return new KeyValue<>(location, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) > 0 ? a : b);

        Max_temperaturePerL.toStream()
                .peek((key, value) -> {
                        String output = "4. Max ==== location: " + key + " maximum: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex4.txt", output);
                        System.out.println(output);}
                        )
                .to(topicResults);

        lines_standard_weather.map((key, value) -> {
            String location = getLocation(value);
            Double temperature = getTemperature(value);
            return new KeyValue<>(location, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) < 0 ? a : b)
                .toStream()
                .peek((key, value) -> {
                        String output = "4. Min ==== location: " + key + " minimum: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex4.txt", output);
                        System.out.println(output);}
                        )
                .to(topicResults);

        // 5. Count the total number of alerts per weather station.
        writeToFile("/workspace/kafkastreams/outputs/ex5.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        lines_weather_alerts.groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> {
                        String output = "5. key: " + key + " value: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex5.txt", output);
                        System.out.println(output);
                })
                .to(topicResults);

        // 6. Count the total alerts per type.
        writeToFile("/workspace/kafkastreams/outputs/ex6.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        lines_weather_alerts.map((key, value) -> {
            String type = getType(value);
            return new KeyValue<>(type, key);
        })
                .groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> {
                        String output = "6. key: " + key + " value: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex6.txt", output);
                        System.out.println(output);
                })
                .to(topicResults);
        // 7. Get minimum temperature of weather stations with red alert events.
        // lines_weather_alerts -> left stream Kstream
        // Min_temperaturePerWs -> right stream Ktable
        writeToFile("/workspace/kafkastreams/outputs/ex7.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        KStream<String, String> redAlertsPerWs = lines_weather_alerts.map((key, value) -> {
            String type = getType(value);
            return new KeyValue<>(key, type);
        }).filter((key, value) -> value.compareTo("red") == 0);

        redAlertsPerWs.join(Min_temperaturePerWs,
                (leftValue, rightValue) -> rightValue)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) < 0 ? a : b)
                .toStream()
                .peek((key, value) -> {
                        String output = "7. key: " + key + " value: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex7.txt", output);
                        System.out.println(output);}
                        )
                .to(topicResults);

        // 8. Get maximum temperature of each location of alert events for the last hour
        // (students are allowed to define a different value for the time window).
        // Max_temperaturePerL -> right
        writeToFile("/workspace/kafkastreams/outputs/ex8.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        KStream<String, String> alertsPerL = lines_weather_alerts.map((key, value) -> {
            String type = getType(value);
            String location = getLocation(value);
            return new KeyValue<>(location, type);
        });

        alertsPerL.join(Max_temperaturePerL, (recordA, recordB) -> {
            return recordB;
        }).groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)));
        // 9. Get minimum temperature per weather station in red alert zones.
        writeToFile("/workspace/kafkastreams/outputs/ex9.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        redAlertsPerWs.join(Min_temperaturePerWs,
                (leftValue, rightValue) -> "Alert: " + leftValue + " - Minimum Temperature: " + rightValue)
                .peek((key, value) -> System.out.println("9. key: " + key + " value: " + value))
                .to(topicResults);

        // 10. Get the average temperature per weather station.
        writeToFile("/workspace/kafkastreams/outputs/ex10.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        KTable<String, Double> sumTemperaturePerWs = lines_standard_weather.map((key, value) -> {
            Double temperature = getTemperature(value);
            return new KeyValue<>(key, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> a + b);

        lines_standard_weather.groupByKey()
                .count()
                .leftJoin(sumTemperaturePerWs, (left, right) -> {
                    return Double.toString(right / left);
                })
                .toStream()
                .peek((key, value) -> {
                        String output = "10. key: " + key + " value: " +value;
                        writeToFile("/workspace/kafkastreams/outputs/ex10.txt", output);
                        System.out.println(output);})
                .to(topicResults);

        // 11. Get the average temperature of weather stations with red alert events for
        // the last hour
        // (students are allowed to define a different value for the time window).
        writeToFile("/workspace/kafkastreams/outputs/ex11.txt","======================= topics: "+ topicStandardWeather + "  " + topicWeatherAlerts);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static String getLocation(String jsonString) {
        JSONObject toString = new JSONObject(jsonString);
        return toString.getString("location");
    }

    public static Double getTemperature(String jsonString) {
        JSONObject toString = new JSONObject(jsonString);

        return toString.getDouble("temperature");
    }

    public static String getType(String jsonString) {
        JSONObject toString = new JSONObject(jsonString);

        return toString.getString("type");
    }

    public static String average(Double d1, Double d2) {
        return Double.toString(d1 / d2);
    }
    public static void writeToFile(String filename, String str ){
        try {
                FileWriter fw = new FileWriter(filename, true);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(str);
                bw.newLine();
                bw.close();
                
              } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
              }
    }
    
}