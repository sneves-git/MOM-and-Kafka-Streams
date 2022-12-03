package pt.uc.dei.streams;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.json.JSONObject;

public class KafkaStreams_ {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Wrong arguments. Please run the class as follows:");
            System.err.println(KafkaStreams_.class.getName() + " input-topic output-topic");
            System.exit(1);
        }
        String topicStandardWeather = args[0].toString();
        String topicWeatherAlerts = args[1].toString();

        String topicResults = "topicResults";

        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines_standard_weather = builder.stream(topicStandardWeather);
        KStream<String, String> lines_weather_alerts = builder.stream(topicWeatherAlerts);

        lines_standard_weather
                .peek((key, value) -> System.out.println("[lines_standard_weather] key: " + key + " value: " + value));
        lines_weather_alerts
                .peek((key, value) -> System.out.println("[lines_weather_alerts] key: " + key + " value: " + value));
        // Exercicios
        // 1. Count temperature readings of standard weather events per weather station.
        lines_standard_weather.groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("1. key: " + key + " value: " + value))
                .to(topicResults);

        // 2. Count temperature readings of standard weather events per location.
        lines_standard_weather.map((key, value) -> {
            String location = getLocation(value);
            return new KeyValue<>(location, key);
        })
                .groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("2. key: " + key + " value: " + value))
                .to("topicResults");

        // 3. Get minimum and maximum temperature per weather station.
        lines_standard_weather.map((key, value) -> {
            Double temperature = getTemperature(value);
            return new KeyValue<>(key, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) > 0 ? a : b)
                .toStream()
                .peek((key, value) -> System.out.println("3. Max ==== WS: " + key + " maximum: " +
                        value))
                .to("topicResults");

        lines_standard_weather.map((key, value) -> {
            Double temperature = getTemperature(value);
            return new KeyValue<>(key, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) < 0 ? a : b)
                .toStream()
                .peek((key, value) -> System.out.println("3. Min ==== WS: " + key + " minimum: " + value))
                .to("topicResults");

        // 4. Get minimum and maximum temperature per location (Students should compute
        // these values in Fahrenheit).
        lines_standard_weather.map((key, value) -> {
            String location = getLocation(value);
            Double temperature = getTemperature(value);
            return new KeyValue<>(location, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) > 0 ? a : b)
                .toStream()
                .peek((key, value) -> System.out.println("4. Max ==== location: " + key + " maximum: " + value))
                .to("topicResults");

        lines_standard_weather.map((key, value) -> {
            String location = getLocation(value);
            Double temperature = getTemperature(value);
            return new KeyValue<>(location, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> Double.compare(a, b) < 0 ? a : b)
                .toStream()
                .peek((key, value) -> System.out.println("4. Min ==== location: " + key + " minimum: " + value))
                .to("topicResults");

        // 5. Count the total number of alerts per weather station.
        lines_weather_alerts.groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("5. key: " + key + " value: " + value))
                .to(topicResults);

        // 6. Count the total alerts per type.
        lines_weather_alerts.map((key, value) -> {
            String type = getType(value);
            return new KeyValue<>(type, key);
        })
                .groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("6. key: " + key + " value: " + value))
                .to("topicResults");

        // 7. Get minimum temperature of weather stations with red alert events.
        lines_weather_alerts.map((key, value) -> {
            String type = getType(value);
            return new KeyValue<>(type, key);
        }).
        // 8. Get maximum temperature of each location of alert events for the last hour
        // (students are allowed to define a different value for the time window).

        // 9. Get minimum temperature per weather station in red alert zones.

        // 10. Get the average temperature per weather station.
        KTable<String, Long> sum = lines_standard_weather.map((key, value) -> {
                            Double temperature = getTemperature(value);
                            return new KeyValue<>(key, temperature);
                        })
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                        .count();

                        
        lines_standard_weather.map((key, value) -> {
            Double temperature = getTemperature(value);
            return new KeyValue<>(key, temperature);
        })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((a, b) -> a + b)
                .toStream()
                .peek((key, value) -> System.out.println("3. Max ==== WS: " + key + " maximum: " +
                        value))
                .to("topicResults");


        // 11. Get the average temperature of weather stations with red alert events for
        // the last hour
        // (students are allowed to define a different value for the time window).

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
}