package pt.uc.dei.streams;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
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

import pt.uc.dei.streams.OtherStreams.IntArraySerde;

public class KafkaStreams_ {

        public static void main(String[] args) {
                if (args.length != 2) {
                        System.err.println("Wrong arguments. Please run the class as follows:");
                        System.err.println(
                                        KafkaStreams_.class.getName() + " StandardWeather-topic WeatherAlerts-topic");
                        System.exit(1);
                }
                String topicStandardWeather = args[0].toString();
                String topicWeatherAlerts = args[1].toString();

                String topicResults = "topicResults";

                java.util.Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-13");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9091");

                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

                StreamsBuilder builder = new StreamsBuilder();
                KStream<String, String> lines_standard_weather = builder.stream(topicStandardWeather);
                KStream<String, String> lines_weather_alerts = builder.stream(topicWeatherAlerts);
                writeToFile("/workspace/kafkastreams/outputs/out.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

                lines_standard_weather
                                .peek((key, value) -> writeToFile("/workspace/kafkastreams/outputs/out.txt",
                                                "[lines_standard_weather] key: " + key + " value: " + value));
                lines_weather_alerts
                                .peek((key, value) -> writeToFile("/workspace/kafkastreams/outputs/out.txt",
                                                "[lines_weather_alerts] key: " + key + " value: " + value));

                lines_standard_weather
                                .peek((key, value) -> System.out
                                                .println("[lines_standard_weather] key: " + key + " value: " + value));
                lines_weather_alerts
                                .peek((key, value) -> System.out
                                                .println("[lines_weather_alerts] key: " + key + " value: " + value));
                // Exercicios
                // 1. Count temperature readings of standard weather events per weather station.
                KTable<String, Long> countTemperaturePerWs = lines_standard_weather.groupByKey()
                                .count();

                writeToFile("/workspace/kafkastreams/outputs/ex1.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);
                countTemperaturePerWs.toStream()
                                .peek((key, value) -> {
                                        String output = "1. key: " + key + " value: " + value;
                                        writeToFile("/workspace/kafkastreams/outputs/ex1.txt", output);
                                        System.out.println("1. key: " + key + " value: " + value);
                                })
                                .to(topicResults);

                // 2. Count temperature readings of standard weather events per location.
                writeToFile("/workspace/kafkastreams/outputs/ex2.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

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

                writeToFile("/workspace/kafkastreams/outputs/ex3.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);
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
                                        System.out.println(output);
                                })
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
                                        System.out.println(output);
                                })
                                .to(topicResults);

                // 4. Get minimum and maximum temperature per location (Students should compute
                // these values in Fahrenheit).
                writeToFile("/workspace/kafkastreams/outputs/ex4.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

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
                                        System.out.println(output);
                                })
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
                                        System.out.println(output);
                                })
                                .to(topicResults);

                // 5. Count the total number of alerts per weather station.
                writeToFile("/workspace/kafkastreams/outputs/ex5.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

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
                writeToFile("/workspace/kafkastreams/outputs/ex6.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

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
                writeToFile("/workspace/kafkastreams/outputs/ex7.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

                KStream<String, String> redAlertsPerWs = lines_weather_alerts.map((key, value) -> {
                        String type = getType(value);
                        return new KeyValue<>(key, type);
                }).filter((key, value) -> value.compareTo("red") == 0);

                KStream<String, String> temp = lines_standard_weather.map((key, value) -> {
                        Double tempDouble = getTemperature(value);
                        return new KeyValue<>(key, String.valueOf(tempDouble));
                });

                ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> {
                        return rightValue;
                };
                redAlertsPerWs.join(temp, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(60)))
                                .map((key, value) -> {
                                        JSONObject joinedValue;
                                        joinedValue = new JSONObject();
                                        joinedValue.put("temperature", Double.valueOf(value));
                                        joinedValue.put("WS", key);
                                        return new KeyValue<>("1", joinedValue.toString());
                                })
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                                .reduce((v1, v2) -> {
                                        Double a = getTemperature(v1);
                                        Double b = getTemperature(v2);

                                        return Double.compare(a, b) < 0 ? v1 : v2;
                                })
                                .toStream()
                                .peek((key, value) -> {
                                        String output = "7. key: " + key + " value: " + value;
                                        writeToFile("/workspace/kafkastreams/outputs/ex7.txt", output);
                                        System.out.println(output);
                                })
                                .to(topicResults);

                // 8. Get maximum temperature of each location of alert events for the last hour
                // (students are allowed to define a different value for the time window).
                // Max_temperaturePerL -> right
                writeToFile("/workspace/kafkastreams/outputs/ex8.txt",
                                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

                KStream<String, String> alertsPerL = lines_weather_alerts.map((key, value) -> {
                        String type = getType(value);
                        String location = getLocation(value);
                        return new KeyValue<>(location, type);
                });
                KStream<String, String> tempLocal = lines_standard_weather.map((key, value) -> {
                        String local = getLocation(value);
                        Double tempDouble = getTemperature(value);
                        return new KeyValue<>(local, String.valueOf(tempDouble));
                });

                Duration windowSize = Duration.ofMinutes(60);
                Duration advanceSize = Duration.ofMinutes(60);
                TimeWindows hoppingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceSize);

                alertsPerL.join(tempLocal, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(60)))
                                .groupByKey()
                                .reduce((v1, v2) -> {
                                        Double a = Double.valueOf(v1);
                                        Double b = Double.valueOf(v1);
                                        return Double.compare(a, b) > 0 ? v1 : v2;
                                })
                                .toStream()
                                .peek((key, value) -> {
                                        String output = "8. key: " + key + " value: " + value;
                                        writeToFile("/workspace/kafkastreams/outputs/ex8.txt", output);
                                        System.out.println(output);
                                })
                                .to(topicResults);

                // 9. Get minimum temperature per weather station in red alert zones.
                writeToFile("/workspace/kafkastreams/outputs/ex9.txt",
                "======================= topics: " + topicStandardWeather + "  " + topicWeatherAlerts);

redAlertsPerWs.join(temp, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(60)))
                .map((key, value) -> {
                        JSONObject joinedValue;
                        joinedValue = new JSONObject();
                        joinedValue.put("temperature", Double.valueOf(value));
                        joinedValue.put("WS", key);
                        return new KeyValue<>("1", joinedValue.toString());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce((v1, v2) -> {
                        Double a = getTemperature(v1);
                        Double b = getTemperature(v2);

                        return Double.compare(a, b) < 0 ? v1 : v2;
                })
                .toStream()
                .peek((key, value) -> {
                        String output = "9. key: " + key + " value: " + value;
                        writeToFile("/workspace/kafkastreams/outputs/ex9.txt", output);
                        System.out.println(output);
                })
                .to(topicResults);
                // 10. Get the average temperature per weather station.

                writeToFile("/workspace/kafkastreams/outputs/ex10.txt",
                                "======================= topics: " + topicStandardWeather + "  " +
                                                topicWeatherAlerts);

                lines_standard_weather.map((key, value) -> {
                        Double temperature = getTemperature(value);
                        return new KeyValue<>(key, temperature);
                }).groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                                .aggregate(() -> new int[] { 0, 0 }, (aggKey, newValue, aggValue) -> {
                                        aggValue[0] += 1;
                                        aggValue[1] += newValue;

                                        return aggValue;
                                }, Materialized.with(Serdes.String(), new IntArraySerde()))
                                .mapValues(v -> {
                                        writeToFile("/workspace/kafkastreams/outputs/ex10.txt",
                                                        "10. " + "count: " + v[0] + " somatorio " + v[1]);
                                        return v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0";
                                })
                                .toStream()
                                .to(topicResults);

                // 11. Get the average temperature of weather stations with red alert events for
                // the last hour (students are allowed to define a different value for the time
                // window).

                writeToFile("/workspace/kafkastreams/outputs/ex11.txt",
                                "======================= topics: " + topicStandardWeather + "  " +
                                                topicWeatherAlerts);

                KTable<String, int[]> tempWS = lines_standard_weather.map((key, value) -> {
                        Double tempDouble = getTemperature(value);
                        return new KeyValue<>(key, tempDouble);
                }).groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                                .aggregate(() -> new int[] { 0, 0 }, (aggKey, newValue, aggValue) -> {
                                        aggValue[0] += 1;
                                        aggValue[1] += newValue;

                                        return aggValue;
                                }, Materialized.with(Serdes.String(), new IntArraySerde()));

                ValueJoiner<String, int[], String> valueJoiner3 = (leftValue, rightValue) -> {
                        JSONObject joinedValue;
                        joinedValue = new JSONObject();

                        joinedValue.put("average", (float) rightValue[1] / rightValue[0]);
                        joinedValue.put("WS", getWS9(leftValue));
                        joinedValue.put("type", getType(leftValue));

                        return joinedValue.toString();
                };

                KStream<String, String> redAlertEvents = lines_weather_alerts.map((key,
                                value) -> {
                        JSONObject joinedValue;
                        joinedValue = new JSONObject();
                        joinedValue.put("type", getType(value));
                        joinedValue.put("weatherStation", key);
                        return new KeyValue<>(key, joinedValue.toString());
                }).filter((key, value) -> {
                        String type = getType(value);
                        return type.compareTo("red") == 0;
                });

                redAlertEvents.join(tempWS, valueJoiner3)
                                .groupByKey()
                                .windowedBy(hoppingWindow)
                                .aggregate(() -> "", (aggKey, newValue, aggValue) -> {
                                        return newValue;
                                }, Materialized.with(Serdes.String(), new StringSerde()))
                                .toStream()
                                .peek((key, value) -> {
                                        writeToFile("/workspace/kafkastreams/outputs/ex11.txt",
                                                        "11. " + value);
                                })
                                .to(topicResults);

                KafkaStreams streams = new KafkaStreams(builder.build(), props);
                streams.start();

        }

        public static String getLocation(String jsonString) {
                JSONObject toString = new JSONObject(jsonString);
                return toString.getString("location");
        }

        public static String getWS(String jsonString) {
                JSONObject toString = new JSONObject(jsonString);
                return toString.getString("WS");
        }

        public static String getWS9(String jsonString) {
                JSONObject toString = new JSONObject(jsonString);
                return toString.getString("weatherStation");
        }

        public static Double getTemperature(String jsonString) {
                JSONObject toString = new JSONObject(jsonString);
                return toString.getDouble("temperature");
        }

        public static String getType(String jsonString) {
                JSONObject toString = new JSONObject(jsonString);

                return toString.getString("type");
        }

        public static String getAverage(Float jsonString) {
                JSONObject toString = new JSONObject(jsonString);

                return toString.getString("average");
        }

        public static void writeToFile(String filename, String str) {
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