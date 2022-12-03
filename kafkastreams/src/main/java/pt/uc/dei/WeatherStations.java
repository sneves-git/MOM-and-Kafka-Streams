package pt.uc.dei;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import pt.uc.dei.topics.StandardWeather;
import pt.uc.dei.topics.WeatherAlerts;

public class WeatherStations {
    public static void main(String[] args) throws Exception { // Assign topicName to string variable

        if (args.length != 2) {
            System.err.println("Wrong arguments. Please run the class as follows:");
            System.err.println(WeatherStations.class.getName() + " StandardWeaher-topic WeatherAlert-topic");
            System.exit(1);
        }
        String StandardWeather = args[0].toString();
        String WeatherAlert = args[1].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties(); // Assign localhost id
        props.put("bootstrap.servers", "broker1:9092");
        // Set acknowledgements for producer requests. props.put("acks", "all");
        // If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        // Specify buffer size in config
        props.put("batch.size", 16384);
        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        // The buffer.memory controls the total amount of memory available to the
        // producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // WeatherStations
        String[] weatherStations = { "WS1", "WS2", "WS3", "WS4" };

        // Locations
        String[] locations = { "Lisboa", "Coimbra", "Viseu", "Guarda", "Leiria" };

        int max = 40;
        int min = -10;
        Random r = new Random();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        // producer configs
        Producer<String, String> producer = new KafkaProducer<>(props);
        int i = 0;
        for (int j = 0; j < 10; j++) {
            // RANDOM -> r.nextInt((max-min) + 1) + min;
            int randomWeatherStation = r.nextInt(4); // Max = 3 & Min = 0
            int randomLocation = r.nextInt(4 - 0 + 1); // Max = 4 & Min = 0
            int randomTopic = r.nextInt(2 - 1 + 1) + 1; // Max = 2 & Min = 1
            // int randomTopic = 1;
            // Standard Weather Topic - location and temperature (in Fahrenheit)
            if (randomTopic == 1) {
                int randomTemperatureCelsius = r.nextInt((max - min) + 1) + min;
                Double randomTemperatureFahrenheit = ((9.0 / 5.0) * randomTemperatureCelsius + 32);
                StandardWeather sw = new StandardWeather(locations[randomLocation], randomTemperatureFahrenheit,
                        weatherStations[randomWeatherStation]);
                String jsonString = gson.toJson(sw);

                producer.send(new ProducerRecord<String, String>(StandardWeather,
                        weatherStations[randomWeatherStation], jsonString));

                System.out.println("[StandardWeather] Sending message to topic " + StandardWeather + "\t" + sw);

            }
            // Weather Alerts Topic - location and type(red/green)
            else {
                int randomAlert = r.nextInt(2 - 1 + 1) + 1; // Max = 2 & Min = 1

                // red alert
                if (randomAlert == 1) {
                    WeatherAlerts wa = new WeatherAlerts(locations[randomLocation], "red",
                            weatherStations[randomWeatherStation]);
                    String jsonString = gson.toJson(wa);

                    producer.send(new ProducerRecord<String, String>(WeatherAlert,
                            weatherStations[randomWeatherStation], jsonString));
                    System.out.println("[WeatherAlert] Sending message to topic " + WeatherAlert + "\t" + wa);
                }
                // green alert
                else {
                    WeatherAlerts wa = new WeatherAlerts(locations[randomLocation], "green",
                            weatherStations[randomWeatherStation]);
                    String jsonString = gson.toJson(wa);

                    producer.send(new ProducerRecord<String, String>(WeatherAlert,
                            weatherStations[randomWeatherStation], jsonString));
                    System.out.println("[WeatherAlert] Sending message to topic " + WeatherAlert + "\t" + wa);
                }
            }
            i++;
        }
        producer.close();
    }
}