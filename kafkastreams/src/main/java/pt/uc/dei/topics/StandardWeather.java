package pt.uc.dei.topics;

public class StandardWeather {
    private String location, weatherStation;
    private Double temperature;

    public StandardWeather(String location, Double temperature, String weatherStation) {
        this.location = location;
        this.temperature = temperature;
        this.weatherStation = weatherStation;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocation() {
        return this.location;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Double getTemperature() {
        return this.temperature;
    }

    public void setWeatherStation(String weatherStation) {
        this.weatherStation = weatherStation;
    }

    public String getWeatherStation() {
        return this.weatherStation;
    }

    public String toString() {
        return "Weather Station: " + weatherStation + " || Location: " + location + " - Temperature: " + temperature;
    }
}