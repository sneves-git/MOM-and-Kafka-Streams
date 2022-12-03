package pt.uc.dei.topics;

public class WeatherAlerts {
    private String location, type, weatherStation;

    public WeatherAlerts(String location, String type, String weatherStation) {
        this.location = location;
        this.type = type;
        this.weatherStation = weatherStation;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocation() {
        return this.location;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public void setWeatherStation(String weatherStation) {
        this.weatherStation = weatherStation;
    }

    public String getWeatherStation() {
        return this.weatherStation;
    }

    public String toString() {
        return "Weather Station: " + this.weatherStation + " || Location: " + this.location + " - " + " Alert Type: "
                + this.type;
    }
}