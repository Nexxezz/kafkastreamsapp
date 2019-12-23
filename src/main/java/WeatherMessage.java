import javax.ws.rs.DefaultValue;

public class WeatherMessage {

    private Double lng;
    private Double lat;
    private Double avg_tmpr_f;
    private Double avg_tmpr_c;
    private String wthr_date;
    @DefaultValue("empty")
    private String geoHash;

    public WeatherMessage() {
        super();
    }

    public WeatherMessage(Double lng, Double lat, Double avg_tmpr_f, Double avg_tmpr_c, String wthr_date) {
        this.lng = lng;
        this.lat = lat;
        this.avg_tmpr_f = avg_tmpr_f;
        this.avg_tmpr_c = avg_tmpr_c;
        this.wthr_date = wthr_date;
        this.geoHash = geoHash;
    }


    public Double getLng() {
        return lng;
    }

    public void setLng(Double lng) {
        this.lng = lng;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getAvg_tmpr_f() {
        return avg_tmpr_f;
    }

    public void setAvg_tmpr_f(Double avg_tmpr_f) {
        this.avg_tmpr_f = avg_tmpr_f;
    }

    public Double getAvg_tmpr_c() {
        return avg_tmpr_c;
    }

    public void setAvg_tmpr_c(Double avg_tmpr_c) {
        this.avg_tmpr_c = avg_tmpr_c;
    }

    public String getWthr_date() {
        return wthr_date;
    }

    public void setWthr_date(String wthr_date) {
        this.wthr_date = wthr_date;
    }

    public void setGeoHash(String geoHash) {
        this.geoHash = geoHash;
    }

    public String getGeoHash() {
        return geoHash;
    }
}
