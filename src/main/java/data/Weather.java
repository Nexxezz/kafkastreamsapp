package data;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Weather {
    @JsonProperty("lng")
    private Double lng;
    @JsonProperty("lat")
    private Double lat;
    @JsonProperty("avg_tmpr_f")
    private Double averageTemperatureFahrenheit;
    @JsonProperty("avg_tmpr_c")
    private Double averageTemperatureCelsius;
    @JsonProperty("wthr_date")
    private String date;
    @JsonProperty("geoHash")
    private String geoHash;
}