package kafka.data;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
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