package kafka.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class HotelWeather {
    @JsonProperty("id")
    public Long hotelID;
    @JsonProperty("name")
    public String hotelName;
    @JsonProperty("avg_tmpr_c")
    public Double averageTemperatureInCelsius;
    @JsonProperty("avg_tmpr_f")
    public Double averageTemperatureInFahrenheit;
    @JsonProperty("wthr_date")
    private String weatherDate;
}
