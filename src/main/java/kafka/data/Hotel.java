package kafka.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class Hotel {

    public static final String COMMA_DELIMETER = ",";

    @JsonProperty("id")
    private Long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("country")
    private String country;

    @JsonProperty("city")
    private String city;

    @JsonProperty("address")
    private String address;

    @JsonProperty("latitude")
    private Double latitude;

    @JsonProperty("longitude")
    private Double longitude;

    @JsonProperty("geoHash")
    private String geoHash;

    @Override
    public String toString() {
        return id.toString() + COMMA_DELIMETER +
                name + COMMA_DELIMETER +
                country + COMMA_DELIMETER +
                city + COMMA_DELIMETER +
                address + COMMA_DELIMETER +
                latitude.toString() + COMMA_DELIMETER +
                longitude.toString() + COMMA_DELIMETER +
                geoHash;
    }
}

