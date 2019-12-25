package kafka.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class Hotel {

    public static final String COMMA_DELIMITER = ",";

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
        if (id == null)
            return "";
        else
            return id.toString() + COMMA_DELIMITER +
                    name + COMMA_DELIMITER +
                    country + COMMA_DELIMITER +
                    city + COMMA_DELIMITER +
                    address + COMMA_DELIMITER +
                    latitude.toString() + COMMA_DELIMITER +
                    longitude.toString() + COMMA_DELIMITER +
                    geoHash;
    }
}

