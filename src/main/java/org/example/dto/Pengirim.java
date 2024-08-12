package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pengirim {
    @JsonProperty("nama")
    private String nama;
    @JsonProperty("nomorRekening")
    private String nomorRekening;
}
