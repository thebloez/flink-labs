package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Detail {
    @JsonProperty("namaPedagang")
    private String namaPedagang;
    @JsonProperty("kategori")
    private String kategori;
    @JsonProperty("deskripsi")
    private String deskripsi;
}
