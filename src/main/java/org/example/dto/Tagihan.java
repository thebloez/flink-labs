package org.example.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Tagihan {
    @JsonProperty("IDTransaksi")
    private String IDTransaksi;
    private String tanggal;
    private String jenis;
    private long jumlah;
    private String mataUang;
    private String penyediaJasa;
    private String nomorPelanggan;
    private String periodeTagihan;
}
