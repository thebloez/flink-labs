package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


/*
    {
    "IDTransaksi": "TRX20240809155109538",
    "tanggal": "2024-08-09",
    "jenis": "transfer",
    "jumlah": 2485410,
    "mataUang": "IDR",
    "pengirim": {
        "nama": "Clara Goodwin",
        "nomorRekening": "0390183949"
    },
    "penerima": {
        "nama": "Cole Kris",
        "nomorRekening": "0895312608"
    }
}
*/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transfer {
    @JsonProperty("IDTransaksi")
    private String IDTransaksi;
    private String tanggal;
    private String jenis;
    private BigDecimal jumlah;
    private String mataUang;
    private Pengirim pengirim;
    private Penerima penerima;
}

