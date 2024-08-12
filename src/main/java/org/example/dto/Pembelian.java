package org.example.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/*
{
    "IDTransaksi": "TRX20240809155108464",
    "tanggal": "2024-08-09",
    "jenis": "pembelian"
    "jumlah": 1937559,
    "mataUang": "IDR",
    "metodePembayaran": "kartu_kredit",
    "detail": {
        "namaPedagang": "xDayta",
        "kategori": "makanan",
        "deskripsi": "Pembelian Barang Pribadi"
    }
}
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pembelian {
    @JsonProperty("IDTransaksi")
    private String IDTransaksi;
    private String tanggal;
    private String jenis;
    private BigDecimal jumlah;
    private String mataUang;
    private String metodePembayaran;
    private Detail detail;
}

