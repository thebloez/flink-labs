package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class PembelianByKategori {
    private String kategori;
    private BigDecimal total;
}
