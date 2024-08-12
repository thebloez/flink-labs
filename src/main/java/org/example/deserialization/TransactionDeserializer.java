package org.example.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.example.dto.Pembelian;
import org.example.dto.Tagihan;
import org.example.dto.Transfer;

import java.io.IOException;
import java.util.Map;

public class TransactionDeserializer implements DeserializationSchema<Object> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Object deserialize(byte[] bytes) throws IOException {
        Map<String, Object> mapper = objectMapper.readValue(bytes, Map.class);
        String jenis = (String) mapper.get("jenis");

        switch (jenis) {
            case "transfer":
                return objectMapper.readValue(bytes, Transfer.class);
            case "pembayaran_tagihan":
                return objectMapper.readValue(bytes, Tagihan.class);
            case "pembelian":
                return objectMapper.readValue(bytes, Pembelian.class);
            default:
                throw new IllegalArgumentException("Unknown transaction type: " + jenis);
        }
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation<Object> getProducedType() {
        return Types.GENERIC(Object.class);
    }
}
