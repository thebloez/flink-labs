package org.example.stateful;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.FraudTransferAggregation;
import org.example.dto.Transfer;

import java.math.BigDecimal;
import java.util.Objects;

public class FraudAgregationProcessFunction extends KeyedProcessFunction<String, Transfer, FraudTransferAggregation> {
    private transient ValueState<BigDecimal> totalTransferState;

    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<BigDecimal> totalTransferDescriptor = new ValueStateDescriptor<>(
                "totalTransferState",
                Types.BIG_DEC
        );
        totalTransferState = getRuntimeContext().getState(totalTransferDescriptor);
    }

    @Override
    public void processElement(Transfer value, Context ctx, Collector<FraudTransferAggregation> out) throws Exception {
        if (Objects.equals(value.getPenerima().getNama(), value.getPengirim().getNama())) {
            BigDecimal currentTotal = totalTransferState.value();
            if (currentTotal == null) {
                currentTotal = BigDecimal.ZERO;
            }
            currentTotal = currentTotal.add(value.getJumlah());
            totalTransferState.update(currentTotal);

            if (currentTotal.compareTo(new BigDecimal(1000_000)) > 0 ) {
                out.collect(new FraudTransferAggregation(value.getPengirim().getNama(), currentTotal));
            }
        }
    }
}
