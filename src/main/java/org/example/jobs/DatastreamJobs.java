package org.example.jobs;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.dto.FraudTransferAggregation;
import org.example.dto.Transfer;
import org.example.stateful.FraudAgregationProcessFunction;

import java.util.Objects;

public class DatastreamJobs {
    public static void createFraudAggregationTable(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transfer_fraud (" +
                        "namapengirim VARCHAR(255) PRIMARY KEY," +
                        "totaltransfer DECIMAL" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {
                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create fraud_aggregation Table Sink");
    }

    public static void insertTransferFraud(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        DataStream<Transfer> transferStream = transactionStream
                .filter(Objects::nonNull)
                .filter(transaction -> transaction instanceof Transfer)
                .map(transaction -> (Transfer) transaction);

        transferStream
                .keyBy(transfer -> transfer.getPenerima().getNama())
                .process(new FraudAgregationProcessFunction())
                .addSink(JdbcSink.sink(
                        "INSERT INTO transfer_fraud (namapengirim, totaltransfer) VALUES (?, ?) " +
                                "ON CONFLICT (namapengirim) DO UPDATE SET totaltransfer = EXCLUDED.totaltransfer",
                        (JdbcStatementBuilder<FraudTransferAggregation>) (preparedStatement, fraudTransferAggregation) -> {
                            preparedStatement.setString(1, fraudTransferAggregation.getNamaPengirim());
                            preparedStatement.setBigDecimal(2, fraudTransferAggregation.getTotalTransfer());
                        },
                        jdbcExeOpt,
                        jdbcConnOpt
                )).name("Insert fraud Aggregation Sink");
    }
}
