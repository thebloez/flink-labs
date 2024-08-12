package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.deserialization.TransactionDeserializer;
import org.example.dto.Pembelian;
import org.example.dto.Tagihan;
import org.example.dto.Transfer;


public class DatastreamJobs {

    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "transactions";
        env.enableCheckpointing(5000);

        KafkaSource<Object> source = KafkaSource.builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetsInitializer.latest().getAutoOffsetResetStrategy()))
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        DataStream<Object> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        transactionStream.print();

        // set jdbc connection options
        JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        // create table sink
        createTableSinks(transactionStream, jdbcExecutionOptions, jdbcConnectionOptions);

        // insert data sink
        insertDataSinks(transactionStream, jdbcExecutionOptions, jdbcConnectionOptions);

        env.execute("Flink Ecommerce Realtime Streaming");

    }

    private static void createAggregateSinks(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS total_pembelian (" +
                        "tanggal VARCHAR(255) PRIMARY KEY," +
                        "total DECIMAL" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create total_pembelian Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS total_tagihan (" +
                        "tanggal VARCHAR(255) PRIMARY KEY," +
                        "total DECIMAL" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create total_tagihan Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS total_transfer (" +
                        "tanggal VARCHAR(255) PRIMARY KEY," +
                        "total DECIMAL" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create total_transfer Table Sink");
    }

    private static void createTableSinks(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS pembelian (" +
                        "IDTransaksi VARCHAR(255) PRIMARY KEY," +
                        "tanggal VARCHAR(255)," +
                        "jenis VARCHAR(255)," +
                        "jumlah DECIMAL," +
                        "mataUang VARCHAR(255)," +
                        "metodePembayaran VARCHAR(255)," +
                        "namaPedagang VARCHAR(255)," +
                        "kategori VARCHAR(255)," +
                        "deskripsi VARCHAR(255)" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create pembelian Table Sink");


        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS tagihan (" +
                        "IDTransaksi VARCHAR(255) PRIMARY KEY," +
                        "tanggal VARCHAR(255)," +
                        "jenis VARCHAR(255)," +
                        "jumlah DECIMAL," +
                        "mataUang VARCHAR(255)," +
                        "penyediaJasa VARCHAR(255)," +
                        "nomorPelanggan VARCHAR(255)," +
                        "periodeTagihan VARCHAR(255)" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create tagihan Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transfer (" +
                        "IDTransaksi VARCHAR(255) PRIMARY KEY," +
                        "tanggal VARCHAR(255)," +
                        "jenis VARCHAR(255)," +
                        "jumlah DECIMAL," +
                        "mataUang VARCHAR(255)," +
                        "namaPengirim VARCHAR(255)," +
                        "nomorRekeningPengirim VARCHAR(255)," +
                        "namaPenerima VARCHAR(255)," +
                        "nomorRekeningPenerima VARCHAR(255)" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create transfer Table Sink");
    }

    private static void insertDataSinks(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExecutionOptions, JdbcConnectionOptions jdbcConnectionOptions) {
        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO tagihan (IDTransaksi, tanggal, jenis, jumlah, mataUang, penyediaJasa, nomorPelanggan, periodeTagihan) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
                        "ON CONFLICT (IDTransaksi) DO UPDATE SET " +
                        "tanggal = EXCLUDED.tanggal, " +
                        "jenis = EXCLUDED.jenis, " +
                        "jumlah = EXCLUDED.jumlah, " +
                        "mataUang = EXCLUDED.mataUang, " +
                        "penyediaJasa = EXCLUDED.penyediaJasa, " +
                        "nomorPelanggan = EXCLUDED.nomorPelanggan, " +
                        "periodeTagihan = EXCLUDED.periodeTagihan",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {
                    if (transaction instanceof Tagihan) {
                        Tagihan tagihan = (Tagihan) transaction;
                        preparedStatement.setString(1, tagihan.getIDTransaksi());
                        preparedStatement.setString(2, tagihan.getTanggal());
                        preparedStatement.setString(3, tagihan.getJenis());
                        preparedStatement.setLong(4, tagihan.getJumlah());
                        preparedStatement.setString(5, tagihan.getMataUang());
                        preparedStatement.setString(6, tagihan.getPenyediaJasa());
                        preparedStatement.setString(7, tagihan.getNomorPelanggan());
                        preparedStatement.setString(8, tagihan.getPeriodeTagihan());
                    }
                },
                jdbcExecutionOptions,
                jdbcConnectionOptions
        )).name("Tagihan Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO transfer (IDTransaksi, tanggal, jenis, jumlah, mataUang, namaPengirim, nomorRekeningPengirim, namaPenerima, nomorRekeningPenerima) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                        "ON CONFLICT (IDTransaksi) DO UPDATE SET " +
                        "tanggal = EXCLUDED.tanggal, " +
                        "jenis = EXCLUDED.jenis, " +
                        "jumlah = EXCLUDED.jumlah, " +
                        "mataUang = EXCLUDED.mataUang, " +
                        "namaPengirim = EXCLUDED.namaPengirim, " +
                        "nomorRekeningPengirim = EXCLUDED.nomorRekeningPengirim, " +
                        "namaPenerima = EXCLUDED.namaPenerima, " +
                        "nomorRekeningPenerima = EXCLUDED.nomorRekeningPenerima",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {
                    if (transaction instanceof Transfer) {
                        Transfer transfer = (Transfer) transaction;
                        preparedStatement.setString(1, transfer.getIDTransaksi());
                        preparedStatement.setString(2, transfer.getTanggal());
                        preparedStatement.setString(3, transfer.getJenis());
                        preparedStatement.setBigDecimal(4, transfer.getJumlah());
                        preparedStatement.setString(5, transfer.getMataUang());
                        preparedStatement.setString(6, transfer.getPengirim().getNama());
                        preparedStatement.setString(7, transfer.getPengirim().getNomorRekening());
                        preparedStatement.setString(8, transfer.getPenerima().getNama());
                        preparedStatement.setString(9, transfer.getPenerima().getNomorRekening());
                    }
                },
                jdbcExecutionOptions,
                jdbcConnectionOptions
        )).name("Transfer Table Sink");
    }
}
