package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.deserialization.TransactionDeserializer;
import org.example.dto.Pembelian;
import org.example.dto.PembelianByKategori;
import org.example.dto.Tagihan;
import org.example.dto.Transfer;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Objects;


public class DatastreamJobs {

    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String CONSUMER_GROUP = "flink-group";
    private static final String TOPIC = "transactions";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        KafkaSource<Object> source = KafkaSource.builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setTopics(TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
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

        // create master table sink
        createTableSinks(transactionStream, jdbcExecutionOptions, jdbcConnectionOptions);

        // create aggregate
        createTotalPembelianBasedOnKategori(transactionStream, jdbcExecutionOptions, jdbcConnectionOptions);


        // insert data sink
        addTransactionSink(transactionStream, Pembelian.class, "pembelian",
                getInsertQuery("pembelian", new String[]{"IDTransaksi", "tanggal", "jenis", "jumlah", "mataUang", "metodePembayaran", "namaPedagang", "kategori", "deskripsi"}),
                jdbcExecutionOptions, jdbcConnectionOptions);

        addTransactionSink(transactionStream, Tagihan.class, "tagihan",
                getInsertQuery("tagihan", new String[]{"IDTransaksi", "tanggal", "jenis", "jumlah", "mataUang", "penyediaJasa", "nomorPelanggan", "periodeTagihan"}),
                jdbcExecutionOptions, jdbcConnectionOptions);

        addTransactionSink(transactionStream, Transfer.class, "transfer",
                getInsertQuery("transfer", new String[]{"IDTransaksi", "tanggal", "jenis", "jumlah", "mataUang", "namaPengirim", "nomorRekeningPengirim", "namaPenerima", "nomorRekeningPenerima"}),
                jdbcExecutionOptions, jdbcConnectionOptions);

        // insert aggregate sink
        addPembelianByKategoriSinkWithoutState(transactionStream, jdbcExecutionOptions, jdbcConnectionOptions);

        // sink to elastic search
        transactionStream.sinkTo(
                new Elasticsearch7SinkBuilder<Object>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setEmitter((element, context, indexer) -> {
                    if (element instanceof Pembelian) {
                        Pembelian pembelian = (Pembelian) element;
                        IndexRequest request = new IndexRequest("pembelian")
                                .id(pembelian.getIDTransaksi())
                                .source("kategori", pembelian.getDetail().getKategori(),
                                        "jumlah", pembelian.getJumlah(),
                                        "tanggal", pembelian.getTanggal(),
                                        "jenis", pembelian.getJenis(),
                                        "mataUang", pembelian.getMataUang(),
                                        "metodePembayaran", pembelian.getMetodePembayaran(),
                                        "namaPedagang", pembelian.getDetail().getNamaPedagang(),
                                        "deskripsi", pembelian.getDetail().getDeskripsi());
                        indexer.add(request);
                    } else if (element instanceof Tagihan) {
                        Tagihan tagihan = (Tagihan) element;
                        IndexRequest request = new IndexRequest("tagihan")
                                .id(tagihan.getIDTransaksi())
                                .source("penyediaJasa", tagihan.getPenyediaJasa(),
                                        "jumlah", tagihan.getJumlah(),
                                        "tanggal", tagihan.getTanggal(),
                                        "jenis", tagihan.getJenis(),
                                        "mataUang", tagihan.getMataUang(),
                                        "nomorPelanggan", tagihan.getNomorPelanggan(),
                                        "periodeTagihan", tagihan.getPeriodeTagihan());
                        indexer.add(request);
                    } else if (element instanceof Transfer) {
                        Transfer transfer = (Transfer) element;
                        IndexRequest request = new IndexRequest("transfer")
                                .id(transfer.getIDTransaksi())
                                .source("namaPengirim", transfer.getPengirim().getNama(),
                                        "nomorRekeningPengirim", transfer.getPengirim().getNomorRekening(),
                                        "namaPenerima", transfer.getPenerima().getNama(),
                                        "nomorRekeningPenerima", transfer.getPenerima().getNomorRekening(),
                                        "jumlah", transfer.getJumlah(),
                                        "tanggal", transfer.getTanggal(),
                                        "jenis", transfer.getJenis(),
                                        "mataUang", transfer.getMataUang());
                        indexer.add(request);
                    }
                }).build()).name("Elasticsearch Sink");

        env.execute("Flink Ecommerce Realtime Streaming");
    }

    private static void createTotalPembelianBasedOnKategori(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS total_pembelian_by_kategori (" +
                        "kategori VARCHAR(255) PRIMARY KEY," +
                        "total DECIMAL" +
                        ")",
                (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {

                },
                jdbcExeOpt,
                jdbcConnOpt
        )).name("Create total_pembelian_by_kategori Table Sink");
    }

    private static void addPembelianByKategoriSink(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        DataStream<Pembelian> pembelianStream = transactionStream
                .filter(transaction -> transaction instanceof Pembelian)
                .map(transaction -> (Pembelian) transaction);

        pembelianStream
                .keyBy(Pembelian::getKategori)
                .process(new KeyedProcessFunction<String, Pembelian, PembelianByKategori>() {
                    private transient ValueState<BigDecimal> totalState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BigDecimal> descriptor = new ValueStateDescriptor<>(
                                "totalState",
                                Types.BIG_DEC
                        );
                        totalState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Pembelian value, Context ctx, Collector<PembelianByKategori> out) throws Exception {
                        BigDecimal currentTotal = totalState.value();
                        if (currentTotal == null) {
                            currentTotal = BigDecimal.ZERO;
                        }
                        currentTotal = currentTotal.add(value.getJumlah());
                        totalState.update(currentTotal);

                        out.collect(new PembelianByKategori(value.getDetail().getKategori(), currentTotal));
                    }
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO total_pembelian_by_kategori (kategori, total) VALUES (?, ?) " +
                                "ON CONFLICT (kategori) DO UPDATE SET total = EXCLUDED.total",
                        (JdbcStatementBuilder<PembelianByKategori>) (preparedStatement, pembelianByKategori) -> {
                            preparedStatement.setString(1, pembelianByKategori.getKategori());
                            preparedStatement.setBigDecimal(2, pembelianByKategori.getTotal());
                        },
                        jdbcExeOpt,
                        jdbcConnOpt
                )).name("Total Pembelian by Kategori Sink");
    }

    private static void addPembelianByKategoriSinkWithoutState(DataStream<Object> transactionStream, JdbcExecutionOptions jdbcExeOpt, JdbcConnectionOptions jdbcConnOpt) {
        DataStream<Pembelian> pembelianStream = transactionStream
                .filter(transaction -> transaction instanceof Pembelian)
                .map(transaction -> (Pembelian) transaction);

        pembelianStream
                .keyBy(Pembelian::getKategori)
                .reduce((p1, p2) -> new Pembelian(
                        p1.getIDTransaksi(),
                        p1.getTanggal(),
                        p1.getJenis(),
                        p1.getJumlah().add(p2.getJumlah()),
                        p1.getMataUang(),
                        p1.getMetodePembayaran(),
                        p1.getDetail()
                ))
                .map(pembelian -> new PembelianByKategori(pembelian.getDetail().getKategori(), pembelian.getJumlah()))
                .addSink(JdbcSink.sink(
                        "INSERT INTO total_pembelian_by_kategori (kategori, total) VALUES (?, ?) " +
                                "ON CONFLICT (kategori) DO UPDATE SET total = EXCLUDED.total",
                        (JdbcStatementBuilder<PembelianByKategori>) (preparedStatement, pembelianByKategori) -> {
                            preparedStatement.setString(1, pembelianByKategori.getKategori());
                            preparedStatement.setBigDecimal(2, pembelianByKategori.getTotal());

                            // Add to batch
                            preparedStatement.addBatch();
                            // Execute batch
                            preparedStatement.executeBatch();
                            },
                        jdbcExeOpt,
                        jdbcConnOpt
                )).name("Total Pembelian by Kategori Sink");
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

    private static <T> void addTransactionSink(DataStream<Object> transactionStream, Class<T> clazz, String tableName, String insertQuery, JdbcExecutionOptions jdbcExecutionOptions, JdbcConnectionOptions jdbcConnectionOptions) {
        transactionStream
                .map(transaction -> {
                    if (clazz.isInstance(transaction)) {
                        System.out.println("Received transaction " + tableName + " " + transaction);
                        return transaction;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .addSink(JdbcSink.sink(
                        insertQuery,
                        (JdbcStatementBuilder<Object>) (preparedStatement, transaction) -> {
                            if (clazz.isInstance(transaction)) {
                                setPreparedStatementParm(preparedStatement, transaction);
                            }
                        },
                        jdbcExecutionOptions,
                        jdbcConnectionOptions
                )).name(tableName + "table sink");
    }

    private static void setPreparedStatementParm(PreparedStatement preparedStatement, Object transaction) throws SQLException {
        if (transaction instanceof Pembelian) {
            Pembelian pembelian = (Pembelian) transaction;
            preparedStatement.setString(1, pembelian.getIDTransaksi());
            preparedStatement.setString(2, pembelian.getTanggal());
            preparedStatement.setString(3, pembelian.getJenis());
            preparedStatement.setBigDecimal(4, pembelian.getJumlah());
            preparedStatement.setString(5, pembelian.getMataUang());
            preparedStatement.setString(6, pembelian.getMetodePembayaran());
            preparedStatement.setString(7, pembelian.getDetail().getNamaPedagang());
            preparedStatement.setString(8, pembelian.getDetail().getKategori());
            preparedStatement.setString(9, pembelian.getDetail().getDeskripsi());
        } else if (transaction instanceof Tagihan) {
            Tagihan tagihan  = (Tagihan) transaction;
            preparedStatement.setString(1, tagihan.getIDTransaksi());
            preparedStatement.setString(2, tagihan.getTanggal());
            preparedStatement.setString(3, tagihan.getJenis());
            preparedStatement.setLong(4, tagihan.getJumlah());
            preparedStatement.setString(5, tagihan.getMataUang());
            preparedStatement.setString(6, tagihan.getPenyediaJasa());
            preparedStatement.setString(7, tagihan.getNomorPelanggan());
            preparedStatement.setString(8, tagihan.getPeriodeTagihan());
        } else if (transaction instanceof Transfer) {
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

        // Add to batch
        preparedStatement.addBatch();

        // Execute batch
        preparedStatement.executeBatch();
    }

    private static String getInsertQuery(String tableName, String[] columns) {
        StringBuilder query = new StringBuilder("INSERT INTO " + tableName + " (");
        for (String column : columns) {
            query.append(column).append(", ");
        }
        query.setLength(query.length() - 2); // Remove last comma and space
        query.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            query.append("?, ");
        }
        query.setLength(query.length() - 2); // Remove last comma and space
        query.append(") ON CONFLICT (IDTransaksi) DO UPDATE SET ");
        for (String column : columns) {
            if (!column.equals("IDTransaksi")) {
                query.append(column).append(" = EXCLUDED.").append(column).append(", ");
            }
        }
        query.setLength(query.length() - 2); // Remove last comma and space
        return query.toString();
    }
}
