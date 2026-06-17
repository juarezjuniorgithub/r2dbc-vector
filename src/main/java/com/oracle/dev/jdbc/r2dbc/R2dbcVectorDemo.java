package com.oracle.dev.jdbc.r2dbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.Consumer;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import oracle.sql.VECTOR;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class R2dbcVectorDemo {

  private static final int DEMO_ID = 1001;
  private static final float[] CREATED_VECTOR = new float[] { 1.0f, 2.0f, 3.0f };
  private static final float[] UPDATED_VECTOR = new float[] { 3.0f, 2.0f, 1.0f };

  private final ConnectionFactory connectionFactory;
  private final String tableName;

  public static void main(String[] args) {
    new R2dbcVectorDemo().runCrudScenario().block();
  }

  public R2dbcVectorDemo() {
    this.connectionFactory = DatabaseConfig.getConnectionFactory();
    this.tableName = qualifiedTableName(DatabaseConfig.getUser(), DatabaseConfig.getDbTableName());
  }

  private Mono<Void> runCrudScenario() {
    return Flux.usingWhen(connectionFactory.create(), this::runCrudScenario, Connection::close).then();
  }

  private Mono<Void> runCrudScenario(Connection connection) {
    return Mono.from(connection.beginTransaction()).then(deleteVector(connection, DEMO_ID))
        .then(createVector(connection, new VectorRecord(DEMO_ID, CREATED_VECTOR))
            .doOnNext(rowsUpdated -> printRowsUpdated("Created", DEMO_ID, rowsUpdated)))
        .then(retrieveVector(connection, DEMO_ID).doOnNext(record -> System.out.println("Retrieved: " + record)))
        .then(updateVector(connection, new VectorRecord(DEMO_ID, UPDATED_VECTOR))
            .doOnNext(rowsUpdated -> printRowsUpdated("Updated", DEMO_ID, rowsUpdated)))
        .then(retrieveVector(connection, DEMO_ID)
            .doOnNext(record -> System.out.println("Retrieved after update: " + record)))
        .then(deleteVector(connection, DEMO_ID)
            .doOnNext(rowsUpdated -> printRowsUpdated("Deleted", DEMO_ID, rowsUpdated)))
        .then(retrieveVector(connection, DEMO_ID).map(record -> "Unexpected vector after delete: " + record)
            .defaultIfEmpty("Verified delete: no vector found for id " + DEMO_ID).doOnNext(System.out::println))
        .then(Mono.from(connection.commitTransaction()))
        .onErrorResume(error -> Mono.from(connection.rollbackTransaction()).onErrorResume(rollbackError -> Mono.empty())
            .then(Mono.error(error)));
  }

  private Mono<Long> createVector(Connection connection, VectorRecord vectorRecord) {
    String sql = "INSERT INTO " + tableName + " (id, embedding) VALUES (:id, :embedding)";

    return executeUpdate(connection, sql,
        statement -> statement.bind("id", vectorRecord.id()).bind("embedding", toVector(vectorRecord.embedding())));
  }

  private Mono<VectorRecord> retrieveVector(Connection connection, int id) {
    String sql = "SELECT id, embedding FROM " + tableName + " WHERE id = :id";

    return Flux.from(connection.createStatement(sql).bind("id", id).execute())
        .flatMap(result -> result.map(
            (row, metadata) -> new VectorRecord(row.get("id", Integer.class), row.get("embedding", float[].class))))
        .singleOrEmpty();
  }

  private Mono<Long> updateVector(Connection connection, VectorRecord vectorRecord) {
    String sql = "UPDATE " + tableName + " SET embedding = :embedding WHERE id = :id";

    return executeUpdate(connection, sql,
        statement -> statement.bind("embedding", toVector(vectorRecord.embedding())).bind("id", vectorRecord.id()));
  }

  private Mono<Long> deleteVector(Connection connection, int id) {
    String sql = "DELETE FROM " + tableName + " WHERE id = :id";

    return executeUpdate(connection, sql, statement -> statement.bind("id", id));
  }

  private Mono<Long> executeUpdate(Connection connection, String sql, Consumer<Statement> bindParameters) {
    Statement statement = connection.createStatement(sql);
    bindParameters.accept(statement);

    return Flux.from(statement.execute()).flatMap(Result::getRowsUpdated).reduce(0L, Long::sum);
  }

  private static VECTOR toVector(float[] values) {
    try {
      return VECTOR.ofFloat32Values(values);
    } catch (SQLException sqlException) {
      throw new IllegalArgumentException("Unable to convert values to an Oracle FLOAT32 VECTOR", sqlException);
    }
  }

  private static String qualifiedTableName(String schema, String table) {
    return requireOracleIdentifier(schema, "schema") + "." + requireOracleIdentifier(table, "table");
  }

  private static String requireOracleIdentifier(String value, String label) {
    if (value == null || !value.matches("[A-Za-z][A-Za-z0-9_$#]*")) {
      throw new IllegalStateException("Invalid Oracle " + label + " identifier: " + value);
    }

    return value.toUpperCase(Locale.ROOT);
  }

  private static void printRowsUpdated(String action, int id, long rowsUpdated) {
    System.out.printf("%s vector id %d (%d row%s updated)%n", action, id, rowsUpdated, rowsUpdated == 1 ? "" : "s");
  }

  private record VectorRecord(int id, float[] embedding) {

    @Override
    public String toString() {
      return "ID: " + id + ", Vector: " + Arrays.toString(embedding);
    }
  }
}
