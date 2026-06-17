package com.oracle.dev.jdbc.r2dbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;

public class DatabaseConfig {

  private static final Properties CONFIG = new Properties();

  static {
    try (InputStream fileStream = openConfig()) {
      CONFIG.load(fileStream);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static InputStream openConfig() throws IOException {
    InputStream classpathConfig = DatabaseConfig.class.getClassLoader()
        .getResourceAsStream("config.properties");

    if (classpathConfig != null) {
      return classpathConfig;
    }

    Path sourceConfig = Path.of("src/main/resources/config.properties");
    return Files.newInputStream(sourceConfig);
  }

  private static final String DRIVER = CONFIG.getProperty("DRIVER");

  private static final String USER = CONFIG.getProperty("USER");

  private static final String PASSWORD = CONFIG.getProperty("PASSWORD");

  private static final String HOST = CONFIG.getProperty("HOST");

  private static final String PORT = CONFIG.getProperty("PORT");

  private static final String DATABASE = CONFIG.getProperty("DATABASE");

  private static final String DB_TABLE_NAME = CONFIG.getProperty("DB_TABLE_NAME");

  public static ConnectionFactory getConnectionFactory() {

    char[] password = new String(DatabaseConfig.getPassword()).toCharArray();
    try {
      ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, DatabaseConfig.getDriver())
          .option(ConnectionFactoryOptions.USER, DatabaseConfig.getUser())
          .option(ConnectionFactoryOptions.PASSWORD, CharBuffer.wrap(password))
          .option(ConnectionFactoryOptions.HOST, DatabaseConfig.getHost())
          .option(ConnectionFactoryOptions.PORT, DatabaseConfig.getPort())
          .option(ConnectionFactoryOptions.DATABASE, DatabaseConfig.getDatabase()).build();
      return ConnectionFactories.get(options);

    } finally {
      Arrays.fill(password, (char) 0);
    }
  }

  public static String getDbTableName() {
    return Objects.requireNonNull(DB_TABLE_NAME, "DB_TABLE_NAME must be configured");
  }

  public static String getDriver() {
    return Objects.requireNonNull(DRIVER, "DRIVER must be configured");
  }

  public static String getUser() {
    return Objects.requireNonNull(USER, "USER must be configured");
  }

  public static String getPassword() {
    return Objects.requireNonNull(PASSWORD, "PASSWORD must be configured");
  }

  public static String getHost() {
    return Objects.requireNonNull(HOST, "HOST must be configured");
  }

  public static int getPort() {
    return Integer.parseInt(Objects.requireNonNull(PORT, "PORT must be configured"));
  }

  public static String getDatabase() {
    return Objects.requireNonNull(DATABASE, "DATABASE must be configured");
  }

}
