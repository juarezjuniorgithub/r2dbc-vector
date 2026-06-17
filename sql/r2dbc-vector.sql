-- Table used by com.oracle.dev.jdbc.r2dbc.R2dbcVectorDemo
-- Oracle DDL commits implicitly, so no COMMIT is needed after CREATE TABLE
CREATE TABLE VECTOR_USER.vector_data (
  id NUMBER(10) NOT NULL,
  embedding VECTOR(3, FLOAT32) NOT NULL,
  CONSTRAINT vector_data_pk PRIMARY KEY (id)
);

-- Inspect the table
-- DESCRIBE VECTOR_USER.vector_data;
-- SELECT id, embedding FROM VECTOR_USER.vector_data ORDER BY id;

-- Optional cleanup commands
-- DELETE FROM VECTOR_USER.vector_data;
-- COMMIT;

-- DROP TABLE VECTOR_USER.vector_data PURGE;
