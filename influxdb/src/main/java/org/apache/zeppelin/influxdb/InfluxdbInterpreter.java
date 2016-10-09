/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.influxdb;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Influxdb interpreter (https://www.influxdata.com/time-series-platform/influxdb/).
 * <p>
 * Use the following properties for interpreter configuration:
 * <p>
 * <ul>
 * <li>{@code Influxdb.addresses} - coma separated list of hosts in form {@code <host>:<port>}
 * or {@code <host>:<port_1>..<port_n>} </li>
 * </ul>
 */
public class InfluxdbInterpreter extends Interpreter {
  private static Logger logger = LoggerFactory.getLogger(InfluxdbInterpreter.class);

  protected static final List<String> COMMANDS = Arrays.asList(
      "ALL", "ALTER", "ANY", "AS", "ASC", "BEGIN", "BY", "CREATE", "CONTINUOUS", "DATABASE",
      "DATABASES", "DEFAULT", "DELETE", "DESC", "DESTINATIONS", "DIAGNOSTICS", "DISTINCT", "DROP",
      "DURATION", "END", "EVERY", "EXPLAIN", "FIELD", "FOR", "FROM", "GRANT", "GRANTS", "GROUP",
      "GROUPS", "IN", "INF", "INSERT", "INTO", "KEY", "KEYS", "KILL", "LIMIT", "SHOW",
      "MEASUREMENT", "MEASUREMENTS", "NAME", "OFFSET", "ON", "ORDER", "PASSWORD", "POLICY",
      "POLICIES", "PRIVILEGES", "QUERIES", "QUERY", "READ", "REPLICATION", "RESAMPLE", "RETENTION",
      "REVOKE", "SELECT", "SERIES", "SET", "SHARD", "SHARDS", "SLIMIT", "SOFFSET", "STATS",
      "SUBSCRIPTION", "SUBSCRIPTIONS", "TAG", "TO", "USER", "USERS", "VALUES", "WHERE", "WITH",
      "WRITE");

  public static final String INFLUXDB_HOST = "influxdb.host";
  public static final String INFLUXDB_PORT = "influxdb.port";
  public static final String INFLUXDB_USERNAME = "influxdb.username";
  public static final String INFLUXDB_PASSWORD = "influxdb.password";

  public static final String INFLUXDB_DBNAME = "influxdb.dbname";

  private String host = "localhost";
  private int port = 8086;
  private String username = "root";
  private String password = "root";
  private String dbname = "default";

  private InfluxDB influxdbClient;

  public InfluxdbInterpreter(Properties property) {
    super(property);
    this.host = getProperty(INFLUXDB_HOST);
    this.port = Integer.parseInt(getProperty(INFLUXDB_PORT));
    this.username = getProperty(INFLUXDB_USERNAME);
    this.password = getProperty(INFLUXDB_PASSWORD);

    this.dbname = getProperty(INFLUXDB_DBNAME);
  }

  @Override
  public void open() {
    this.influxdbClient = InfluxDBFactory.connect(this.host + ":" + this.port,
        this.username, this.password);
  }

  @Override
  public InterpreterResult interpret(String influxdbQuery, InterpreterContext context) {
    //prepare query
    Query query = new Query(influxdbQuery, this.dbname);
    //run it
    QueryResult queryRes = influxdbClient.query(query);
    if (queryRes == null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, "Query result is empty.");
    }
    if (queryRes.hasError()) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, queryRes.getError());
    }
    //prepare result
    return new InterpreterResult(
        InterpreterResult.Code.SUCCESS,
        InterpreterResult.Type.TEXT,
        "" + queryRes);
  }

  @Override
  public void close() {
    //nothing to do really
  }

  @Override
  public void cancel(InterpreterContext context) {
    //nothing to do really
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String s, int cursor) {
    final List<InterpreterCompletion> suggestions = new ArrayList<InterpreterCompletion>();

    for (String cmd : COMMANDS) {
      if (cmd.toUpperCase().contains(s)) {
        suggestions.add(new InterpreterCompletion(cmd, cmd));
      }
    }
    return suggestions;
  }
}
