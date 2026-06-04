package com.awsbench;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

//-----------------------------------------------------------
//
// BenchPayload
//
// Builds and parses the structured JSON payloads exchanged
// between the controller and workers.
//
// Command payloads (controller → workers via SNS):
//   READY_REQUEST, EXEC, STOP
//
// Result payloads (workers → controller via SQS):
//   READY, EXEC_RESULT, RESULT, ERROR
//
// All payloads carry a "type" discriminator and a "sessionId"
// that ties every message in a run together. Receivers discard
// payloads whose sessionId does not match the current session,
// preventing stale messages from a previous run being acted on.
//
//-----------------------------------------------------------

class BenchPayload {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // -------------------------------------------------------
  // Command builders (controller → workers)
  // -------------------------------------------------------

  // Initiates the session handshake. Workers respond with READY only
  // after receiving this message, ensuring no stale READY signals exist.
  public static String readyRequest(String sessionId) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "READY_REQUEST");
    m.put("sessionId", sessionId);
    m.put("timestamp", Instant.now().toString());
    return toJson(m);
  }

  public static String exec(String sessionId, String commandId, Map<String, Object> command) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "EXEC");
    m.put("sessionId", sessionId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("command",   command);
    return toJson(m);
  }

  public static String stop(String sessionId, String commandId) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "STOP");
    m.put("sessionId", sessionId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    return toJson(m);
  }

  // -------------------------------------------------------
  // Result builders (workers → controller)
  // -------------------------------------------------------

  public static String ready(String workerId, String sessionId) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "READY");
    m.put("workerId",  workerId);
    m.put("sessionId", sessionId);
    m.put("timestamp", Instant.now().toString());
    return toJson(m);
  }

  public static String execResult(String workerId, String sessionId, String commandId,
      Map<String, Object> result) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "EXEC_RESULT");
    m.put("workerId",  workerId);
    m.put("sessionId", sessionId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("result",    result);
    return toJson(m);
  }

  public static String result(String workerId, String sessionId, String commandId,
      Map<String, Object> resultData) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "RESULT");
    m.put("workerId",  workerId);
    m.put("sessionId", sessionId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("result",    resultData);
    return toJson(m);
  }

  public static String error(String workerId, String sessionId, String commandId,
      String message, String detail) {
    LinkedHashMap<String, Object> err = new LinkedHashMap<>();
    err.put("message", message);
    err.put("detail",  detail);
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    m.put("type",      "ERROR");
    m.put("workerId",  workerId);
    m.put("sessionId", sessionId);
    m.put("commandId", commandId);
    m.put("timestamp", Instant.now().toString());
    m.put("error",     err);
    return toJson(m);
  }

  // -------------------------------------------------------
  // Parsing
  // -------------------------------------------------------

  public static String getType(String json) {
    return getField(json, "type");
  }

  public static String getField(String json, String field) {
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = MAPPER.readValue(json, Map.class);
      Object val = map.get(field);
      return val != null ? val.toString() : null;
    } catch (Exception e) {
      return null;
    }
  }

  // -------------------------------------------------------
  // Helpers
  // -------------------------------------------------------

  public static String prettyPrint(String json) {
    try {
      Object obj = MAPPER.readValue(json, Object.class);
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    } catch (Exception e) {
      return json;
    }
  }

  private static String toJson(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize payload: " + e.getMessage(), e);
    }
  }
}
