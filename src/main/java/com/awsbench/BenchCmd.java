package com.awsbench;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

//-----------------------------------------------------------
//
// BenchCmd
//
// Models a single command entry in a BenchDoc.
// The "targets" field controls which workers execute the
// command: "all" (every worker), "one" (leader only),
// or an integer N (first N workers by count).
//
//-----------------------------------------------------------

class BenchCmd {

  @JsonProperty("description")
  private String description;

  @JsonProperty("cmd_seq")
  private int cmdSeq;

  @JsonProperty("cmd")
  private String cmd;

  @JsonProperty("wait")
  private boolean wait;

  @JsonProperty("targets")
  private JsonNode targets;

  public String  getDescription() { return description; }
  public int     getCmdSeq()      { return cmdSeq; }
  public String  getCmd()         { return cmd; }
  public boolean isWait()         { return wait; }

  // Returns the number of workers expected to send EXEC_RESULT for this command.
  public int expectedResultCount(int workerCount) {
    if (targets == null || (targets.isTextual() && "all".equals(targets.asText())))
      return workerCount;
    if (targets.isTextual() && "one".equals(targets.asText()))
      return 1;
    if (targets.isNumber())
      return Math.min(targets.asInt(), workerCount);
    return workerCount;
  }

  // Returns targets as a string for inclusion in the EXEC payload.
  public String getTargetsString() {
    return (targets == null) ? "all" : targets.asText();
  }
}
