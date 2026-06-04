package com.awsbench;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

//-----------------------------------------------------------
//
// BenchDoc
//
// Parses, validates, and resolves the benchmark configuration
// document (benchmarks.json) supplied to the controller via
// the -config flag.
//
// The document defines a named set of commands, a props
// dictionary for variable substitution, and the ordered
// list of commands to execute across ECS workers.
//
//-----------------------------------------------------------

class BenchDoc {

  private static final ObjectMapper MAPPER       = new ObjectMapper();
  private static final Pattern      PROP_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

  @JsonProperty("name")
  private String name;

  @JsonProperty("description")
  private String description;

  @JsonProperty("props")
  private Map<String, String> props;

  @JsonProperty("commands")
  private List<BenchCmd> commands;

  public String getName()        { return name; }
  public String getDescription() { return description; }

  // Parses the document from the given file path.
  // Exits with an error message if the file is missing or malformed.
  public static BenchDoc load(String path) {
    File file = new File(path);
    if (!file.exists()) {
      System.err.println("Benchmark document not found: " + file.getAbsolutePath());
      System.exit(1);
    }
    try {
      BenchDoc doc = MAPPER.readValue(file, BenchDoc.class);
      System.out.println("Loaded execution configuration: " + file.getAbsolutePath());
      return doc;
    } catch (IOException e) {
      System.err.println("Failed to parse benchmark document '" + path + "': " + e.getMessage());
      System.exit(1);
      return null;
    }
  }

  // Validates the document against the given worker count.
  // Returns a list of error messages; an empty list means the document is valid.
  public List<String> validate(int workerCount) {
    List<String> errors = new ArrayList<>();

    if (commands == null || commands.isEmpty()) {
      errors.add("Document contains no commands.");
      return errors;
    }

    for (BenchCmd cmd : commands) {
      // Check every ${propName} reference is defined in props.
      Matcher m = PROP_PATTERN.matcher(cmd.getCmd());
      while (m.find()) {
        String key = m.group(1);
        if (props == null || !props.containsKey(key))
          errors.add("Command " + cmd.getCmdSeq() +
              " references undefined property '${" + key + "}'.");
      }

      // Check the targets value is compatible with the available worker count.
      int count = cmd.expectedResultCount(workerCount);
      if (count <= 0 || count > workerCount)
        errors.add("Command " + cmd.getCmdSeq() + " targets " + count +
            " worker(s) but only " + workerCount + " are available.");
    }

    return errors;
  }

  // Substitutes all ${propName} references in cmd with values from props.
  // Unresolved references are left as-is.
  public String resolveCmd(String cmd) {
    if (props == null || props.isEmpty()) return cmd;
    StringBuffer sb = new StringBuffer();
    Matcher m = PROP_PATTERN.matcher(cmd);
    while (m.find()) {
      String key   = m.group(1);
      String value = props.getOrDefault(key, m.group(0));
      m.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  // Returns commands sorted by cmd_seq.
  public List<BenchCmd> getCommandsInOrder() {
    return commands.stream()
        .sorted(Comparator.comparingInt(BenchCmd::getCmdSeq))
        .collect(Collectors.toList());
  }
}
