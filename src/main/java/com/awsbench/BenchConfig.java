package com.awsbench;

import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//-----------------------------------------------------------
//
// BenchConfig
//
// Loads and validates the benchman.properties file.
// Call load() once at startup before accessing any property.
// All required keys are checked upfront; the program exits
// with a clear error if the file or any key is missing.
//
//-----------------------------------------------------------

class BenchConfig {

  private static final String[] REQUIRED_KEYS = {
    "benchman.region",
    "benchman.command.topic.name",
    "benchman.results.queue.name",
    "benchman.cluster.name",
    "benchman.task.definition",
    "benchman.subnet.id",
    "benchman.sqs.poll.wait.sec",
    "benchman.sqs.max.messages",
    "benchman.worker.ready.timeout.sec",
    "benchman.result.wait.timeout.sec",
    "benchman.worker.start.timeout.sec",
    "benchman.worker.idle.timeout.sec",
  };

  private static Properties props = null;

  // Loads properties from the given path, or from benchman.properties in the
  // working directory if path is null. Exits on missing file or missing keys.
  public static void load(String path) {
    File file = (path != null) ? new File(path) : new File("benchman.properties");
    if (!file.exists()) {
      if (path != null)
        System.err.println("Properties file not found: " + file.getAbsolutePath());
      else
        System.err.println("Properties file not found: " + file.getAbsolutePath() +
            "\nUse -props <file> to specify a different location.");
      System.exit(1);
    }

    props = new Properties();
    try (FileInputStream fis = new FileInputStream(file)) {
      props.load(fis);
    } catch (IOException e) {
      System.err.println("Failed to read properties file '" + file.getAbsolutePath() +
          "': " + e.getMessage());
      System.exit(1);
    }

    validate();
    System.out.println("Loaded configuration from: " + file.getAbsolutePath());
  }

  // Checks all required keys are present and reports every missing one before exiting.
  private static void validate() {
    List<String> missing = new ArrayList<>();
    for (String key : REQUIRED_KEYS) {
      if (props.getProperty(key) == null)
        missing.add(key);
    }
    if (!missing.isEmpty()) {
      System.err.println("Missing required properties:");
      missing.forEach(k -> System.err.println("  " + k));
      System.exit(1);
    }
  }

  public static String getString(String key) {
    return props.getProperty(key).trim();
  }

  public static int getInt(String key) {
    String val = getString(key);
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      System.err.println("Property '" + key + "' must be an integer, got: '" + val + "'");
      System.exit(1);
      return 0;
    }
  }

  public static Region getRegion(String key) {
    return Region.of(getString(key));
  }
}
