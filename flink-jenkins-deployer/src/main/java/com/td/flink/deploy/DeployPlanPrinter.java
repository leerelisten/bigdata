package com.td.flink.deploy;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Print deployment plan from Jenkins parameters.
 * This class does not execute any deployment action.
 */
public class DeployPlanPrinter {

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();
        Map<String, String> plan = new LinkedHashMap<>();
        plan.put("timestamp", Instant.now().toString());
        plan.put("build_mode", value(env, "BUILD_MODE", "package"));
        plan.put("deploy_env", value(env, "DEPLOY_ENV", "prod"));
        plan.put("dry_run", value(env, "DRY_RUN", "true"));
        plan.put("flink_home", value(env, "FLINK_HOME", "/opt/flink/flink"));
        plan.put("job_name", value(env, "JOB_NAME_TO_DEPLOY", ""));
        plan.put("main_class", value(env, "MAIN_CLASS", "com.td.flink.job.KafkaBinlogToPaimonJob"));
        plan.put("jar_path", value(env, "JAR_PATH", "/data/project/flink-transfer-paimon/flink-transfer-paimon-1.0-SNAPSHOT.jar"));
        plan.put("app_config", value(env, "APP_CONFIG", "/data/project/flink-transfer-paimon/application.yaml"));
        plan.put("parallelism", value(env, "PARALLELISM", ""));
        plan.put("allow_non_restored_state", value(env, "ALLOW_NON_RESTORED_STATE", "false"));
        plan.put("restart_from", value(env, "RESTART_FROM", "checkpoint"));
        plan.put("checkpoint_path", value(env, "CHECKPOINT_PATH", ""));
        plan.put("savepoint_path", value(env, "SAVEPOINT_PATH", ""));
        plan.put("extra_args", value(env, "EXTRA_ARGS", ""));

        System.out.println("==== Flink Deployment Plan (No-Op) ====");
        for (Map.Entry<String, String> e : plan.entrySet()) {
            System.out.printf("%-30s : %s%n", e.getKey(), e.getValue());
        }
        System.out.println("========================================");
        System.out.println("This helper only prints plan and is safe for validation.");
    }

    private static String value(Map<String, String> env, String key, String defaultValue) {
        String v = env.get(key);
        return (v == null || v.trim().isEmpty()) ? defaultValue : v.trim();
    }
}
