# flink-jenkins-deployer

用于 Jenkins WebUI 的 Flink 部署辅助工程。默认 `DRY_RUN=true`，只打印命令，不会停止或拉起线上任务。

## 目录

- `Jenkinsfile`: Jenkins Pipeline 定义（参数化）
- `scripts/flink_deploy.sh`: 部署脚本（支持 checkpoint/savepoint 恢复）
- `src/main/java/com/td/flink/deploy/DeployPlanPrinter.java`: 打印部署计划（no-op）

## 关键参数

- `DRY_RUN`: `true`（默认）仅预演；`false` 才执行命令
- `STOP_MODE`: `none|cancel|savepoint`
- `TARGET_JOB_ID`: 明确指定停哪个任务
- `JOB_NAME_TO_DEPLOY`: 若未填 `TARGET_JOB_ID`，按任务名自动匹配
- `RESTART_FROM`: `checkpoint|savepoint|none`
- `CHECKPOINT_PATH` / `SAVEPOINT_PATH`: 恢复路径
- `MAIN_CLASS`: 主类
- `JAR_PATH`: jar 路径
- `APP_CONFIG`: 任务配置文件参数

## 安全建议

1. 首次运行保持 `DRY_RUN=true`，确认输出命令无误。
2. 只在确认窗口期后，将 `DRY_RUN=false`。
3. 建议把 `TARGET_JOB_ID` 明确写死，避免按名字匹配错停。
4. 建议在 Jenkins 中把生产部署步骤加 `input` 人工确认。

## 本项目范围

本项目只提供构建与部署模板，不会自动变更现网任务。
