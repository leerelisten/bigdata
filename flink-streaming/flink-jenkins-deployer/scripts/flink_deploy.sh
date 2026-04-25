#!/usr/bin/env bash
set -euo pipefail

DRY_RUN="${DRY_RUN:-true}"
BUILD_MODE="${BUILD_MODE:-package}"
FLINK_HOME="${FLINK_HOME:-/opt/flink/flink}"
MAVEN_CMD="${MAVEN_CMD:-mvn}"
STOP_MODE="${STOP_MODE:-cancel}" # cancel | savepoint | none
SAVEPOINT_DIR="${SAVEPOINT_DIR:-}"
TARGET_JOB_ID="${TARGET_JOB_ID:-}"
JOB_NAME_TO_DEPLOY="${JOB_NAME_TO_DEPLOY:-}"
MAIN_CLASS="${MAIN_CLASS:-}"
JAR_PATH="${JAR_PATH:-}"
APP_CONFIG="${APP_CONFIG:-}"
PARALLELISM="${PARALLELISM:-}"
ALLOW_NON_RESTORED_STATE="${ALLOW_NON_RESTORED_STATE:-false}"
RESTART_FROM="${RESTART_FROM:-checkpoint}" # checkpoint | savepoint | none
CHECKPOINT_PATH="${CHECKPOINT_PATH:-}"
SAVEPOINT_PATH="${SAVEPOINT_PATH:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

print_cmd() {
  printf 'CMD: '
  printf '%q ' "$@"
  printf '\n'
}

run_cmd() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    print_cmd "$@"
  else
    print_cmd "$@"
    "$@"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    log "missing command: $1"
    exit 1
  }
}

resolve_job_id_by_name() {
  local job_name="$1"
  local line
  line="$(${FLINK_HOME}/bin/flink list -r 2>/dev/null | awk -v n="$job_name" '$0 ~ n {print; exit}')"
  if [[ -z "$line" ]]; then
    return 0
  fi
  echo "$line" | sed -E 's/.* : ([0-9a-f]{32}) : .*/\1/'
}

build_project() {
  if [[ "${BUILD_MODE}" != "package" ]]; then
    log "skip build because BUILD_MODE=${BUILD_MODE}"
    return
  fi
  run_cmd "${MAVEN_CMD}" -U -DskipTests clean package
}

stop_existing_job() {
  if [[ -z "${TARGET_JOB_ID}" ]]; then
    if [[ -n "${JOB_NAME_TO_DEPLOY}" ]]; then
      TARGET_JOB_ID="$(resolve_job_id_by_name "${JOB_NAME_TO_DEPLOY}")"
      if [[ -n "${TARGET_JOB_ID}" ]]; then
        log "resolved job id by name: ${JOB_NAME_TO_DEPLOY} -> ${TARGET_JOB_ID}"
      fi
    fi
  fi

  if [[ -z "${TARGET_JOB_ID}" || "${STOP_MODE}" == "none" ]]; then
    log "skip stop step"
    return
  fi

  if [[ "${STOP_MODE}" == "savepoint" ]]; then
    if [[ -n "${SAVEPOINT_DIR}" ]]; then
      run_cmd "${FLINK_HOME}/bin/flink" stop --savepointPath "${SAVEPOINT_DIR}" "${TARGET_JOB_ID}"
    else
      run_cmd "${FLINK_HOME}/bin/flink" stop "${TARGET_JOB_ID}"
    fi
  else
    run_cmd "${FLINK_HOME}/bin/flink" cancel "${TARGET_JOB_ID}"
  fi
}

submit_job() {
  if [[ -z "${JAR_PATH}" ]]; then
    log "JAR_PATH is required"
    exit 1
  fi

  local -a cmd
  cmd=("${FLINK_HOME}/bin/flink" run -d)

  if [[ -n "${PARALLELISM}" ]]; then
    cmd+=( -p "${PARALLELISM}" )
  fi

  # Flink CLI uses -s/--fromSavepoint to restore state. In practice,
  # many deployments also pass a retained checkpoint metadata path here.
  if [[ "${RESTART_FROM}" == "checkpoint" && -n "${CHECKPOINT_PATH}" ]]; then
    cmd+=( -s "${CHECKPOINT_PATH}" )
  elif [[ "${RESTART_FROM}" == "savepoint" && -n "${SAVEPOINT_PATH}" ]]; then
    cmd+=( -s "${SAVEPOINT_PATH}" )
  fi

  if [[ "${ALLOW_NON_RESTORED_STATE}" == "true" ]]; then
    cmd+=( -n )
  fi

  if [[ -n "${MAIN_CLASS}" ]]; then
    cmd+=( -c "${MAIN_CLASS}" )
  fi

  cmd+=( "${JAR_PATH}" )

  if [[ -n "${APP_CONFIG}" ]]; then
    cmd+=( "${APP_CONFIG}" )
  fi

  if [[ -n "${EXTRA_ARGS}" ]]; then
    # shellcheck disable=SC2206
    local extra=( ${EXTRA_ARGS} )
    cmd+=( "${extra[@]}" )
  fi

  run_cmd "${cmd[@]}"
}

main() {
  require_cmd date
  require_cmd sed
  require_cmd awk

  if [[ ! -x "${FLINK_HOME}/bin/flink" ]]; then
    log "flink binary not found: ${FLINK_HOME}/bin/flink"
    exit 1
  fi

  log "DRY_RUN=${DRY_RUN}, this script will not touch online jobs unless DRY_RUN=false"
  build_project
  stop_existing_job
  submit_job
  log "done"
}

main "$@"
