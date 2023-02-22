// This file can be deleted when everything has been switched over to the RPC write path.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::Duration;

#[test]
fn querier_errors_with_mode_env_var_and_shard_to_ingester_mapping() {
    let shard_to_ingesters_json = r#"{
          "ingesters": {
            "i1": {
              "addr": "arbitrary"
            }
          },
          "shards": {
            "0": {
              "ingester": "i1"
            }
        }
    }"#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("querier")
        .arg("--shard-to-ingesters")
        .arg(shard_to_ingesters_json)
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` is set but shard to ingester mappings were provided",
        ));
}

#[test]
fn querier_errors_without_mode_env_var_and_ingester_addresses() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("querier")
        .arg("--ingester-addresses")
        .arg("http://arbitrary:8082")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "`INFLUXDB_IOX_RPC_MODE` is unset but ingester addresses were provided",
        ));
}

#[test]
fn querier_without_ingesters_without_mode_env_var_uses_write_buffer() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .arg("run")
        .arg("querier")
        .arg("-v")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("using the write buffer path"));
}

#[test]
fn querier_without_ingesters_with_mode_env_var_uses_rpc_write() {
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .env_clear()
        .env("INFLUXDB_IOX_RPC_MODE", "2")
        .arg("run")
        .arg("querier")
        .arg("-v")
        .arg("--catalog")
        .arg("memory")
        .timeout(Duration::from_secs(2))
        .assert()
        .failure()
        .stdout(predicate::str::contains("using the RPC write path"));
}
