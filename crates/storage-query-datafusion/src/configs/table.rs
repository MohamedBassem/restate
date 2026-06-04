// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use restate_types::config::Configuration;
use tokio::sync::mpsc::Sender;

use crate::context::QueryContext;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

use super::row::append_config_row;
use super::schema::ConfigsBuilder;

pub fn register_self(
    ctx: &QueryContext,
    config: restate_types::live::Live<Configuration>,
) -> datafusion::common::Result<()> {
    let config_table = GenericTableProvider::new(
        ConfigsBuilder::schema(),
        Arc::new(ConfigsScanner { config }),
    );
    ctx.register_non_partitioned_table("configs", Arc::new(config_table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("ConfigsScanner")]
struct ConfigsScanner {
    config: restate_types::live::Live<Configuration>,
}

impl Scan for ConfigsScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 2);
        let tx = stream_builder.tx();

        let config = self.config.snapshot();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, config, batch_size).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    config: Arc<Configuration>,
    batch_size: usize,
) {
    let mut builder = ConfigsBuilder::new(schema.clone());
    let json_value = serde_json::to_value(config).unwrap();

    let mut res = vec![];
    flatten_json(&json_value, "".to_string(), &mut res);

    for (k, v) in res.iter() {
        append_config_row(&mut builder, k, v);

        if builder.num_rows() >= batch_size {
            let batch = builder.finish_and_new();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}

fn flatten_json(val: &serde_json::Value, parent: String, res: &mut Vec<(String, String)>) {
    match val {
        serde_json::Value::Object(v) => {
            for (k, v) in v {
                flatten_json(
                    v,
                    if !parent.is_empty() {
                        format!("{}.{}", parent, k)
                    } else {
                        k.to_string()
                    },
                    res,
                );
            }
        }
        _ => {
            res.push((parent, serde_json::to_string(val).unwrap()));
        }
    }
}
