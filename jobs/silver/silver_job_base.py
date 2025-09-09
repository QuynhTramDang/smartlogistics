# silver_job_base.py
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name, col

from silver.silver_utils import (
    init_spark, register_hive_table, validate_and_clean,
    apply_and_format_data_quality, append_fact_table,
    upsert_scd2_table, write_rejects, log_batch
)

logger = logging.getLogger("SilverJobBase")


class BaseSilverJob:
    def __init__(self, config: dict, batch_id: str = None):
        self.config = config
        self.batch_id = batch_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.job_name = config["job_name"]
        metastore_uri = config.get("metastore_uri", "thrift://delta-metastore:9083")
        self.spark: SparkSession = init_spark(
            app_name=f"Silver_{self.job_name}",
            minio_endpoint=config["minio"]["endpoint"],
            access_key=config["minio"]["access_key"],
            secret_key=config["minio"]["secret_key"],
            metastore_uri=metastore_uri,   # <- pass metastore here
        )

        self.raw_df: DataFrame = None
        self.valid_df: DataFrame = None
        self.reject_df: DataFrame = None

    def run(self):
        try:
            logger.info(f"🚀 Start job: {self.job_name} | batch_id={self.batch_id}")
            self.read_raw_data()
            self.flatten_raw()
            self.validate_required()
            self.apply_data_quality()
            self.write_target_table()
            self.register_table()
            self.write_rejects()
            self.log_success()
            logger.info(f"✅ Job complete: {self.job_name}")
        except Exception as e:
            logger.error(f"❌ Job failed: {e}", exc_info=True)
            try:
                self.log_failure()
            except Exception as lf_e:
                logger.error(f"Failed to write failure batch log: {lf_e}")
            raise
        finally:
            self.spark.stop()

    def read_raw_data(self):
        logger.info(f"📥 Reading raw data from: {self.config['raw_path']}")
        self.raw_df = self.spark.read.schema(self.config["schema"]) \
            .json(self.config["raw_path"]) \
            .withColumn("filename", input_file_name())

    def flatten_raw(self):
        flatten_expr = self.config.get("flatten_expr", [])
        if flatten_expr:
            self.raw_df = self.raw_df.selectExpr(flatten_expr).select("record.*")
            logger.info(f"📄 Flattened columns: {self.raw_df.columns}")
        else:
            logger.warning("⚠ No flatten expression provided, skipping flatten.")

    def validate_required(self):
        required_cols = self.config.get("required_columns", [])
        self.valid_df, self.reject_df = validate_and_clean(
            df=self.raw_df,
            required_cols=required_cols,
            reject_reason="missing_required"
        )
        valid_count = self.valid_df.count()
        reject_count = self.reject_df.count()
        logger.info(f"✅ Required validation passed: {valid_count} rows")
        logger.info(f"❌ Rejected rows (required): {reject_count} rows")

    def apply_data_quality(self):
        dq_rules = self.config.get("data_quality_rules", {})
        if dq_rules:
            self.valid_df, dq_reject_df = apply_and_format_data_quality(
                df=self.valid_df,
                rules=dq_rules,
                reject_reason_prefix="dq"
            )
            self.reject_df = self.reject_df.unionByName(dq_reject_df)
            valid_count = self.valid_df.count()
            dq_rejected = dq_reject_df.count()
            logger.info(f"🧪 Data quality applied: valid={valid_count}, dq_rejected={dq_rejected}")
        else:
            logger.info("ℹ No data quality rules defined.")

    def register_table(self):
        register_hive_table(
            self.spark,
            f"smartlogistics.{self.job_name}",
            self.config["delta_path"]
        )

    def write_rejects(self):
        write_rejects(
            df=self.reject_df,
            path=self.config["reject_path"],
            batch_id=self.batch_id,
            job_name=self.job_name
        )

    def log_success(self):
        try:
            log_batch(
                spark=self.spark,
                batch_id=self.batch_id,
                record_count=self.valid_df.count(),
                rejected_count=self.reject_df.count(),
                status="success",
                log_path=self.config["log_path"],
                job_name=self.job_name
            )
        except Exception as e:
            logger.error(f"Failed to write success batch log: {e}")

    def log_failure(self):
        try:
            log_batch(
                spark=self.spark,
                batch_id=self.batch_id,
                record_count=0,
                rejected_count=0,
                status="failed",
                log_path=self.config["log_path"],
                job_name=self.job_name
            )
        except Exception as e:
            logger.error(f"Failed to write failure batch log: {e}")

    # ➤ Abstract method cần override
    def write_target_table(self):
        raise NotImplementedError("Subclasses must implement write_target_table()")


class DimSilverJob(BaseSilverJob):
    def write_target_table(self):
        logger.info("💾 Writing to DIM (SCD2) table")
        upsert_scd2_table(
            spark=self.spark,
            df=self.valid_df,
            path=self.config["delta_path"],
            key_cols=self.config.get("key_columns", []),
            scd_cols=self.config.get("scd_columns", []),
            batch_id=self.batch_id,
            job_name=self.job_name,
            partition_by=self.config.get("partition_by", [])
        )


class FactSilverJob(BaseSilverJob):
    def write_target_table(self):
        logger.info("💾 Writing to FACT (append) table")
        append_fact_table(
            df=self.valid_df,
            path=self.config["delta_path"],
            batch_id=self.batch_id,
            job_name=self.job_name,
            partition_by=self.config.get("partition_by", [])
        )
