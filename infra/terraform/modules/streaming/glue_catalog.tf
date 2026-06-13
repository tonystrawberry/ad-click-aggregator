# Glue Data Catalog table describing the raw click-event Parquet records
# (contracts/click-event.schema.json). Used by Firehose for format conversion
# and by the Spark reconciliation job to read raw clicks (T041).

resource "aws_glue_catalog_database" "clicks" {
  name = "${replace(var.name_prefix, "-", "_")}_clicks"
}

resource "aws_glue_catalog_table" "click_events" {
  name          = "click_events"
  database_name = aws_glue_catalog_database.clicks.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }
  partition_keys {
    name = "hr"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.raw_bucket_name}/raw/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "impression_id"
      type = "string"
    }
    columns {
      name = "ad_id"
      type = "string"
    }
    columns {
      name = "campaign_id"
      type = "string"
    }
    columns {
      name = "advertiser_id"
      type = "string"
    }
    columns {
      name = "click_ts"
      type = "string"
    }
    columns {
      name = "minute_bucket"
      type = "string"
    }
    columns {
      name = "user_agent"
      type = "string"
    }
    columns {
      name = "ingest_ts"
      type = "string"
    }
  }
}
