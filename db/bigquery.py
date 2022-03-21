from typing import Any

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

DATASET = "Facebook"


def load(
    table: str,
    schema: list[dict[str, Any]],
    id_key: list[str],
    ads_account_id: str,
):
    def _load(data: list[dict[str, Any]]) -> int:
        if len(data) == 0:
            return 0

        output_rows = (
            BQ_CLIENT.load_table_from_json(
                data,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        _update(table, id_key, ads_account_id)
        return output_rows

    return _load


def _update(table: str, id_key: list[str], ads_account_id: str):
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {DATASET}.{table}_{ads_account_id} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {','.join(id_key)}
            ORDER BY _batched_at DESC) AS row_num
        FROM {DATASET}.{table}_{ads_account_id}
    ) WHERE row_num = 1"""
    ).result()
