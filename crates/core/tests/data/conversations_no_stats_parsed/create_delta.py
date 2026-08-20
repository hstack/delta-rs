from datetime import date, timedelta
from pathlib import Path
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

TABLE_PATH = Path("delta")
START_DATE = date.fromisoformat("2024-01-01")
CHECKPOINT_DATE = date.fromisoformat("2024-01-04")  # Use None for the final version.
DAYS = 7
BATCHES_PER_DAY = 2
FILES_PER_PARTITION = 2
ROWS_PER_FILE = 2

SCHEMA = pa.schema(
    [
        pa.field(
            "_acp_system_metadata",
            pa.struct(
                [
                    pa.field("acp_sourceBatchId", pa.string()),
                    pa.field("commitBatchId", pa.string()),
                    pa.field("trackingId", pa.string()),
                    pa.field("rowId", pa.string()),
                    pa.field("rowVersion", pa.int64()),
                    pa.field(
                        "primaryIdentity",
                        pa.struct(
                            [
                                pa.field("id", pa.string()),
                                pa.field(
                                    "namespace",
                                    pa.struct([pa.field("code", pa.string())]),
                                ),
                            ]
                        ),
                    ),
                    pa.field("ingestTime", pa.int64()),
                    pa.field("isDeleted", pa.bool_()),
                ]
            ),
        ),
        pa.field(
            "agenticExperience",
            pa.struct(
                [
                    pa.field(
                        "agents",
                        pa.list_(pa.struct([pa.field("agentID", pa.string())])),
                    ),
                    pa.field("version", pa.string()),
                ]
            ),
        ),
        pa.field(
            "conversation",
            pa.struct(
                [
                    pa.field("conversationID", pa.string()),
                    pa.field("turnID", pa.string()),
                    pa.field(
                        "text",
                        pa.struct(
                            [
                                pa.field("raw", pa.string()),
                                pa.field("source", pa.string()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        pa.field(
            "identityMap",
            pa.map_(
                pa.string(),
                pa.list_(
                    pa.struct(
                        [
                            pa.field("id", pa.string()),
                            pa.field("prim", pa.bool_()),
                        ]
                    )
                ),
            ),
        ),
        pa.field("_ACP_DATE", pa.date32()),
        pa.field("_ACP_BATCHID", pa.string()),
    ]
)

shutil.rmtree(TABLE_PATH, ignore_errors=True)

commit = 0
for day_index in range(DAYS):
    current_date = START_DATE + timedelta(days=day_index)
    for batch_number in range(1, BATCHES_PER_DAY + 1):
        batch_id = f"batch-{batch_number}-{current_date.isoformat()}"
        file_batches = []

        for file_number in range(1, FILES_PER_PARTITION + 1):
            file_batch_id = f"{batch_id}--file-{file_number}"
            rows = []
            for row_number in range(1, ROWS_PER_FILE + 1):
                row_key = (
                    f"{current_date.isoformat()}-batch-{batch_number}"
                    f"-file-{file_number}-row-{row_number}"
                )
                identity = f"user-{row_key}@example.com"
                rows.append(
                    {
                        "_acp_system_metadata": {
                            "acp_sourceBatchId": batch_id,
                            "commitBatchId": f"commit-{batch_id}",
                            "trackingId": f"tracking-{row_key}",
                            "rowId": f"row-{row_key}",
                            "rowVersion": 1,
                            "primaryIdentity": {
                                "id": identity,
                                "namespace": {"code": "email"},
                            },
                            "ingestTime": int(
                                f"{current_date:%Y%m%d}"
                                f"{batch_number}{file_number}{row_number}"
                            ),
                            "isDeleted": False,
                        },
                        "agenticExperience": {
                            "agents": [{"agentID": f"agent-{batch_number}"}],
                            "version": "1.0",
                        },
                        "conversation": {
                            "conversationID": f"conversation-{current_date}-{file_number}",
                            "turnID": f"turn-{batch_number}-{file_number}-{row_number}",
                            "text": {
                                "raw": f"Conversation text for {row_key}",
                                "source": "generated",
                            },
                        },
                        "identityMap": [
                            ("email", [{"id": identity, "prim": True}])
                        ],
                        "_ACP_DATE": current_date,
                        # A temporary partition forces delta-rs to create this file.
                        "_ACP_BATCHID": file_batch_id,
                    }
                )

            file_batches.append(pa.RecordBatch.from_pylist(rows, schema=SCHEMA))

        write_deltalake(
            TABLE_PATH,
            pa.RecordBatchReader.from_batches(SCHEMA, file_batches),
            mode="overwrite" if commit == 0 else "append",
            partition_by=["_ACP_DATE", "_ACP_BATCHID"],
            configuration=(
                {
                    "delta.checkpoint.writeStatsAsJson": "true",
                    "delta.checkpoint.writeStatsAsStruct": "true",
                }
                if commit == 0
                else None
            ),
        )

        # Move both files into the real partition and normalize their add actions.
        transaction_path = TABLE_PATH / "_delta_log" / f"{commit:020}.json"
        actions = [json.loads(line) for line in transaction_path.read_text().splitlines()]
        add_actions = [action["add"] for action in actions if "add" in action]
        if len(add_actions) != FILES_PER_PARTITION:
            raise RuntimeError(
                f"Expected {FILES_PER_PARTITION} files, got {len(add_actions)}"
            )

        destination_directory = (
            TABLE_PATH
            / f"_ACP_DATE={current_date.isoformat()}"
            / f"_ACP_BATCHID={batch_id}"
        )
        destination_directory.mkdir(parents=True, exist_ok=True)
        for add in add_actions:
            source = TABLE_PATH / add["path"]
            destination = destination_directory / source.name
            source.rename(destination)
            source.parent.rmdir()
            add["path"] = destination.relative_to(TABLE_PATH).as_posix()
            add["partitionValues"]["_ACP_BATCHID"] = batch_id

        transaction_path.write_text(
            "\n".join(json.dumps(action, separators=(",", ":")) for action in actions)
            + "\n"
        )
        commit += 1

    if CHECKPOINT_DATE == current_date:
        DeltaTable(TABLE_PATH).create_checkpoint()

if CHECKPOINT_DATE is None:
    DeltaTable(TABLE_PATH).create_checkpoint()

# Ensure this checkpoint has only raw stats and raw partition values.
checkpoint_path = next((TABLE_PATH / "_delta_log").glob("*.checkpoint.parquet"))
checkpoint = pq.read_table(checkpoint_path)
add_index = checkpoint.schema.get_field_index("add")
add_field = checkpoint.schema.field(add_index)
adds = checkpoint.column(add_index).to_pylist()

for add in adds:
    if add is None:
        continue
    add.pop("stats_parsed", None)
    add.pop("partitionValues_parsed", None)

original_add_type = add_field.type
patched_add_type = pa.struct(
    [
        field
        for field in original_add_type
        if field.name not in {"stats_parsed", "partitionValues_parsed"}
    ]
)
patched_add_field = pa.field(
    "add",
    patched_add_type,
    nullable=add_field.nullable,
    metadata=add_field.metadata,
)
checkpoint = checkpoint.set_column(
    add_index,
    patched_add_field,
    pa.array(adds, type=patched_add_type),
)
pq.write_table(checkpoint, checkpoint_path, compression="snappy")

last_checkpoint_path = TABLE_PATH / "_delta_log" / "_last_checkpoint"
last_checkpoint = json.loads(last_checkpoint_path.read_text())
last_checkpoint["sizeInBytes"] = checkpoint_path.stat().st_size
last_checkpoint_path.write_text(json.dumps(last_checkpoint, separators=(",", ":")))

print(
    f"Created {TABLE_PATH} with {commit} versions and a raw-only checkpoint"
)
