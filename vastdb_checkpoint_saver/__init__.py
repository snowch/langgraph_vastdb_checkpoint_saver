import pyarrow as pa
import vastdb

from contextlib import asynccontextmanager, contextmanager
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Sequence
)

from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

class VastDBCheckPointSaver(BaseCheckpointSaver[str]):
    """
    A CheckpointSaver implementation that uses VastDB as the backend.
    """

    def __init__(
        self,
        endpoint: str,
        access: str,
        secret: str,
        bucket_name: str,
        schema_name: str,
        table_name: str,
        *,
        serde: Optional[SerializerProtocol] = None,
    ) -> None:
        super().__init__(serde=serde)
        self.endpoint = endpoint
        self.access = access
        self.secret = secret
        self.bucket_name = bucket_name
        self.schema_name = schema_name
        self.table_name = table_name
        self._session = vastdb.connect(
            endpoint=self.endpoint, access=self.access, secret=self.secret
        )

        with self._session.transaction() as tx:
            bucket = tx.bucket(self.bucket_name)
            if self.schema_name not in [s.name for s in bucket.schemas()]:
                schema = bucket.create_schema(self.schema_name)
            else:
                schema = bucket.schema(self.schema_name)

            if self.table_name not in [t.name for t in schema.tables()]:
                columns = pa.schema(
                    [
                        ("thread_id", pa.string()),
                        ("checkpoint_ns", pa.string()),
                        ("checkpoint_id", pa.string()),
                        ("checkpoint_data", pa.binary()),
                        ("metadata_data", pa.binary()),
                    ]
                )
                schema.create_table(self.table_name, columns)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = config["configurable"].get("checkpoint_id")

        with self._session.transaction() as tx:
            bucket = tx.bucket(self.bucket_name)
            schema = bucket.schema(self.schema_name)
            table = schema.table(self.table_name)

            if checkpoint_id:
                predicate = (
                    (table["thread_id"] == thread_id)
                    & (table["checkpoint_ns"] == checkpoint_ns)
                    & (table["checkpoint_id"] == checkpoint_id)
                )
            else:
                predicate = (table["thread_id"] == thread_id) & (
                    table["checkpoint_ns"] == checkpoint_ns
                )

                # Use internal_row_id to get the latest record
                reader = table.select(predicate=predicate, internal_row_id=True)
                result = reader.read_all()
                
                if result.num_rows > 0:
                    # Sort by row_id to get the latest record
                    rows = result.to_pylist()
                    latest_row = max(rows, key=lambda x: x["$row_id"])
                    checkpoint = self.serde.loads(latest_row["checkpoint_data"])
                    metadata = self.serde.loads(latest_row["metadata_data"])
                    return CheckpointTuple(config, checkpoint, metadata)
                else:
                    return None
            
            # If checkpoint_id is specified, we don't need to get the latest
            reader = table.select(predicate=predicate)
            result = reader.read_all()
            
            if result.num_rows > 0:
                row = result.to_pylist()[0]
                checkpoint = self.serde.loads(row["checkpoint_data"])
                metadata = self.serde.loads(row["metadata_data"])
                return CheckpointTuple(config, checkpoint, metadata)
            else:
                return None

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        if config is None:
            return iter([])

        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]

        with self._session.transaction() as tx:
            bucket = tx.bucket(self.bucket_name)
            schema = bucket.schema(self.schema_name)
            table = schema.table(self.table_name)

            predicate = (table["thread_id"] == thread_id) & (
                table["checkpoint_ns"] == checkpoint_ns
            )
            
            if filter:
                # For metadata filtering, we might need a different approach
                # This is a simplified example that assumes metadata fields are directly accessible
                # You may need to adjust this based on how metadata is actually stored
                for key, value in filter.items():
                    metadata_field = f"metadata_data.{key}"
                    predicate = predicate & (table[metadata_field] == value)

            if before:
                before_id = before["configurable"].get("checkpoint_id")
                if before_id:
                    predicate = predicate & (table["checkpoint_id"] < before_id)

            # Include internal_row_id to help with sorting
            reader = table.select(
                predicate=predicate, 
                order_by=["checkpoint_id DESC"], 
                limit=limit,
                internal_row_id=True
            )
            result = reader.read_all()

            # Convert to list to be able to sort by row_id if needed
            rows = result.to_pylist()
            
            # Optional: Sort by row_id for most recent first if checkpoint_id is not sufficient
            # rows.sort(key=lambda x: x["$row_id"], reverse=True)
            
            for row in rows:
                checkpoint = self.serde.loads(row["checkpoint_data"])
                metadata = self.serde.loads(row["metadata_data"])
                yield CheckpointTuple(config, checkpoint, metadata)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = checkpoint["id"]

        with self._session.transaction() as tx:
            bucket = tx.bucket(self.bucket_name)
            schema = bucket.schema(self.schema_name)
            table = schema.table(self.table_name)

            data = pa.table(
                [
                    [thread_id],
                    [checkpoint_ns],
                    [checkpoint_id],
                    [self.serde.dumps(checkpoint)],
                    [self.serde.dumps(metadata)],
                ],
                names=[
                    "thread_id",
                    "checkpoint_ns",
                    "checkpoint_id",
                    "checkpoint_data",
                    "metadata_data",
                ],
            )
            try:
                table.insert(data)
            except Exception as e:
                print(f"Error saving data {data}")
                raise(e)
        return config

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        # VastDB is used to save checkpoints, not individual writes.
        pass

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        return self.get_tuple(config)

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        for item in self.list(config, filter=filter, before=before, limit=limit):
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self.put(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self.put_writes(config, writes, task_id, task_path)
