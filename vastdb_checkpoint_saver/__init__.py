import pyarrow as pa
import vastdb

class VastDBCheckPointSaver(BaseCheckpointSaver[int]):
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
                        ("checkpoint_data", pa.string()),
                        ("metadata_data", pa.string()),
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
                    (table.thread_id == thread_id)
                    & (table.checkpoint_ns == checkpoint_ns)
                    & (table.checkpoint_id == checkpoint_id)
                )
            else:
                predicate = (table.thread_id == thread_id) & (
                    table.checkpoint_ns == checkpoint_ns
                )

            reader = table.select(predicate=predicate, order_by=["checkpoint_id DESC"], limit=1)

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

            predicate = (table.thread_id == thread_id) & (
                table.checkpoint_ns == checkpoint_ns
            )
            if filter:
                for key, value in filter.items():
                    predicate = predicate & (getattr(table, f"metadata_data.{key}") == value)

            if before:
                before_id = before["configurable"].get("checkpoint_id")
                if before_id:
                    predicate = predicate & (table.checkpoint_id < before_id)

            reader = table.select(predicate=predicate, order_by=["checkpoint_id DESC"], limit=limit)
            result = reader.read_all()

            for row in result.to_pylist():
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
            table.insert(data)
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
