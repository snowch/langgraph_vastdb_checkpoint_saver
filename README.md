# Langgraph VastDB Checkpoint Saver

## Installation

To install the package, use the following command:

```bash
pip3 install --upgrade --quiet git+https://github.com/snowch/langgraph_vastdb_checkpoint_saver.git --use-pep517
```

## Overview

The `VastDBCheckPointSaver` class is a CheckpointSaver implementation that uses VastDB as the backend. This class allows you to save, retrieve, and list checkpoints in a VastDB database.

### Class: `VastDBCheckPointSaver`

#### Constructor

```python
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
) -> None
```

- **Parameters:**
  - `endpoint`: The endpoint URL for the VastDB service.
  - `access`: The access key for the VastDB service.
  - `secret`: The secret key for the VastDB service.
  - `bucket_name`: The name of the bucket to use in VastDB.
  - `schema_name`: The name of the schema to use in the bucket.
  - `table_name`: The name of the table to use in the schema.
  - `serde`: (Optional) A serializer protocol for serializing and deserializing checkpoint data.

#### Methods

- `get_tuple(config: RunnableConfig) -> Optional[CheckpointTuple]`
  - Retrieves a checkpoint tuple based on the provided configuration.

- `list(config: Optional[RunnableConfig], *, filter: Optional[Dict[str, Any]] = None, before: Optional[RunnableConfig] = None, limit: Optional[int] = None) -> Iterator[CheckpointTuple]`
  - Lists checkpoint tuples based on the provided configuration and optional filters.

- `put(config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig`
  - Saves a checkpoint tuple to the VastDB table.

- `put_writes(config: RunnableConfig, writes: Sequence[Tuple[str, Any]], task_id: str, task_path: str = "") -> None`
  - This method is not used in this implementation.

- `aget_tuple(config: RunnableConfig) -> Optional[CheckpointTuple]`
  - Asynchronous version of `get_tuple`.

- `alist(config: Optional[RunnableConfig], *, filter: Optional[Dict[str, Any]] = None, before: Optional[RunnableConfig] = None, limit: Optional[int] = None) -> AsyncIterator[CheckpointTuple]`
  - Asynchronous version of `list`.

- `aput(config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata, new_versions: ChannelVersions) -> RunnableConfig`
  - Asynchronous version of `put`.

- `aput_writes(config: RunnableConfig, writes: Sequence[Tuple[str, Any]], task_id: str, task_path: str = "") -> None`
  - Asynchronous version of `put_writes`.

## Example

```python
from vastdb_checkpoint_saver import VastDBCheckPointSaver

saver = VastDBCheckPointSaver(
    endpoint="https://vastdb.example.com",
    access="your_access_key",
    secret="your_secret_key",
    bucket_name="your_bucket_name",
    schema_name="your_schema_name",
    table_name="your_table_name"
)

config = {
    "configurable": {
        "thread_id": "thread_1",
        "checkpoint_ns": "namespace_1"
    }
}

# Save a checkpoint
saver.put(config, checkpoint={"id": "checkpoint_1"}, metadata={}, new_versions={})

# Retrieve the checkpoint
checkpoint_tuple = saver.get_tuple(config)
print(checkpoint_tuple)
```
