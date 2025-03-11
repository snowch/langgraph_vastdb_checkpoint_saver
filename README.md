# Langgraph VastDB Checkpoint Saver

## Installation

To install the package, use the following command:

```bash
pip3 install --upgrade --quiet git+https://github.com/snowch/langgraph_vastdb_checkpoint_saver.git --use-pep517
```

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
