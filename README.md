# Langgraph VastDB Checkpoint Saver

## Installation

To install the package, use the following command:

```bash
pip3 install --upgrade langraph
pip3 uninstall -y vastdb_checkpoint_saver # --upgrade doesn't work with git+https?
pip3 install --quiet git+https://github.com/snowch/langgraph_vastdb_checkpoint_saver.git --use-pep517
```

##  Caveats

- This is an early prototype - bugs will exist!
- Multiple versions of records can exist.  Row_id is used to select the latest update.
- Logic should be reviewed to verify row_id approach, and tests implemented

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
