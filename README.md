# Dynamodx

A developer-friendly library for DynamoDB that simplifies single-table design without ORM lock-in.

## Features

-   **Type-safe primary keys** with `PrimaryKey` and `SortKey`
-   **Model mapping** for Pydantic, dataclasses, or plain classes
-   **Transactional reads** with `transact_get()`
-   **Transactional writes** with `transact_writer()`
-   **Custom exceptions** for conditional operations

## Installation

```bash
pip install dynamodx
```

## Quick Start

### Basic Item Operations

```python
from dynamodx.client import DynamoDBClient
from dynamodx.keys import PrimaryKey, SortKey

client = DynamoDBClient(table_name='your-table', client=boto3_client)

# Get a full item
item = client.get_item(
    PrimaryKey(id='USER#123', sk='0')
)

# Get a specific attribute
name = client.get_item(
    PrimaryKey(
        id='USER#123',
        sk=SortKey(sk='0', path_spec='name')
    )
)

# Get with custom exception on missing
try:
    client.get_item(
        PrimaryKey(id='USER#123', sk='0'),
        exc_cls=UserNotFoundError
    )
except UserNotFoundError:
    pass
```

### Model Mapping

Works with **Pydantic**, **dataclasses**, or plain classes:

#### Pydantic (model_config style)

```python
from pydantic import BaseModel, ConfigDict
from dynamodx.client import DynamoDBClient

class User(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,  # Pydantic settings
        table='users',              # DynamoDB settings
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str

client = DynamoDBClient(table_name='users')
user = User(id='123', sk='0', name='Aragorn')
client.put_item(user)
```

#### Dataclass

```python
from dataclasses import dataclass
from dynamodx.client import ConfigDict

@dataclass
class User:
    __dynamodb_config__ = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str

client.put_item(User(id='123', sk='0', name='Legolas'))
```

### Transactional Reads

Fetch multiple items in a single transaction:

```python
from dynamodx.keys import PrimaryKey, SortKey

output = client.transact_get().get_items(
    PrimaryKey(
        id='PASSWORD_RESET',
        sk=SortKey(
            sk='USER#f841e66c',
            rename_key='reset_code',
            path_spec='code',
        ),
    )
    + PrimaryKey(
        id='USER#f841e66c',
        sk=SortKey(sk='0', rename_key='user'),
    ),
    flatten_top=False,
)

# Returns:
# {
#     'reset_code': 'RoM1e1m4...',
#     'user': {'id': 'USER#...', 'name': 'Legolas'}
# }
```

### Transactional Writes with Conditional Operations

```python
from dynamodx.transact_writer import TransactWriter, TransactionOperationFailed

class EmailConflictError(TransactionOperationFailed):
    pass

try:
    with TransactWriter(table_name='users', client=boto3_client) as tx:
        tx.put(item={'id': user_id, 'sk': '0', 'name': 'Bilbo'})

        tx.put(
            item={'id': 'EMAIL', 'sk': 'bilbo@bag.com'},
            cond_expr='attribute_not_exists(sk)',
            exc_cls=EmailConflictError,
            return_on_cond_fail='ALL_OLD',
        )

except EmailConflictError as err:
    existing_user_id = err.reason['old_item']['user_id']
```

### Multiple Failures with fail_fast Disabled

```python
from dynamodx.transact_writer import TransactionCanceledException

try:
    with TransactWriter(table_name='users', fail_fast=False) as tx:
        tx.put(
            item={'id': 'EMAIL', 'sk': 'bilbo@bag.com'},
            cond_expr='attribute_not_exists(sk)',
            exc_cls=EmailConflictError,
        )
        tx.put(
            item={'id': 'USERNAME', 'sk': 'bilbo'},
            cond_expr='attribute_not_exists(sk)',
            exc_cls=UsernameConflictError,
        )

except TransactionCanceledException as err:
    for reason in err.reasons:
        print(reason)
```

## API Reference

### DynamoDBClient

-   `get_item(key, exc_cls=..., default=...)` - Get single item
-   `put_item(item, cond_expr=..., exc_cls=...)` - Put item
-   `update_item(key, update_expr, ...)` - Update item
-   `delete_item(key, cond_expr=...)` - Delete item
-   `query(key_cond_expr, ...)` - Query items
-   `transact_get()` - Start transactional read
-   `transact_writer(flush_amount=50)` - Start transactional write

### Keys

-   `PrimaryKey(id, sk)` - Primary key with partition and sort
-   `SortKey(sk, rename_key=..., path_spec=..., projection_expr=...)` - Sort key with projections
-   `PartitionKey(pk)` - Partition key only

### ConfigDict

```python
from dynamodx.client import ConfigDict

ConfigDict(
    table='table-name',
    partition_key='pk',
    sort_key='sk',  # optional
)
```

## License

Dynamodx is open-source software licensed under the [MIT License](LICENSE).
