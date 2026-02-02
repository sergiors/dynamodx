## Dynamodx

A developer-friendly library for DynamoDB, simplifying single-table design without ORM lock-in.

```python
import boto3

from dynamodx.transact_writer import TransactWriter, TransactionOperationFailed

class EmailConflictError(TransactionOperationFailed): pass

try:
    with TransactWriter(table_name=..., client=...) as transact:
        transact.put(
            item={
                'pk': user_id,
                'sk': '0',
                'name': name,
                'email': email,
                'phone': phone,
            }
        )
        transact.put(
            item={
                'pk': f'EMAIL',
                'sk': email,
                'user_id': user_id,
            },
            cond_expr='attribute_not_exists(sk)',
            return_on_cond_fail='ALL_OLD',
            exc_cls=EmailConflictError,
        )
except EmailConflictError as err:
    # Got existing `user_id`
    user_id = err.reason['old_image']['user_id']
```
