import pytest

from dynamodx.transact_writer import (
    TransactionCanceledException,
    TransactionOperationFailed,
    TransactWriter,
)
from tests.conftest import DynamoDBClient, DynamoDBSettings, Seeds


def test_transact_write_items(
    dynamodb_settings: DynamoDBSettings,
    dynamodb_seeds: Seeds,
    dynamodb_client: DynamoDBClient,
):
    table_name = dynamodb_settings['TableName']
    dynamodb_seeds('transact_writer.jsonl')

    class EmailConflictError(TransactionOperationFailed):
        pass

    with pytest.raises(EmailConflictError) as err:
        with TransactWriter(table_name, client=dynamodb_client) as transact:
            transact.put(
                item={
                    'id': 'USER#ff05221a-1c30-486c-8750-d9f27d152e62',
                    'sk': '0',
                    'name': 'Bilbo Baggins',
                },
            )
            transact.put(
                item={
                    'id': 'USER#ff05221a-1c30-486c-8750-d9f27d152e62',
                    'sk': 'EMAIL#bilbo@baggins.com',
                },
            )
            transact.put(
                item={
                    'id': 'EMAIL',
                    'sk': 'bilbo@baggins.com',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=EmailConflictError,
                return_on_cond_fail='ALL_OLD',
            )

    assert (
        err.value.reason['old_item']['user_id']
        # See `seeds.jsonl` if you need more info
        == 'f966f7e5-a9d3-4d0f-8219-dfc12602bffd'
    )


def test_when_fail_fast_disabled(
    dynamodb_seeds: Seeds,
    dynamodb_client: DynamoDBClient,
):
    dynamodb_seeds('transact_writer.jsonl')

    class EmailConflictError(TransactionOperationFailed):
        pass

    class UsernameConflictError(TransactionOperationFailed):
        pass

    with pytest.raises(TransactionCanceledException) as err:
        with TransactWriter(
            'pytest', client=dynamodb_client, fail_fast=False
        ) as transact:
            transact.put(
                item={
                    'id': 'EMAIL',
                    'sk': 'bilbo@baggins.com',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=EmailConflictError,
            )
            transact.put(
                item={
                    'id': 'USERNAME',
                    'sk': 'bilbo.baggins',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=UsernameConflictError,
            )

    assert len(err.value.reasons) == 2
