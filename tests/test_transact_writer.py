import pytest

from dynamodx.transact_writer import (
    TransactionCanceledException,
    TransactionOperationFailed,
    TransactWriter,
)


@pytest.mark.parametrize('seeds', ['transact_writer.jsonl'], indirect=True)
def test_transact_writer_put_with_conditional_expression_and_custom_exception(
    boto3_dynamodb_client, settings, seeds
):
    table_name = settings['table_name']

    class EmailConflictError(TransactionOperationFailed):
        pass

    with pytest.raises(EmailConflictError) as err:
        with TransactWriter(table_name, client=boto3_dynamodb_client) as tx:
            tx.put(
                item={
                    'id': 'USER#ff05221a-1c30-486c-8750-d9f27d152e62',
                    'sk': '0',
                    'name': 'Bilbo Baggins',
                },
            )
            tx.put(
                item={
                    'id': 'USER#ff05221a-1c30-486c-8750-d9f27d152e62',
                    'sk': 'EMAIL#bilbo@baggins.com',
                },
            )
            tx.put(
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
        # See `transact_writer.jsonl` if you need more info
        == 'f966f7e5-a9d3-4d0f-8219-dfc12602bffd'
    )


@pytest.mark.parametrize('seeds', ['transact_writer.jsonl'], indirect=True)
def test_transact_writer_multiple_failures_when_fail_fast_disabled(
    boto3_dynamodb_client, settings, seeds
):
    table_name = settings['table_name']

    class EmailConflictError(TransactionOperationFailed):
        pass

    class UsernameConflictError(TransactionOperationFailed):
        pass

    with pytest.raises(TransactionCanceledException) as err:
        with TransactWriter(
            table_name, client=boto3_dynamodb_client, fail_fast=False
        ) as tx:
            tx.put(
                item={
                    'id': 'EMAIL',
                    'sk': 'bilbo@baggins.com',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=EmailConflictError,
            )
            tx.put(
                item={
                    'id': 'USERNAME',
                    'sk': 'bilbo.baggins',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=UsernameConflictError,
            )

    assert len(err.value.reasons) == 2
