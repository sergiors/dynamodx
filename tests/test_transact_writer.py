import pytest

from dynamodx.transact_writer import (
    TransactionCanceledException,
    TransactionOperationFailed,
    TransactWriter,
)


def test_transact_write_items(
    dynamodb_seeds,
    dynamodb_client,
):
    class EmailConflictError(TransactionOperationFailed):
        pass

    with pytest.raises(EmailConflictError) as err:
        with TransactWriter('pytest', client=dynamodb_client) as transact:
            transact.put(
                item={
                    'pk': 'ff05221a-1c30-486c-8750-d9f27d152e62',
                    'sk': '0',
                    'name': 'Bilbo Baggins',
                },
            )
            transact.put(
                item={
                    'pk': 'ff05221a-1c30-486c-8750-d9f27d152e62',
                    'sk': 'EMAIL#bilbo@baggins.com',
                },
            )
            transact.put(
                item={
                    'pk': 'EMAIL',
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
    dynamodb_seeds,
    dynamodb_client,
):
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
                    'pk': 'EMAIL',
                    'sk': 'bilbo@baggins.com',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=EmailConflictError,
            )
            transact.put(
                item={
                    'pk': 'USERNAME',
                    'sk': 'bilbo.baggins',
                },
                cond_expr='attribute_not_exists(sk)',
                exc_cls=UsernameConflictError,
            )

    assert len(err.value.reasons) == 2
