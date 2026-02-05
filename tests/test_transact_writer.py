import pytest

from dynamodx.transact_writer import TransactionOperationFailed, TransactWriter


def test_transact_write_items(
    dynamodb_seeds,
    dynamodb_client,
):
    class EmailConflictError(TransactionOperationFailed): ...

    with pytest.raises(EmailConflictError):
        with TransactWriter('pytest', client=dynamodb_client) as transact:
            transact.put(
                item={
                    'pk': '5OxmMjL-ujoR5IMGegQz',
                    'sk': '0',
                    'name': 'Bilbo Baggins',
                },
            )
            transact.put(
                item={
                    'pk': '5OxmMjL-ujoR5IMGegQz',
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
            )
