import pytest

from dynamodx.keys import PartitionKey, SortKey
from dynamodx.transact_get import TransactGet


@pytest.mark.parametrize('seeds', ['transact_get.jsonl'], indirect=True)
def test_transact_get_with_multiple_sort_keys_and_projections(
    boto3_dynamodb_client,
    settings,
    seeds,
):
    table_name = settings['table_name']
    tx = TransactGet(table_name, client=boto3_dynamodb_client)

    user = tx.get_items(
        PartitionKey(id='USER#4df0f9ac-a235-41a2-9746-7a84409a809b')
        + SortKey(sk='0')
        + SortKey(
            sk='RATE_LIMIT_EXCEEDED',
            rename_key='exceeded',
            projection_expr='#sk',
            expr_attr_names={'#sk': 'sk'},
        )
        + SortKey(
            sk='TEMPORARY_PASSWORD',
            rename_key='temporary_password',
        )
        + SortKey(
            sk='EMAIL#aragorn@gondor.com',
            rename_key='mx_record_exists',
            path_spec='mx_record_exists',
        )
    )
    assert user == {
        'sk': '0',
        'name': 'Aragorn II Elessar',
        'id': 'USER#4df0f9ac-a235-41a2-9746-7a84409a809b',
        'exceeded': {'sk': 'RATE_LIMIT_EXCEEDED'},
        'mx_record_exists': True,
    }
