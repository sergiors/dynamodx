from dynamodx.keys import PartitionKey, SortKey
from dynamodx.transact_get import TransactGet
from tests.conftest import DynamoDBClient, DynamoDBSettings, Seeds


def test_transact_get(
    dynamodb_settings: DynamoDBSettings,
    dynamodb_seeds: Seeds,
    dynamodb_client: DynamoDBClient,
):
    dynamodb_seeds('transact_get.jsonl')
    table_name = dynamodb_settings['TableName']
    transact = TransactGet(table_name, client=dynamodb_client)

    user = transact.get_items(
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
            path_spec='mx_record.exists',
        )
    )
    assert user == {
        'sk': '0',
        'name': 'Aragorn II Elessar',
        'id': 'USER#4df0f9ac-a235-41a2-9746-7a84409a809b',
        'exceeded': {'sk': 'RATE_LIMIT_EXCEEDED'},
        'mx_record_exists': True,
    }
