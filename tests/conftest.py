from pathlib import Path
from typing import TYPE_CHECKING, Generator

import boto3
import jsonlines
import pytest

from dynamodx.types import serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
else:
    Boto3DynamoDBClient = object


@pytest.fixture
def settings() -> dict:
    return {
        'table_name': 'pytest',
        'partition_key': 'id',
        'sort_key': 'sk',
    }


@pytest.fixture
def boto3_dynamodb_client(
    settings,
) -> Generator[Boto3DynamoDBClient, None, None]:
    table_name = settings['table_name']
    pk = settings['partition_key']
    sk = settings['sort_key']

    client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {'AttributeName': pk, 'AttributeType': 'S'},
            {'AttributeName': sk, 'AttributeType': 'S'},
        ],
        KeySchema=[
            {'AttributeName': pk, 'KeyType': 'HASH'},
            {'AttributeName': sk, 'KeyType': 'RANGE'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123,
        },
    )

    yield client

    client.delete_table(TableName=table_name)


@pytest.fixture()
def seeds(
    request,
    settings,
    boto3_dynamodb_client,
):
    seedfile = request.param
    table_name = settings['table_name']

    with open(Path('tests/seeds') / seedfile, 'rb') as fp:
        reader = jsonlines.Reader(fp)

        for line in reader.iter(type=dict, skip_invalid=True):
            boto3_dynamodb_client.put_item(TableName=table_name, Item=serialize(line))
