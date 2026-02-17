from pathlib import Path
from typing import TYPE_CHECKING, Callable, Generator, TypedDict

import boto3
import jsonlines
import pytest

from dynamodx.types import serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
else:
    DynamoDBClient = object


DynamoDBSettings = TypedDict(
    'DynamoDBSettings',
    {
        'TableName': str,
        'PartitionKey': str,
        'SortKey': str,
    },
)


@pytest.fixture
def dynamodb_settings() -> dict:
    return {
        'TableName': 'pytest',
        'PartitionKey': 'id',
        'SortKey': 'sk',
    }


@pytest.fixture
def dynamodb_client(
    dynamodb_settings: DynamoDBSettings,
) -> Generator[DynamoDBClient, None, None]:
    table_name = dynamodb_settings['TableName']
    pk = dynamodb_settings['PartitionKey']
    sk = dynamodb_settings['SortKey']

    client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')
    client.create_table(
        AttributeDefinitions=[
            {'AttributeName': pk, 'AttributeType': 'S'},
            {'AttributeName': sk, 'AttributeType': 'S'},
        ],
        TableName=table_name,
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
def dynamodb_seeds(dynamodb_client: DynamoDBClient) -> 'Seeds':
    return Seeds(dynamodb_client)


class Seeds:
    def __init__(self, client: DynamoDBClient):
        self._client = client

    def __call__(self, seedfile: str) -> None:
        with jsonlines.open(Path('tests/seeds') / seedfile) as lines:
            for line in lines:
                self._client.put_item(TableName='pytest', Item=serialize(line))
