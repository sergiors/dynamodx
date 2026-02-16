from typing import TYPE_CHECKING, Generator

import boto3
import jsonlines
import pytest

from dynamodx.types import serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
else:
    DynamoDBClient = object


@pytest.fixture
def dynamodb_settings() -> dict:
    return {
        'TableName': 'pytest',
        'PartitionKey': 'pk',
        'SortKey': 'sk',
    }


@pytest.fixture
def dynamodb_client(dynamodb_settings) -> Generator[DynamoDBClient, None, None]:
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
def dynamodb_seeds(dynamodb_client):
    def seeds(seedfile: str):
        with jsonlines.open(f'tests/seeds/{seedfile}') as lines:
            for line in lines:
                dynamodb_client.put_item(TableName='pytest', Item=serialize(line))

    return seeds
