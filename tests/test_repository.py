import pytest

from dynamodx.repository import DynamoDBRepository
from tests.conftest import DynamoDBClient


@pytest.fixture
def dyn(dynamodb_client: DynamoDBClient):
    return DynamoDBRepository('pytest', client=dynamodb_client)


def test_put_item(dyn: DynamoDBRepository): ...
