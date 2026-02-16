import pytest

from dynamodx.repository import DynamoDBRepository


@pytest.fixture
def dyn(dynamodb_client):
    return DynamoDBRepository('pytest', client=dynamodb_client)


def test_put_item(dyn): ...
