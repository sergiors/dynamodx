from dataclasses import dataclass

import pytest

from dynamodx.keys import PrimaryKey, SortKey
from dynamodx.repository import DynamoDBRepository, dynamodb_mapping
from tests.conftest import DynamoDBClient, DynamoDBSettings, Seeds


@pytest.fixture
def dyn(
    dynamodb_client: DynamoDBClient,
    dynamodb_settings: DynamoDBSettings,
):
    return DynamoDBRepository(
        dynamodb_settings['TableName'],
        client=dynamodb_client,
    )


def test_get_items(
    dyn: DynamoDBRepository,
    dynamodb_seeds: Seeds,
):
    dynamodb_seeds('test_repository.jsonl')

    user_id = 'f841e66c-f7b9-48be-9429-9e6da362aeba'
    output = dyn.transact_get().get_items(
        PrimaryKey(
            id='PASSWORD_RESET',
            sk=SortKey(
                sk=f'USER#{user_id}',
                rename_key='reset_code',
                path_spec='code',
            ),
        )
        + PrimaryKey(
            id=f'USER#{user_id}',
            sk=SortKey(
                sk='0',
                rename_key='user',
            ),
        ),
        flatten_top=False,
    )

    assert output == {
        'reset_code': (
            'RoM1e1m4h9V6Ari4LQ00i4KRo3sAV5GVY7kKz8hRRHCDkdFoyvtSjEZXRBF2CFHh'
        ),
        'user': {
            'id': 'USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
            'sk': '0',
            'name': 'Legolas Greenleaf',
        },
    }


def test_get_item(
    dyn: DynamoDBRepository,
    dynamodb_seeds: Seeds,
):
    dynamodb_seeds('test_repository.jsonl')
    output = dyn.get_item(
        PrimaryKey(
            id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
            sk=SortKey(
                sk='0',
                path_spec='name',
            ),
        ),
    )

    assert output == 'Legolas Greenleaf'

    nothing = dyn.get_item(
        PrimaryKey(
            id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
            sk=SortKey(
                sk='0',
                path_spec='email',
            ),
        ),
    )

    assert nothing is None


def test_get_item_not_found_error(
    dyn: DynamoDBRepository,
):
    class UserNotFounedError(Exception):
        pass

    with pytest.raises(UserNotFounedError):
        dyn.get_item(
            PrimaryKey(
                id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
                sk='0',
            ),
            exc_cls=UserNotFounedError,
        )


def test_put_item_from_dataclass(
    dynamodb_settings: DynamoDBSettings,
    dyn: DynamoDBRepository,
):
    @dynamodb_mapping(
        dynamodb_settings['TableName'],
        partition_key='id',
        sort_key='sk',
    )
    @dataclass
    class User:
        id: str
        sk: str
        name: str

    user = User(id='233', sk='1233', name='Elrond Peredhel')

    assert dyn.put_item(user)

    r = dyn.get_item(PrimaryKey(id=user.id, sk=user.sk))
    assert r['name'] == 'Elrond Peredhel'
