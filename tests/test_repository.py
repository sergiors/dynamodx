import pytest

from dynamodx.keys import PrimaryKey, SortKey
from dynamodx.repository import DynamoDBRepository
from tests.conftest import DynamoDBClient, Seeds


@pytest.fixture
def dyn(dynamodb_client: DynamoDBClient):
    return DynamoDBRepository('pytest', client=dynamodb_client)


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
