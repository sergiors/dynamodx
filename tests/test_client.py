from dataclasses import dataclass

import pytest

from dynamodx.client import DynamoDBClient
from dynamodx.keys import PrimaryKey, SortKey


@pytest.fixture
def client(settings, boto3_dynamodb_client):
    return DynamoDBClient(
        table_name=settings['table_name'],
        client=boto3_dynamodb_client,
    )


@pytest.mark.parametrize('seeds', ['test_repository.jsonl'], indirect=True)
def test_transact_get_returns_nested_items_with_flatten_top_false(client, seeds):
    user_id = 'f841e66c-f7b9-48be-9429-9e6da362aeba'
    output = client.transact_get().get_items(
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


@pytest.mark.parametrize('seeds', ['test_repository.jsonl'], indirect=True)
def test_get_item_with_sort_key_path_spec_and_none_fallback(client, seeds):
    output = client.get_item(
        PrimaryKey(
            id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
            sk=SortKey(
                sk='0',
                path_spec='name',
            ),
        ),
    )

    assert output == 'Legolas Greenleaf'

    nothing = client.get_item(
        PrimaryKey(
            id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
            sk=SortKey(
                sk='0',
                path_spec='email',
            ),
        ),
    )

    assert nothing is None


def test_get_item_raises_custom_exception_when_item_missing(client):
    class UserNotFounedError(Exception):
        pass

    with pytest.raises(UserNotFounedError):
        client.get_item(
            PrimaryKey(
                id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
                sk='0',
            ),
            exc_cls=UserNotFounedError,
        )


def test_put_item_accepts_dataclass_with_config_dict(settings, client):
    from dynamodx.client import ConfigDict

    table_name = settings['table_name']

    @dataclass
    class User:
        __dynamodb_config__ = ConfigDict(
            table=table_name,
            partition_key='id',
            sort_key='sk',
        )
        id: str
        sk: str
        name: str

    user = User(id='233', sk='1233', name='Elrond Peredhel')

    assert client.put_item(user)

    r = client.get_item(PrimaryKey(id=user.id, sk=user.sk))
    assert r['name'] == 'Elrond Peredhel'


@pytest.mark.parametrize('seeds', ['test_repository.jsonl'], indirect=True)
def test_client_uses_boto3_internally_for_get_item(settings, seeds, client):
    output = client.get_item(
        PrimaryKey(
            id='USER#f841e66c-f7b9-48be-9429-9e6da362aeba',
            sk=SortKey(
                sk='0',
                path_spec='name',
            ),
        ),
    )

    assert output == 'Legolas Greenleaf'


def test_put_item_with_pydantic_model(settings, client):
    from pydantic import BaseModel

    table_name = settings['table_name']

    class User(BaseModel):
        __dynamodb_config__ = {
            'table': table_name,
            'partition_key': 'id',
            'sort_key': 'sk',
        }
        id: str
        sk: str
        name: str

    user = User(id='334', sk='2344', name='Arwen Undómiel')

    assert client.put_item(user)

    r = client.get_item(PrimaryKey(id=user.id, sk=user.sk))
    assert r['name'] == 'Arwen Undómiel'


def test_put_item_with_plain_dataclass(settings, client):
    from dataclasses import dataclass

    table_name = settings['table_name']

    @dataclass
    class Project:
        __dynamodb_config__ = {
            'table': table_name,
            'partition_key': 'id',
            'sort_key': 'sk',
        }
        id: str
        sk: str
        title: str

    project = Project(id='PROJ#1', sk='1', title='Mithril excavation')

    assert client.put_item(project)

    r = client.get_item(PrimaryKey(id=project.id, sk=project.sk))
    assert r['title'] == 'Mithril excavation'


def test_put_item_with_pydantic_model_config_style(settings, client):
    from pydantic import BaseModel, ConfigDict

    table_name = settings['table_name']

    class User(BaseModel):
        model_config = ConfigDict(  # type: ignore
            # Pydantic settings
            str_strip_whitespace=True,
            # DynamoDB settings
            table=table_name,
            partition_key='id',
            sort_key='sk',
        )
        id: str
        sk: str
        name: str

    user = User(id='445', sk='3455', name='Gandalf the Grey')

    assert client.put_item(user)

    r = client.get_item(PrimaryKey(id=user.id, sk=user.sk))
    assert r['name'] == 'Gandalf the Grey'
