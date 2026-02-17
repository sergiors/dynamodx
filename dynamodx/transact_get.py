from typing import TYPE_CHECKING, Any

from dynamodx.keys import PrimaryKey, PrimaryKeySet
from dynamodx.types import deserialize, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import GetTypeDef, TransactGetItemTypeDef

else:
    DynamoDBClient = Any
    GetTypeDef = Any
    TransactGetItemTypeDef = Any


class TransactGet:
    def __init__(
        self,
        table_name: str,
        *,
        client: DynamoDBClient,
    ) -> None:
        self._table_name = table_name
        self._client = client

    def get_items(self, kset: PrimaryKeySet, *, flatten_top: bool = True):
        table_name = self._table_name
        transact_items: list[TransactGetItemTypeDef] = [
            _build_get(pk, table_name) for pk in kset.pairs
        ]

        output = self._client.transact_get_items(TransactItems=transact_items)
        items = [
            deserialize(res.get('Item', {})) for res in output.get('Responses', [])
        ]

        if flatten_top and items:
            head, tail = items[0], items[1:]
        else:
            head, tail = {}, items

        pairs = kset.pairs[1:] if flatten_top else kset.pairs
        nested = {
            _get_sk(pk): item for pk, item in zip(pairs, tail, strict=True) if item
        }
        return {**head, **nested}


def _build_get(pk: PrimaryKey, table_name: str) -> TransactGetItemTypeDef:
    sk = pk[pk.name_sk]
    attrs: GetTypeDef = {
        'TableName': pk.table_name or table_name,
        'Key': serialize(pk),
    }

    projection_expr = getattr(sk, 'projection_expr', None)
    expr_attr_names = getattr(sk, 'expr_attr_names', None)

    if projection_expr is not None:
        attrs['ProjectionExpression'] = projection_expr

    if expr_attr_names is not None:
        attrs['ExpressionAttributeNames'] = expr_attr_names

    return {'Get': attrs}


def _get_sk(pk: PrimaryKey) -> str:
    sk = pk.sk
    return str(getattr(sk, 'rename_key', sk))
