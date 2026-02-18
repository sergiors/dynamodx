from typing import TYPE_CHECKING, Any

import jmespath

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

    def get_items(
        self,
        keyset: PrimaryKeySet,
        *,
        flatten_top: bool = True,
    ) -> dict[str, Any]:
        """Get multiple items via a transaction based on the provided TransactKey.

        Parameters
        ----------
        key : PrimaryKeySet

        flatten_top : bool, optional
            Determines whether the first nested item in the transaction result
            should be flattened,
            i.e., extracted to serve as the primary item at the top level of
            the returned dict.
            If True, the nested item is promoted to the top level.

        Returns
        -------
        dict[str, Any]
            A dict of items retrieved from the transaction.
        """
        table_name = self._table_name
        transact_items: list[TransactGetItemTypeDef] = [
            _build_get(pk, table_name) for pk in keyset.pairs
        ]

        output = self._client.transact_get_items(TransactItems=transact_items)
        items = [deserialize(r.get('Item', {})) for r in output.get('Responses', [])]

        if flatten_top and items:
            head, tail = items[0], items[1:]
        else:
            head, tail = {}, items

        pairs = keyset.pairs[1:] if flatten_top else keyset.pairs
        nested = {
            _output_key(pk): _project_item(pk, item)
            for pk, item in zip(pairs, tail, strict=True)
            if item
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


def _output_key(pk: PrimaryKey) -> str:
    sk = pk.sk
    return str(getattr(sk, 'rename_key', sk))


def _project_item(pk: PrimaryKey, item: dict) -> dict:
    sk = pk.sk
    path_spec = getattr(sk, 'path_spec', None)

    if path_spec is None:
        return item

    return jmespath.compile(path_spec).search(item)
