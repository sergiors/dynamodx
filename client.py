import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import TYPE_CHECKING, Any, Type, TypedDict, TypeVar
from urllib.parse import quote, unquote

import boto3

from dynamodx.keys import PrimaryKey

from .transact_get import TransactGet, project_item
from .transact_writer import TransactWriter
from .types import deserialize, serialize, to_dict

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
    from mypy_boto3_dynamodb.literals import (
        ReturnValuesOnConditionCheckFailureType,
        ReturnValueType,
        SelectType,
    )
    from mypy_boto3_dynamodb.type_defs import (
        AttributeValueTypeDef,
        DeleteItemOutputTypeDef,
        PutItemOutputTypeDef,
        UpdateItemOutputTypeDef,
    )
else:
    Boto3DynamoDBClient = Any
    ReturnValuesOnConditionCheckFailureType = Any
    ReturnValueType = Any
    SelectType = Any
    AttributeValueTypeDef = Any
    DeleteItemOutputTypeDef = Any
    PutItemOutputTypeDef = Any
    UpdateItemOutputTypeDef = Any


class QueryOutput(TypedDict):
    items: list[dict[str, Any]]
    count: int
    last_key: str | None


class DynamoDBClient:
    """Table-scoped DynamoDB client that owns a boto3 client when not provided."""

    def __init__(
        self,
        table_name: str,
        *,
        client: Boto3DynamoDBClient | None = None,
        **client_kwargs: Any,
    ) -> None:
        self._table_name = table_name
        self._client = client or boto3.client('dynamodb', **client_kwargs)

    def query(
        self,
        key_cond_expr: str,
        *,
        select: SelectType | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        exclusive_start_key: str | None = None,
        filter_expr: str | None = None,
        projection_expr: str | None = None,
        limit: int | None = None,
        scan_index_forward: bool = True,
        table_name: str | None = None,
    ) -> QueryOutput:
        """You must provide the name of the partition key attribute
        and a single value for that attribute.

        Query returns all items with that partition key value.
        Optionally, you can provide a sort key attribute and use a comparison operator
        to refine the search results.

        ...

        A `Query` operation always returns a result set. If no matching items are found,
        the result set will be empty.
        Queries that do not return results consume the minimum number
        of read capacity units for that type of read operation.

        - https://docs.aws.amazon.com/boto3/latest/reference/services/dynamodb/client/query.html
        """
        attrs: dict = {
            'TableName': table_name or self._table_name,
            'KeyConditionExpression': key_cond_expr,
            'ScanIndexForward': scan_index_forward,
        }

        if select:
            attrs['Select'] = select

        if limit:
            attrs['Limit'] = limit

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if exclusive_start_key:
            attrs['ExclusiveStartKey'] = _startkey_b64decode(exclusive_start_key)

        if filter_expr:
            attrs['FilterExpression'] = filter_expr

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.query(**attrs)

        return {
            'items': [deserialize(item) for item in output.get('Items', [])],
            'count': output.get('Count', 0),
            'last_key': _startkey_b64encode(output.get('LastEvaluatedKey', None)),
        }

    def get_item(
        self,
        key: dict[str, str] | PrimaryKey,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        projection_expr: str | None = None,
        raise_on_error: bool = True,
        exc_cls: Type[Exception] = Exception,
        default: Any = None,
    ) -> dict[str, Any]:
        """Get an item with the given primary key.

        Parameters
        ----------
        key : dict[str, str] | PrimaryKey
            Primary key of the item to be retrieved.
        table_name : str | None, optional
            Uses default table if not provided.
        expr_attr_names : dict | None, optional
            Expression attribute name mappings.
        projection_expr : str | None, optional
            Attributes to return. Returns full item if None.
        raise_on_error : bool, optional
            If True, raises ``exc_cls`` when item is not found.
        exc_cls : Type[Exception], optional
            Exception class to be used if the item is not found.
        default : Any, optional
            Default value returned if the item is not found.

        Returns
        -------
        dict[str, Any]
            Data of the retrieved item or the default value if not found.

        Raises
        ------
        Exception
            If item is not found and ``raise_on_error`` is True.
        """
        attrs = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
        }

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.get_item(**attrs)
        item = deserialize(output.get('Item', {}))

        if raise_on_error and not item:
            raise exc_cls(f'Item not found ({key!r})')

        if isinstance(key, PrimaryKey):
            return project_item(key, item)

        return item or default

    def put_item(
        self,
        item: dict,
        *,
        cond_expr: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_values: ReturnValueType | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
    ) -> PutItemOutputTypeDef:
        config = get_dynamodb_config(item.__class__)
        serialized = serialize(to_dict(item) if config else item)  # type: ignore
        attrs = {
            'TableName': table_name or self._table_name,
            'Item': serialized,
        }

        if cond_expr:
            attrs['ConditionExpression'] = cond_expr

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        return self._client.put_item(**attrs)

    def update_item(
        self,
        key: dict,
        *,
        update_expr: str,
        cond_expr: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_values: ReturnValueType | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
    ) -> UpdateItemOutputTypeDef:
        attrs: dict = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
            'UpdateExpression': update_expr,
        }

        if cond_expr:
            attrs['ConditionExpression'] = cond_expr

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        return self._client.update_item(**attrs)

    def delete_item(
        self,
        key: dict,
        *,
        cond_expr: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
    ) -> DeleteItemOutputTypeDef:
        """Deletes a single item in a table by primary key. You can perform
        a conditional delete operation that deletes the item if it exists,
        or if it has an expected attribute value.
        """
        attrs: dict = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
        }

        if cond_expr:
            attrs['ConditionExpression'] = cond_expr

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        return self._client.delete_item(**attrs)

    def transact_writer(
        self,
        flush_amount: int = 50,
        table_name: str | None = None,
    ) -> TransactWriter:
        return TransactWriter(
            table_name=table_name or self._table_name,
            client=self._client,
            flush_amount=flush_amount,
        )

    def transact_get(
        self,
        table_name: str | None = None,
    ) -> TransactGet:
        return TransactGet(
            table_name=table_name or self._table_name,
            client=self._client,
        )


def _startkey_b64encode(obj: dict[str, AttributeValueTypeDef] | None) -> str | None:
    if not obj:
        return None

    s = json.dumps(obj)
    b = urlsafe_b64encode(s.encode('utf-8')).decode('utf-8')
    return quote(b)


def _startkey_b64decode(s: str) -> dict[str, AttributeValueTypeDef]:
    b = unquote(s).encode('utf-8')
    s = urlsafe_b64decode(b).decode('utf-8')
    return json.loads(s)


T = TypeVar('T')


class ConfigDict(TypedDict, total=False):
    """Configuration for DynamoDB mapping, similar to Pydantic's model_config."""

    table: str
    partition_key: str
    sort_key: str | None


def get_dynamodb_config(obj: Any) -> ConfigDict | None:
    """Extract DynamoDB config from model_config or __dynamodb_config__.
    
    Checks model_config first (Pydantic style), then falls back to __dynamodb_config__.
    """
    # Check model_config first (Pydantic style)
    model_config = getattr(obj, 'model_config', None)
    if model_config and isinstance(model_config, dict) and 'table' in model_config:
        return ConfigDict(
            table=model_config.get('table', ''),
            partition_key=model_config.get('partition_key'),
            sort_key=model_config.get('sort_key'),
        )
    
    # Fall back to __dynamodb_config__ (dataclass/plain class style)
    return getattr(obj, '__dynamodb_config__', None)
