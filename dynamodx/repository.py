from typing import TYPE_CHECKING, Any, cast

from .transact_writer import TransactWriter
from .types import deserialize, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.literals import (
        ReturnValuesOnConditionCheckFailureType,
        ReturnValueType,
        SelectType,
    )
    from mypy_boto3_dynamodb.type_defs import (
        DeleteItemOutputTypeDef,
        GetItemOutputTypeDef,
        PutItemOutputTypeDef,
        QueryOutputTypeDef,
        UpdateItemOutputTypeDef,
    )
else:
    DynamoDBClient = Any
    ReturnValueType = Any
    ReturnValuesOnConditionCheckFailureType = Any
    DeleteItemOutputTypeDef = Any
    QueryOutputTypeDef = Any
    GetItemOutputTypeDef = Any
    PutItemOutputTypeDef = Any
    UpdateItemOutputTypeDef = Any


class Repository:
    def __init__(
        self,
        table_name: str,
        *,
        client: DynamoDBClient,
    ) -> None:
        self._table_name = table_name
        self._client = client

    def query(
        self,
        key_cond_expr: str,
        *,
        select: SelectType | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        filter_expr: str | None = None,
        projection_expr: str | None = None,
        limit: int | None = None,
        scan_index_forward: bool = True,
        exclusive_start_key: dict | None = None,
        table_name: str | None = None,
    ) -> QueryOutputTypeDef:
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

        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
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
            attrs['ExclusiveStartKey'] = serialize(exclusive_start_key)

        if filter_expr:
            attrs['FilterExpression'] = filter_expr

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.query(**attrs)

        return cast(
            QueryOutputTypeDef,
            output
            | dict(
                Items=[deserialize(item) for item in output.get('Items', [])],
            ),
        )

    def get_item(
        self,
        key: dict,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        projection_expr: str | None = None,
    ) -> GetItemOutputTypeDef:
        """The GetItem operation returns a set of attributes for the item
        with the given primary key.

        If there is no matching item, GetItem does not return any data and
        there will be no Item element in the response.

        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/get_item.html
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

        return cast(
            GetItemOutputTypeDef,
            output
            | {
                'Item': deserialize(output.get('Item', {})),
            },
        )

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
        attrs = {
            'TableName': table_name or self._table_name,
            'Item': serialize(item),
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


DynamoDBRepository = Repository
