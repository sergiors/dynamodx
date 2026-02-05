from typing import TYPE_CHECKING, Any, Literal, Self, Type, TypedDict

import jmespath

from .types import deserialize, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import TransactWriteItemTypeDef
else:
    DynamoDBClient = object
    TransactWriteItemTypeDef = Any


class TransactionCanceledReason(TypedDict):
    code: str
    message: str
    operation: dict
    old_item: dict


class TransactionOperationFailed(Exception):
    msg: str
    reason: TransactionCanceledReason

    def __init__(
        self,
        msg: str = '',
        *,
        reason: TransactionCanceledReason,
    ) -> None:
        super().__init__(msg)
        self.msg = msg
        self.reason = reason


class TransactionCanceledException(Exception):
    def __init__(
        self,
        msg: str = '',
        *,
        reasons: list[TransactionCanceledReason] = [],
    ) -> None:
        super().__init__(msg)
        self.msg = msg
        self.reasons = reasons


class TransactOperation:
    def __init__(
        self,
        operation: dict,
        exc_cls: type[Exception] | None = None,
    ) -> None:
        self.operation = operation
        self.exc_cls = exc_cls


class TransactWriter:
    def __init__(
        self,
        table_name: str,
        *,
        flush_amount: int = 50,
        client: DynamoDBClient,
    ) -> None:
        self._table_name = table_name
        self._items_buffer: list[TransactOperation] = []
        self._flush_amount = flush_amount
        self._client = client

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_details) -> None:
        # When we exit, we need to keep flushing whatever's left
        # until there's nothing left in our items buffer.
        while self._items_buffer:
            self._flush()

    def condition(
        self,
        key: dict,
        cond_expr: str,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        return_on_cond_fail: Literal['ALL_OLD', 'NONE'] = 'NONE',
        exc_cls: Type[Exception] | None = None,
    ) -> None:
        attrs: dict = {}

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        self._add_op_and_process(
            TransactOperation(
                {
                    'ConditionCheck': dict(
                        TableName=table_name or self._table_name,
                        Key=serialize(key),
                        ConditionExpression=cond_expr,
                        **attrs,
                    )
                },
                exc_cls,
            )
        )

    def put(
        self,
        item: dict,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        cond_expr: str | None = None,
        return_on_cond_fail: Literal['ALL_OLD', 'NONE'] = 'NONE',
        exc_cls: Type[Exception] | None = None,
    ) -> None:
        attrs: dict = {}

        if cond_expr:
            attrs['ConditionExpression'] = cond_expr

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        self._add_op_and_process(
            TransactOperation(
                {
                    'Put': dict(
                        TableName=table_name or self._table_name,
                        Item=serialize(item),
                        **attrs,
                    )
                },
                exc_cls,
            ),
        )

    def delete(
        self,
        key: dict,
        *,
        table_name: str | None = None,
        cond_expr: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        return_on_cond_fail: Literal['ALL_OLD', 'NONE'] = 'NONE',
        exc_cls: Type[Exception] | None = None,
    ) -> None:
        attrs: dict = {}

        if cond_expr:
            attrs['ConditionExpression'] = cond_expr

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        self._add_op_and_process(
            TransactOperation(
                {
                    'Delete': dict(
                        TableName=table_name or self._table_name,
                        Key=serialize(key),
                        **attrs,
                    )
                },
                exc_cls,
            ),
        )

    def update(
        self,
        key: dict,
        update_expr: str,
        *,
        cond_expr: str | None = None,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        return_on_cond_fail: Literal['ALL_OLD', 'NONE'] = 'NONE',
        exc_cls: Type[Exception] | None = None,
    ) -> None:
        attrs: dict = {}

        if cond_expr:
            attrs['ConditionExpression'] = cond_expr

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            attrs['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        self._add_op_and_process(
            TransactOperation(
                {
                    'Update': dict(
                        TableName=table_name or self._table_name,
                        Key=serialize(key),
                        UpdateExpression=update_expr,
                        **attrs,
                    )
                },
                exc_cls,
            )
        )

    def _add_op_and_process(self, op: TransactOperation) -> None:
        self._items_buffer.append(op)
        self._flush_if_needed()

    def _flush_if_needed(self) -> None:
        if len(self._items_buffer) >= self._flush_amount:
            self._flush()

    def _flush(self) -> bool:
        items_to_send = self._items_buffer[: self._flush_amount]
        self._items_buffer = self._items_buffer[self._flush_amount :]

        transact_items: list[TransactWriteItemTypeDef] = [
            item.operation  # type: ignore
            for item in items_to_send
        ]

        try:
            self._client.transact_write_items(TransactItems=transact_items)
        except self._client.exceptions.TransactionCanceledException as err:
            error_msg = jmespath.search('response.Error.Message || `Unknown`', err)
            cancellations = err.response.get('CancellationReasons', [])
            reasons = []

            for idx, reason in enumerate(cancellations):
                if 'Message' not in reason:
                    continue

                item = items_to_send[idx]
                cancellation_reason = TransactionCanceledReason(
                    code=reason['Code'],  # type: ignore
                    message=reason['Message'],
                    operation=item.operation,
                    old_item=deserialize(reason.get('Item', {})),
                )

                if item.exc_cls:
                    _raise_for_reason(item.exc_cls, error_msg, cancellation_reason)

                reasons.append(cancellation_reason)

            raise TransactionCanceledException(error_msg, reasons=reasons)
        else:
            return True


def _raise_for_reason(
    exc_cls: Type[Exception],
    msg: str,
    reason: TransactionCanceledReason,
):
    if issubclass(exc_cls, TransactionOperationFailed):
        raise exc_cls(msg, reason=reason)

    exc = exc_cls(msg)
    setattr(exc, '__reason__', reason)
    raise exc
