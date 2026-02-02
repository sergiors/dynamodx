"""
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
"""

from abc import ABC, abstractmethod
from decimal import Decimal
from functools import reduce
from typing import Any, Literal


class _Unset:
    pass


class Expr(ABC):
    path: str
    value: str | set | Decimal | _Unset

    def expr_attr_names(self) -> dict:
        return {self.name_placeholder: self.path}

    def expr_attr_values(self) -> dict:
        return {self.value_placeholder: self.value}

    @property
    def name_placeholder(self) -> str:
        return f'#n_{self.path}'.replace('.', '_')

    @property
    def value_placeholder(self) -> str:
        return f':v_{self.path}'

    @abstractmethod
    def expr(self) -> str: ...


class FuncExpr(Expr, ABC):
    def __init__(
        self,
        func: str,
        path: str,
        value: Any,
    ):
        self.func = func
        self.path = path
        self.value = value

    def expr(self) -> str:
        func = self.func
        name = self.name_placeholder
        value = self.value_placeholder
        return f'{func}({name}, {value})'


class IfNotExistsExpr(FuncExpr):
    def __init__(
        self,
        path: str,
        value: Any,
        *,
        r_value: int | None = None,
        operand: Literal['+', '-'] | None = None,
    ):
        super().__init__('if_not_exists', path, value)

        self.operand = operand
        self.r_value = r_value

    def expr(self) -> str:
        expr = super().expr()
        operand = self.operand

        if not self.r_value:
            return expr

        return f'{expr} {operand} {self.value_placeholder}_r'

    def expr_attr_values(self) -> dict:
        attrs = super().expr_attr_values()

        if not self.r_value:
            return attrs

        return attrs | {
            f'{self.value_placeholder}_r': self.r_value,
        }

    def __add__(self, right_op: int) -> 'IfNotExistsExpr':
        return IfNotExistsExpr(
            path=self.path,
            value=self.value,
            r_value=right_op,
            operand='+',
        )

    def __sub__(self, right_op: int) -> 'IfNotExistsExpr':
        return IfNotExistsExpr(
            path=self.path,
            value=self.value,
            r_value=right_op,
            operand='-',
        )


def list_append(**kwargs):
    (k, v), *_ = kwargs.items()
    return FuncExpr('list_append', k, v)


def if_not_exists(**kwargs):
    (k, v), *_ = kwargs.items()
    return IfNotExistsExpr(k, v)


class Set(Expr):
    """
    Use the `SET` action in an update expression to add one or more attributes
    to an item.

    If any of these attributes already exists, they are overwritten by the new values.

    If you want to avoid overwriting an existing attribute, you can use `SET`
    with the `if_not_exists` function.

    The `if_not_exists` function is specific to the SET action and can only
    be used in an update expression.
    """

    def __init__(
        self,
        *,
        operand: Literal['=', '+', '-'] | None = None,
        **kwargs,
    ):
        (k, v), *_ = kwargs.items()

        # if isinstance(v, FuncExpr):
        #     self.func = v
        #     self.path = k
        #     self.value = v.value
        # else:
        #     self.func = None

        self.path = k
        self.value = v
        self.operand = operand

    def expr(self) -> str:
        name = self.name_placeholder
        value = self.value_placeholder
        operand = self.operand

        if isinstance(self.value, FuncExpr):
            expr = self.value.expr()
            return f'{name} = {expr}'

        if operand in ('+', '-'):
            # Incrementing and decrementing numeric attributes
            # You can add to or subtract from an existing numeric attribute.
            # To do this, use the + (plus) and - (minus) operators.
            return f'{name} = {name} {operand} {value}'

        return f'{name} = {value}'

    def expr_attr_names(self) -> dict:
        attrs = super().expr_attr_names()

        if isinstance(self.value, FuncExpr):
            return attrs | self.value.expr_attr_names()

        return attrs

    def expr_attr_values(self) -> dict:
        if isinstance(self.value, FuncExpr):
            return self.value.expr_attr_values()

        return super().expr_attr_values()


class Add(Expr):
    def __init__(self, **kwargs) -> None:
        (k, v), *_ = kwargs.items()

        if not isinstance(v, (set, Decimal)):
            raise ValueError('ADD action supports only number and set data types')

        self.path = k
        self.value = v

    def expr(self) -> str:
        return f'{self.name_placeholder} {self.value_placeholder}'


class Remove(Expr):
    def __init__(self, path: str) -> None:
        self.path = path
        self.value = _Unset()

    def expr(self) -> str:
        return self.name_placeholder

    def expr_attr_values(self) -> dict:
        return {}


class Delete(Expr):
    def __init__(self, **kwargs) -> None:
        (k, v), *_ = kwargs.items()

        if not isinstance(v, set):
            raise ValueError('DELETE action supports only Set data types')

        self.path = k
        self.value = v

    def expr(self) -> str:
        return f'{self.name_placeholder} {self.value_placeholder}'


class UpdateExpr(dict):
    def __init__(self, *args) -> None:
        super().__init__()
        exprs = [x for x in args if x.value is not None]
        self.update(self.__asdict(exprs))

    def __asdict(self, exprs: list[Expr] = []) -> dict:
        expr_attr_names = reduce(
            lambda acc, attr: {**acc, **attr.expr_attr_names()}, exprs, {}
        )
        expr_attr_values = reduce(
            lambda acc, attr: {**acc, **attr.expr_attr_values()}, exprs, {}
        )

        sets = list(filter(lambda attr: isinstance(attr, Set), exprs))
        adds = list(filter(lambda attr: isinstance(attr, Add), exprs))
        removes = list(filter(lambda attr: isinstance(attr, Remove), exprs))
        deletes = list(filter(lambda attr: isinstance(attr, Delete), exprs))

        expr_parts = []
        if sets:
            set_expr = ', '.join(attr.expr() for attr in sets)
            expr_parts.append(f'SET {set_expr}')

        if adds:
            add_expr = ', '.join(attr.expr() for attr in adds)
            expr_parts.append(f'ADD {add_expr}')

        if removes:
            remove_expr = ', '.join(attr.expr() for attr in removes)
            expr_parts.append(f'REMOVE {remove_expr}')

        if deletes:
            delete_expr = ', '.join(attr.expr() for attr in deletes)
            expr_parts.append(f'DELETE {delete_expr}')

        update_expr = ' '.join(expr_parts)

        return {
            'update_expr': update_expr,
            'expr_attr_names': expr_attr_names,
            'expr_attr_values': expr_attr_values,
        }
