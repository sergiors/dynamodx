from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self


class Key(ABC, dict):
    @abstractmethod
    def expr_attr_names(self) -> dict: ...

    @abstractmethod
    def expr_attr_values(self) -> dict: ...


class SortKey(str):
    sk: str | None = None

    """
    SortKey encapsulates the sort key value and optionally stores additional
    attributes for nested data extraction.

    Parameters
    ----------
    path_spec: str, optional
        Optional specification for nested data extraction.
    rename_key : str, optional
        If provided, renames the sort key in the output.
    """

    def __new__(
        cls,
        *args,
        path_spec: str | None = None,
        rename_key: str | None = None,
        projection_expr: str | None = None,
        expr_attr_names: dict | None = None,
        **kwargs,
    ) -> Self:
        if len(args):
            name_sk, value_sk = None, args[0]
        elif kwargs:
            (name_sk, value_sk), *_ = kwargs.items()
        else:
            raise TypeError('SortKey() requires a value')

        obj = super().__new__(cls, value_sk)
        obj.sk = name_sk
        return obj

    def __init__(
        self,
        *args,
        path_spec: str | None = None,
        rename_key: str | None = None,
        projection_expr: str | None = None,
        expr_attr_names: dict | None = None,
        **kwargs,
    ) -> None:
        # __init__ is used to store the parameters for later reference.
        # For immutable types like str, __init__ cannot change the instance's value.
        self.path_spec = path_spec
        self.rename_key = rename_key
        self.projection_expr = projection_expr
        self.expr_attr_names = expr_attr_names


class PartitionKey(Key):
    """Represents a partition key for DynamoDB queries"""

    def __init__(
        self,
        *,
        table_name: str | None = None,
        **kwargs,
    ) -> None:
        (name_pk, value_pk), *_ = kwargs.items()
        super().__init__(**{name_pk: value_pk})

        self.name_pk = name_pk
        self.table_name = table_name

    def expr_attr_names(self) -> dict:
        return {'#pk': self.name_pk}

    def expr_attr_values(self) -> dict:
        return {':pk': self[self.name_pk]}

    def __add__(self, other: SortKey) -> 'PrimaryKey':
        pk = self.name_pk
        sk = other.sk
        kwargs = {
            pk: self[pk],
            sk: other,
            'table_name': self.table_name,
            'projection_expr': other.projection_expr,
        }
        return PrimaryKey(**kwargs)


class PrimaryKey(Key):
    """Represents a composite key (partition key and sort key) for DynamoDB queries"""

    def __init__(
        self,
        *,
        table_name: str | None = None,
        **kwargs,
    ) -> None:
        """
        Initializes a composite key using partition and sort key.

        Parameters
        ----------
        pk : str
            The partition key.
        sk : str
            The sort key.
        table_name : str, optional
        """
        (name_pk, value_pk), (name_sk, value_sk), *_ = kwargs.items()
        super().__init__(**{name_pk: value_pk, name_sk: value_sk})

        self.name_pk = name_pk
        self.name_sk = name_sk
        self.table_name = table_name

    @property
    def sk(self):
        return self[self.name_sk]

    def expr_attr_names(self) -> dict:
        return {
            '#pk': self.name_pk,
            '#sk': self.name_sk,
        }

    def expr_attr_values(self) -> dict:
        sk = self[self.name_sk]
        return {
            ':pk': self[self.name_pk],
            ':sk': str(sk) if isinstance(sk, SortKey) else sk,
        }

    def __add__(self, other: Self | SortKey) -> 'PrimaryKeySet':
        if isinstance(other, PrimaryKey):
            return PrimaryKeySet((self, other))

        if isinstance(other, SortKey):
            pk, sk = self.name_pk, other.sk
            kwargs = {pk: self[pk], sk: other}
            return PrimaryKeySet((self, PrimaryKey(**kwargs)))

        return NotImplemented

    def __radd__(self, other: Any):
        if isinstance(other, PrimaryKeySet):
            return other + self

        return NotImplemented


@dataclass(frozen=True)
class PrimaryKeySet:
    pairs: tuple[PrimaryKey, ...] = ()

    def __add__(self, other: PrimaryKey | SortKey) -> 'PrimaryKeySet':
        if not isinstance(other, (PrimaryKey, SortKey)):
            return NotImplemented

        if isinstance(other, PrimaryKey):
            return PrimaryKeySet(pairs=self.pairs + (other,))

        last_pair = self.pairs[-1]
        pk, sk = last_pair.name_pk, last_pair.name_sk
        kwargs = {
            pk: last_pair[pk],
            sk: other,
            'table_name': last_pair.table_name,
        }
        next_pair = PrimaryKey(**kwargs)

        if next_pair in self.pairs:
            return self

        return PrimaryKeySet(pairs=self.pairs + (next_pair,))
