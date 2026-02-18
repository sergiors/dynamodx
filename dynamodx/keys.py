from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self


class Key(dict, ABC):
    @abstractmethod
    def expr_attr_names(self) -> dict[str, str]: ...

    @abstractmethod
    def expr_attr_values(self) -> dict[str, Any]: ...


class SortKey(str):
    """
    SortKey encapsulates a sort key value and optionally stores metadata
    used for nested data extraction and output transformation.

    Parameters
    ----------
    path_spec : str, optional
        JMESPath expression used to project nested data from the item.

    rename_key : str, optional
        If provided, renames the sort key in the output.

    projection_expr : str, optional
        Projection expression associated with the key.

    expr_attr_names : dict[str, str], optional
        Attribute name mapping used with projection expressions.
    """

    __slots__ = (
        'sk',
        'path_spec',
        'rename_key',
        'projection_expr',
        'expr_attr_names',
    )

    sk: str | None
    path_spec: str | None
    rename_key: str | None
    projection_expr: str | None
    expr_attr_names: dict[str, str] | None

    def __new__(
        cls,
        *,
        path_spec: str | None = None,
        rename_key: str | None = None,
        projection_expr: str | None = None,
        expr_attr_names: dict[str, str] | None = None,
        **kwargs: str,
    ) -> Self:
        if len(kwargs) != 1:
            raise TypeError(
                f'SortKey() takes exactly one keyword argument ({len(kwargs)} given)'
            )

        ((name_sk, value_sk),) = kwargs.items()
        obj = super().__new__(cls, value_sk)

        obj.sk = name_sk
        obj.path_spec = path_spec
        obj.rename_key = rename_key
        obj.projection_expr = projection_expr
        obj.expr_attr_names = expr_attr_names

        return obj


class PartitionKey(Key):
    """Represents a partition key"""

    def __init__(
        self,
        *,
        table_name: str | None = None,
        **kwargs,
    ) -> None:
        if len(kwargs) != 1:
            raise TypeError(
                f'PartitionKey() takes exactly one key=value argument '
                f'({len(kwargs)} given)'
            )

        ((name_pk, value_pk),) = kwargs.items()
        super().__init__(**{name_pk: value_pk})

        self.name_pk = name_pk
        self.table_name = table_name

    def expr_attr_names(self) -> dict[str, str]:
        return {'#pk': self.name_pk}

    def expr_attr_values(self) -> dict[str, Any]:
        return {':pk': self[self.name_pk]}

    def __add__(self, other: SortKey) -> 'PrimaryKey':
        pk = self.name_pk
        sk = other.sk
        kwargs = {
            pk: self[pk],
            sk: other,
            'table_name': self.table_name,
        }
        return PrimaryKey(**kwargs)


class PrimaryKey(Key):
    """Represents a composite key (partition key and sort key)"""

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
        if len(kwargs) != 2:
            raise TypeError(
                f'PrimaryKey() takes exactly two key=value arguments '
                f'({len(kwargs)} given)'
            )

        (
            (name_pk, value_pk),
            (name_sk, value_sk),
        ) = kwargs.items()
        super().__init__(**{name_pk: value_pk, name_sk: value_sk})

        self.name_pk = name_pk
        self.name_sk = name_sk
        self.table_name = table_name

    @property
    def sk(self) -> SortKey | str:
        return self[self.name_sk]

    def expr_attr_names(self) -> dict[str, str]:
        return {
            '#pk': self.name_pk,
            '#sk': self.name_sk,
        }

    def expr_attr_values(self) -> dict[str, Any]:
        return {
            ':pk': self[self.name_pk],
            ':sk': self[self.name_sk],
        }

    def __add__(self, other: Self | SortKey) -> 'PrimaryKeySet':
        if isinstance(other, PrimaryKey):
            return PrimaryKeySet((self, other))

        if isinstance(other, SortKey):
            if other.sk is None:
                raise ValueError('SortKey name is required')

            pk, sk = self.name_pk, other.sk
            kwargs = {pk: self[pk], sk: other}
            return PrimaryKeySet((self, PrimaryKey(**kwargs)))

        return NotImplementedError

    def __radd__(self, other: Any):
        if isinstance(other, PrimaryKeySet):
            return other + self

        return NotImplementedError


@dataclass(frozen=True)
class PrimaryKeySet:
    pairs: tuple[PrimaryKey, ...] = ()

    def __add__(self, other: PrimaryKey | SortKey) -> 'PrimaryKeySet':
        if not isinstance(other, (PrimaryKey, SortKey)):
            return NotImplementedError

        if isinstance(other, PrimaryKey):
            return PrimaryKeySet(pairs=self.pairs + (other,))

        if not self.pairs:
            raise ValueError('Cannot add SortKey to empty PrimaryKeySet')

        last_pair = self.pairs[-1]
        next_pair = PrimaryKey(
            **{
                last_pair.name_pk: last_pair[last_pair.name_pk],
                last_pair.name_sk: other,
            },
            table_name=last_pair.table_name,
        )

        if next_pair in self.pairs:
            return self

        return PrimaryKeySet(pairs=self.pairs + (next_pair,))
