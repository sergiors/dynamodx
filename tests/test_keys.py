from dynamodx.keys import PartitionKey, PrimaryKey, PrimaryKeySet, SortKey


def test_primary_key():
    pk = PrimaryKey(id='123', sk='abc')
    assert pk.expr_attr_names() == {'#pk': 'id', '#sk': 'sk'}
    assert pk.expr_attr_values() == {':pk': '123', ':sk': 'abc'}
    assert pk == {'id': '123', 'sk': 'abc'}


def test_sort_key():
    sk = SortKey('abc')
    assert sk == 'abc'

    sk_kw = SortKey(sk='abc', rename_key='alphabet')
    assert sk_kw == 'abc'
    assert sk_kw.rename_key == 'alphabet'


def test_partition_key():
    pk = PartitionKey(pk='123')
    assert pk.expr_attr_names() == {'#pk': 'pk'}
    assert pk.expr_attr_values() == {':pk': '123'}
    assert pk == {'pk': '123'}


def test_primary_key_set():
    kset = (
        PartitionKey(id='123')
        + SortKey(sk='ITEMS', rename_key='items')
        + SortKey(sk='USER', rename_key='user')
        + SortKey(sk='TRANSACTION#STATS', rename_key='stats')
    )

    assert kset == PrimaryKeySet(
        pairs=(
            PrimaryKey(id='123', sk=SortKey(sk='ITEMS')),
            PrimaryKey(id='123', sk=SortKey(sk='USER')),
            PrimaryKey(id='123', sk=SortKey(sk='TRANSACTION#STATS')),
        )
    )
