from dynamodx.keys import PartitionKey, PrimaryKey, SortKey


def test_primary_key():
    pk = PrimaryKey(id='123', sk='abc')
    assert pk.expr_attr_names() == {'#pk': 'id', '#sk': 'sk'}
    assert pk.expr_attr_values() == {':pk': '123', ':sk': 'abc'}
    assert repr(pk) == "PrimaryKey(id='123', sk='abc')"


def test_sort_key():
    sk = SortKey('abc')
    assert repr(sk) == "SortKey('abc')"

    sk_kw = SortKey(sk='abc')
    assert repr(sk_kw) == "SortKey(sk='abc')"


def test_partition_key():
    pk = PartitionKey(pk='123')
    assert pk.expr_attr_names() == {'#pk': 'pk'}
    assert pk.expr_attr_values() == {':pk': '123'}
    assert repr(pk) == "PartitionKey(pk='123')"


def test_primary_key_set():
    pk = (
        PartitionKey(id='123', table_name='pytest')
        + SortKey(sk='ITEMS', rename_key='items')
        + SortKey(sk='USER', rename_key='user')
        + SortKey(sk='TRANSACTION#STATS', rename_key='stats')
    )

    print(pk)
