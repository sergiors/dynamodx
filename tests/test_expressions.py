from decimal import Decimal

from dynamodx.expressions import (
    Add,
    Delete,
    Remove,
    Set,
    UpdateExpr,
    if_not_exists,
    list_append,
)


def test_update_expr_exclude_none():
    expr = UpdateExpr(
        Set(name='Juscelino Kubitschek'),
        Set(email='kubitschek@gov.br'),
        Set(phone=None),
        Add(emails={'kubitschek@gov.br'}),
    )
    assert expr == {
        'update_expr': (
            'SET #n_name = :v_name, #n_email = :v_email ADD #n_emails :v_emails'
        ),
        'expr_attr_names': {
            '#n_name': 'name',
            '#n_email': 'email',
            '#n_emails': 'emails',
        },
        'expr_attr_values': {
            ':v_name': 'Juscelino Kubitschek',
            ':v_email': 'kubitschek@gov.br',
            ':v_emails': {'kubitschek@gov.br'},
        },
    }


def test_update_expr_funcs():
    expr = UpdateExpr(
        Set(name='Juscelino Kubitschek'),
        Set(score=10, operand='+'),
        Set(points=if_not_exists(points=0)),
        Set(tags=list_append(tags=['python', 'aws'])),
        Add(score=Decimal(5)),
        Set(phone=None),
        Remove('quantity'),
        Remove('brand.name'),
        Delete(emails={'kubitschek@gov.br'}),
    )
    assert expr == {
        'update_expr': (
            'SET #n_name = :v_name, '
            '#n_score = #n_score + :v_score, '
            '#n_points = if_not_exists(#n_points, :v_points), '
            '#n_tags = list_append(#n_tags, :v_tags) '
            'ADD #n_score :v_score '
            'REMOVE #n_quantity, #n_brand_name '
            'DELETE #n_emails :v_emails'
        ),
        'expr_attr_names': {
            '#n_name': 'name',
            '#n_score': 'score',
            '#n_points': 'points',
            '#n_tags': 'tags',
            '#n_quantity': 'quantity',
            '#n_brand_name': 'brand.name',
            '#n_emails': 'emails',
        },
        'expr_attr_values': {
            ':v_name': 'Juscelino Kubitschek',
            ':v_score': Decimal('5'),
            ':v_points': 0,
            ':v_tags': ['python', 'aws'],
            ':v_emails': {'kubitschek@gov.br'},
        },
    }


def test_update_expr_sum():
    expr = UpdateExpr(
        Set(points=if_not_exists(points=1)),
        Set(attempts=if_not_exists(attempts=0) + 1),
        Set(score=if_not_exists(score=100) - 1),
    )
    assert expr == {
        'update_expr': (
            'SET #n_points = if_not_exists(#n_points, :v_points), '
            '#n_attempts = if_not_exists(#n_attempts, :v_attempts) + :v_attempts_r, '
            '#n_score = if_not_exists(#n_score, :v_score) - :v_score_r'
        ),
        'expr_attr_names': {
            '#n_points': 'points',
            '#n_attempts': 'attempts',
            '#n_score': 'score',
        },
        'expr_attr_values': {
            ':v_points': 1,
            ':v_attempts': 0,
            ':v_attempts_r': 1,
            ':v_score': 100,
            ':v_score_r': 1,
        },
    }

    assert UpdateExpr(
        Set(overall_score=if_not_exists(score=0) + 1),
    ) == {
        'update_expr': (
            'SET #n_overall_score = if_not_exists(#n_score, :v_score) + :v_score_r'
        ),
        'expr_attr_names': {
            '#n_overall_score': 'overall_score',
            '#n_score': 'score',
        },
        'expr_attr_values': {
            ':v_score': 0,
            ':v_score_r': 1,
        },
    }
