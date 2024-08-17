from konlsearch import utility


def test_is_sorted():
    l1 = [1, 2, 3, 4]
    assert utility.is_sorted(l1)

    l2 = [1, 2, 4, 3]
    assert not utility.is_sorted(l2)
