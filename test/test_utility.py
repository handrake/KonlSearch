from konlsearch import utility

import pytest


def test_is_sorted():
    l1 = [1, 2, 3, 4]
    assert utility.is_sorted(l1) == True

    l2 = [1, 2, 4, 3]
    assert utility.is_sorted(l2) == False
