from konlsearch.search import KonlSearch
from konlsearch.index import TokenSearchMode, IndexingStatusCode

import pytest


@pytest.fixture
def konl_search():
    ks = KonlSearch("./test-db-kowiki")

    yield ks

    ks.close()
    ks.destroy()


@pytest.fixture
def index(konl_search):
    f_read = open("./test/kowiki-20240801-all-titles-in-ns0", "r")

    index_name = "kowiki"
    index = konl_search.index(index_name)

    while 1:
        line = f_read.readline()

        if not line:
            break

        title = line.replace("_", " ").replace("\n", "")
        r = index.index(title)

        assert r.status_code == IndexingStatusCode.SUCCESS

    yield index

    index.close()

    f_read.close()


def test_search(index):
    document_ids = index.search(["건담"], TokenSearchMode.OR)

    assert document_ids == [95848, 152604, 152606, 156696, 156697, 156698,
                            156699, 156700, 156701, 156716, 156717, 156718,
                            156719, 156720, 156721, 156722, 156764, 160655,
                            179125, 179127, 186998, 187014, 187015, 187389,
                            188761, 188762, 188763, 199473, 200765, 206579,
                            216673, 216674, 216675, 216676, 216677, 216678,
                            216679, 216680, 216681, 216682, 216683, 216684,
                            216685, 216686, 216687, 216688, 216689, 216690,
                            216691, 216692, 216693, 216694, 216695, 216696,
                            216697, 216698, 216699, 216700, 216701, 216702,
                            216703, 216704, 216705, 216706, 216707, 216708,
                            216709, 216710, 216711, 216712, 216713, 216714,
                            216715, 216716, 216717, 216718, 216719, 216720,
                            216721, 216722, 216723, 216724, 216725, 216726,
                            216727, 216728, 216729, 216730, 216731, 216732,
                            216733, 216734, 216735, 216736, 216737, 216738,
                            216739, 216740, 216741, 216742, 216743, 216744,
                            216745, 216746, 216747, 216748, 216749, 216750,
                            216751, 216752, 216753, 216754, 216755, 216756,
                            216757, 216758, 216759, 216760, 216761, 217341,
                            220666, 284222, 284523, 291652, 291653, 291654,
                            291661, 291667, 291668, 291669, 291670, 291671,
                            291672, 291673, 291674, 291675, 291676, 291677,
                            291678, 291679, 291680, 291681, 291682, 291683,
                            291684, 291685, 291686, 291687, 291688, 291689,
                            291690, 291691, 291692, 291693, 291694, 291695,
                            291696, 291697, 291698, 291699, 291700, 291701,
                            291702, 291703, 291704, 291705, 291706, 291707,
                            291708, 291709, 291710, 291711, 291712, 291713,
                            291714, 291715, 291716, 291717, 291718, 291719,
                            291720, 291721, 291722, 291723, 291724, 291725,
                            291726, 291727, 291728, 291729, 291730, 291731,
                            291732, 291733, 291734, 343940, 344135, 357707,
                            358069, 360205, 360206, 362165, 367127, 403073,
                            411476, 411643, 411778, 416470, 416476, 421683,
                            435016, 445797, 445798, 445828, 447149, 450796,
                            462152, 471533, 476276, 478250, 480730, 508570,
                            509319, 525607, 563926, 572394, 586610, 597096,
                            609482, 621243, 629587, 630825, 631665, 631676,
                            654585, 679428, 681711, 681712, 681773, 690632,
                            693572, 704296, 732218, 760829, 767506, 769662,
                            769678, 793950, 807888, 807911, 807912, 815388,
                            827800, 827801, 827802, 827803, 827804, 827805,
                            827806, 827807, 873176, 873179, 874078, 921365,
                            938058, 1031886, 1035728, 1035729, 1035730,
                            1035731, 1035750, 1037818, 1037819, 1037820,
                            1037821, 1037822, 1037830, 1038655, 1048624,
                            1082362, 1082374, 1086545, 1098166, 1104814,
                            1106491, 1127483, 1127524, 1129361, 1162309,
                            1164050, 1164051, 1164061, 1204994, 1208005,
                            1213136, 1213137, 1258330, 1262209, 1267823,
                            1268958, 1268961, 1269598, 1296307, 1296308,
                            1324490, 1324513, 1324514, 1324515, 1326517,
                            1361832, 1362674, 1363216, 1363240, 1374916,
                            1375120, 1375121, 1385363, 1385364, 1385365,
                            1385644, 1385645, 1385646, 1393475, 1393483,
                            1394301, 1395020, 1395039, 1398443, 1417014,
                            1418225, 1424925]
