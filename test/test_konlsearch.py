from konlsearch.search import KonlSearch
from konlsearch.index import TokenSearchMode
from konlsearch.set import KonlSet, KonlSetWriteBatch
from konlsearch.dict import KonlDict, KonlDictWriteBatch

import pytest
import rocksdict

title_document = '''
거신병 도쿄에 나타나다
결혼한다는 게, 정말인가요
경비실에서 안내방송 드립니다
고교생을 환불해 주세요
고래의 아이들은 모래 위에서 노래한다
공주님 "고문"의 시간입니다
괴롭히지 말아요, 나가토로 양
교정 뒤에는 천사가 묻혀 있다
귀환자의 마법은 특별해야 합니다
그 비스크 돌은 사랑을 한다
그 사람의 위장에는 내가 부족해
그날 본 꽃의 이름을 우리는 아직 모른다
그대들은 어떻게 살 것인가
그래도 마을은 돌아간다
그래도 아유무는 다가온다
그래도 우리는 게임을 만든다
그럼에도 세상은 아름답다
금단의 낙원아일랜드 ……에서 이런 일이나 저런 일 해버리거나 당해버리거나 해서 어쩌면 눈물 똑 떨어질 일이나 할짝거려질 일까지 있을지도 모르니까 싫~엉! 넵튠 부끄러워~ 같은!
깨끗하게 해주시겠어요?
나는 100만 명의 목숨 위에 서 있다
나는 공주님이 될 수 없다
나를 화나게 만들지 마 준교수
나에게 천사가 내려왔다!
나에게만 상냥한 깡패 소녀
나왔습니다! 파워퍼프걸Z!
나의 밤은 당신의 낮보다 아름답다
나의 백합은 일입니다!
날아라 호빵맨
내 여배우가 가장 음란하다
내가 인기 없는 건 아무리 생각해도 너희들 탓이야!
내가 인기 있어서 어쩌자는 거야
내가 좋아했던 여자애를 먹어치우고 그녀로 변한 괴물과 사귀고 있다.
내세에는 남남이 좋겠어
너를 너무너무너무너무 좋아하는 100명의 그녀
너만은 죽어도 사양할게
너에게 사랑받아 아팠다
너의 눈물을 다 마시고 싶어
다이아몬드는 부서지지 않는다
키시베 로한은 움직이지 않는다
덤벨 몇 킬로까지 들 수 있어?
데미는 이야기하고 싶어
서큐버스 선생님은 야한 짓을 하고 싶어
도망치는 건 부끄럽지만 도움이 된다
동거인 사노 군은 그저 유능한 담당 편집자일 뿐입니다
두 번 다시 셀카 안 보내줘!
등자는 반투명하게 다시 잠든다
뜬금없지만, 내일 결혼합니다
룸메가 관심법을 쓰는 건에 대하여
마법소녀 따위는 이제 됐으니까.
만지지 마세요 코테사시군
목소리를 못 내는 소녀는 「그녀가 너무 착하다」고 생각한다
물은 바다를 향해 흐른다
미스터리라 하지 말지어다
밤의 해파리는 헤엄칠 수 없어
배신자는 내 이름을 알고 있다
별똥별이 떨어지는 그 곳에서 기다려
볼룸에 오신 것을 환영합니다
북한과 미국, 누가 이길까?
사검씨는 곧잘 흔들린다
사에키 씨는 잠들어 있어
사와타리 이즈미 쟁탈전 시리즈! 절규 시험! 비탕 특급 토호쿠 뜨거운 연기 시어머니 살인 욕망에 불타는 엘리트 가정 마돈나 교사의 흐트러진 맨션 경영의 상속 싸움을 가정부는 봤다!
소꿉친구가 너무 잘생겨서 거부할 수 없어
소꿉친구로는 참을 수 없어
소꿉친구하곤 러브 코미디를 할 수 없어
숨을 쉴 수 없는 건 네 탓이야
시골에 돌아가니 유독 잘 따르는 갈색 포니테일 쇼타가 있다
싫어할 거예요, 사야마 군!
심부름꾼 사이토 씨, 이세계에 가다
아오노 군에게 닿고 싶으니까 죽고 싶어
악마에 입문했습니다! 이루마 군
안드로이드는 경험인 수에 들어가나요??
안타깝지만 모험의 서는 마왕의 것이 되었습니다
야스지의 포르노라마 해버려!!
양치기 소년은 오늘도 거짓말을 되풀이한다
어떤 게 사랑인지 모르겠어
어서오세요, 305호에!
어제 뭐 먹었어?
어째서 토도인 세이야 16세는 여자친구가 안 생기는 것인가?
얼굴만으론 좋아할 수 없어요
여동생과 그 친구가 너무 에로해서 내 사타구니가 위험해
여우는 같은 덫에 두 번 걸리지 않는다
극장판 에그엔젤 코코밍: 푸르밍과 두근두근 코코밍 세계
오육칠의 괴물은 반드시 죽는다
오키나와에서 좋아하게 된 아이가 사투리가 심해서 너무 괴로워
오타쿠에게 사랑은 어려워
용사다 시리즈
용왕님의 셰프가 되었습니다
우리 남동생들이 죄송합니다
음란한 아오는 공부를 할 수 없어
이 미술부에는 문제가 있다!
이 중에 1명, 내 신부가 있다
이세계 호색무쌍록 ~이세계 전생의 지식과 힘을, 그저 xxxx를 위해서 쓴다~
이과가 사랑에 빠졌기에 증명해보았다
이렇게 귀여운 간첩은 어디에 신고하나요?
이별의 아침에 약속의 꽃을 장식하자
이 힐러, 귀찮아
일찍이 마법소녀와 악은 적대하고 있었다.
자고로 영웅은 소년에게서 나온다
저, 이세계에서 노예가 되어 버렸어요 심지어 주인님은 성격이 나쁜 엘프 여왕님! 너무 무능해서 매번 욕만 먹지만 동료 오크가 치유계고 마을의 엘프는 귀여워서 상당히 즐기고 있는 저랍니다.
전생했더니 내가 히로인이고 그 녀석이 용사였다
전생했더니 초반에 죽는 중간보스였다 ~히로인을 권속화해서 살아남는다~
정글은 언제나 맑음 뒤 흐림
정신병동에도 아침이 와요
종말 트레인은 어디로 향하나?
좋아하는 애가 안경을 깜박했다
주문은 토끼입니까?
죽고 싶을만큼 한심한 날들이 죽고 싶을만큼 한심해서 죽도록 죽고 싶지 않은 날들
쪼그만 선배가 너무 귀여워
최근 이 세계는 나만의 것이 되었습니다……
최애가 부도칸에 가 준다면 난 죽어도 좋아
츠다 군은 마스다 쌤과 사이가 나쁘다
카구야 님은 고백받고 싶어 ~천재들의 연애 두뇌전~
케로로의 온천에서 일어난 살인사건 연기 속을 떠도는 우주에서 제일 불쌍한 남매의 영혼 오빠가 만든 수납장 온천 들어가려고 했더니 한쪽 발밖에 담글 수 없어 충격으로 미끄러진 오빠가 기절할 때 동생의 눈물이 춤을 추며 온천에 떨어진다. 입니다.
코모리 양은 거절하지 못해!
코미 양은 커뮤증입니다
쿠보 양은 나를 내버려두지 않아
클론 파트너와 연애라는 게 말이 돼?
타나카 군은 항상 나른해
타다 군은 사랑을 하지 않는다
평화로운 나라의 시마자키에게
하지만 마왕님은 그를 싫어하는걸
한때는 신이었던 짐승들에게
향기로운 꽃은 늠름하게 핀다
행복은 먹고 자고 기다리고
형 친구지만 신부가 되어줘
후지마루 리츠카는 잘 모르겠다
흡혈귀는 툭하면 죽는다
히메노는 공주가 되고 싶지 않아
Don't Hug Me I'm Scared
If the Emperor had a Text-to-Speech Device
My Life as a Teenage Robot
SEX로 레벨업! 만렙은 어디까지?!
'''

titles = [title for title in title_document.split("\n") if title != '']


@pytest.fixture(scope="session", autouse=True)
def print_titles():
    print("\n")

    for i, title in enumerate(titles):
        print(f'{i+1}: {title}')

    print("\n")


@pytest.fixture
def konl_search():
    ks = KonlSearch("./test-db")

    yield ks

    ks.close()
    ks.destroy()


@pytest.fixture
def index(konl_search):
    index_name = "title"
    index = konl_search.index(index_name)

    for title in titles:
        index.index(title)

    yield index

    index.close()


def test_search_mode_or(index):
    document_ids = index.search(["같은", "비스크"], TokenSearchMode.OR)

    assert document_ids == [10, 18, 81]


def test_search_mode_and(index):
    document_ids = index.search(["마법", "특별"], TokenSearchMode.AND)

    assert document_ids == [9]


def test_search_mode_phrase(index):
    document_ids = index.search(["마법", "특별"], TokenSearchMode.PHRASE)

    assert document_ids == [9]

    document_ids = index.search(["특별", "마법"], TokenSearchMode.PHRASE)

    assert document_ids == []


def test_index_writebatch(index):
    index.toWriteBatch().index("기동전사 건담")

    assert len(index) == 133


def test_index_len(index):
    assert len(index) == 132


def test_index_get(index):
    r = index.get(10)

    assert r["id"] == 10 and r["document"] == '그 비스크 돌은 사랑을 한다'


def test_index_get_all(index):
    result = index.get_all()

    index.delete(10)
    index.delete(12)

    assert len(result) == 132 and len(index) == 130


def test_index_get_range(index):
    index.delete(20)

    result = index.get_range(10, 20)

    document_ids = [document["id"] for document in result]

    assert document_ids == [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]


def test_index_get_multi(index):
    result = index.get_multi([10, 15, 20, 1000])

    document_ids = [document["id"] for document in result]

    assert document_ids == [10, 15, 20]


def test_inverted_index_delete(index):
    token = "다이아몬드"

    inverted = index._inverted_index

    assert inverted[token] == {38}

    inverted.delete(38, {token})

    assert token not in inverted


def test_set(index):
    s = KonlSet(index._cf, "test")

    s.add("1")
    s.add("2")
    s.add("3")

    assert list(s.items()) == ["1", "2", "3"]
    assert len(s) == 3

    s.remove("1")

    assert list(s.items()) == ["2", "3"]
    assert len(s) == 2

    s.remove("1")

    assert list(s.items()) == ["2", "3"]
    assert len(s) == 2

    s.remove("2")

    assert list(s.items()) == ["3"]
    assert len(s) == 1

    s.remove("3")

    assert list(s.items()) == []
    assert len(s) == 0


def test_set_writebatch(index):
    wb1 = rocksdict.WriteBatch()
    cf_handle = index._db.get_column_family_handle(index._name)
    wb1.set_default_column_family(column_family=cf_handle)
    s_wb = KonlSetWriteBatch(wb1, "test")

    s_wb.add("1")
    s_wb.add("2")
    s_wb.add("3")

    index._cf.write(wb1)

    s = KonlSet(index._cf, "test")

    assert list(s.items()) == ["1", "2", "3"]
    assert len(s) == 3

    wb2 = rocksdict.WriteBatch()
    wb2.set_default_column_family(column_family=cf_handle)
    s_wb = KonlSetWriteBatch(wb2, "test")

    s_wb.remove("1")

    index._cf.write(wb2)

    s = KonlSet(index._cf, "test")

    assert list(s.items()) == ["2", "3"]
    assert len(s) == 2


def test_dict(index):
    d = KonlDict(index._cf, "test")

    d["a"] = "1"
    d["b"] = "2"
    d["c"] = "3"

    assert list(d.items()) == [("a", "1"), ("b", "2"), ("c", "3")]
    assert len(d) == 3

    del d["a"]

    assert list(d.items()) == [("b", "2"), ("c", "3")]
    assert len(d) == 2

    del d["c"]

    assert list(d.items()) == [("b", "2")]
    assert len(d) == 1

    del d["b"]

    assert list(d.items()) == []
    assert len(d) == 0


def test_dict_writebatch(index):
    wb1 = rocksdict.WriteBatch()
    cf_handle = index._db.get_column_family_handle(index._name)
    wb1.set_default_column_family(column_family=cf_handle)
    d_wb = KonlDictWriteBatch(wb1, "test")

    d_wb["a"] = "1"
    d_wb["b"] = "2"
    d_wb["c"] = "3"

    index._cf.write(wb1)

    d = KonlDict(index._cf, "test")

    assert list(d.items()) == [("a", "1"), ("b", "2"), ("c", "3")]
    assert len(d) == 3

    wb2 = rocksdict.WriteBatch()
    wb2.set_default_column_family(column_family=cf_handle)
    d_wb = KonlDictWriteBatch(wb2, "test")

    del d_wb["a"]

    index._cf.write(wb2)

    d = KonlDict(index._cf, "test")

    assert list(d.items()) == [("b", "2"), ("c", "3")]
    assert len(d) == 2


def test_trie_suggestion(index):
    prefix = "특"

    suggestions = index.search_suggestions(prefix)

    assert suggestions == ["특급", "특별", "특별해야"]


def test_get_all_indexes(konl_search, index):
    indexes = sorted(konl_search.get_all_indexes())

    assert indexes == ["title"]
