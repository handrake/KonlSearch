from konlsearch.search import KonlSearch

ks = KonlSearch("./test-db")

document = '''
일본의 라이트 노벨. 작가는 와타리 와타루, 삽화가는 퐁칸⑧.

작가 와타리의 전작 아야카시가타리가 망한 후 당시 대세인 러브 코미디에 맞춰 쓴 작품이다.

원래 제목은 『내 청춘 러브 코미디가 잘못된 건(俺の青春ラブコメが間違っている件)』이었다고 한다. 공식 약칭은 『오레가이루(俺ガイル)』. 응모를 받은 것 중에 선별된 『하마치(はまち)』라는 약칭을 애니 방영 전에 쓰고 있었지만 애니판이 '오레가이루'를 공식으로 밀기 시작하며 보기 드물어졌다. 한국에서는 긴 문장형의 제목을 흔히 축약하는 방식으로 '내청코'나 '내청춘' 내지는 '청춘럽코' 또는 '역내청' 등으로 불린다.

2020년 3월에 전세계 누적 판매량 1천만 부를 돌파하는 등 엄청난 인기를 얻었고, 미디어 믹스화도 다양하게 이뤄지고 있다. 원작은 완결되었지만, 관련 굿즈 및 외전이 꾸준히 발매되고 있어 인기는 현재진행형이다.
'''

index_name = "document"

index = ks.create_or_get_index(index_name)

document_id = index.index(document)

print(document_id, index.get(document_id))

index.delete(document_id)
