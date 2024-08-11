# KonlSearch: Korean Natural Language Search Engine

This project aims to implement a minimal database that can be used as an embedded search engine.

## Quickstart

```python
>>> from konlsearch.search import KonlSearch
>>> from konlsearch.inverted_index import TokenSearchMode
>>> ks = KonlSearch("./test-db")
>>> index = ks.index("animation_title") # similar to creating DB
>>> # KonlIndex.index() is like inserting into DB
>>> index.index("귀환자의 마법은 특별해야 합니다")
1 # document is indexed in document_id 1
>>> index.index("마법소녀 따위는 이제 됐으니까.")
2
>>> index.index("일찍이 마법소녀와 악은 적대하고 있었다.")
3
>>> index.search(["마법"], TokenSearchMode.OR)
[1] # "마법" is indexed in document 1
>>> index.search(["마법소녀"], TokenSearchMode.OR)
[2, 3] # "마법소녀" is indexed in document 2, 3
>>> index.search(["마법소녀", "적대"], TokenSearchMode.AND) # matches only documents that have both "마법소녀" and "적대"
[3]
>>> index.search_suggestions("ㅈ") # searches all tokens that begin with 'ㅈ', useful for autocomplete
['적대', '적대하고']
>>> ks.close()
>>> ks.destroy() # deletes db
```

## Usage

More internal and advanced usage can be found under [test/test_konlsearch.py](test/test_konlsearch.py)

## Acknowledgements

- KonlSearch relies on [python-mecab-ko](https://github.com/jonghwanhyeon/python-mecab-ko) for tokenizing and [hangul-toolkit](https://github.com/bluedisk/hangul-toolkit) for decomposing Korean characters into consonants and vowels.
- [RocksDB](https://github.com/facebook/rocksdb) is used as a storage engine and [RocksDict](https://github.com/Congyuwang/RocksDict) for RockDB Python binding

## License

- [Apache-2.0](LICENSE)
