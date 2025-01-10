from nationwide_test.schema import Trie


def test_Trie_add() -> None:
    words = ["abc", "ab", "acb", "def"]
    vendors = ["v1", "v2", "v3", "v3"]
    trie = Trie()

    for ind in range(len(words)):
        trie.add(words[ind], vendors[ind])

    assert not trie.root.children["a"].is_end
    assert trie.root.children["a"].children["b"].is_end
    assert trie.root.children["a"].children["b"].children["c"].is_end
    assert trie.root.children["a"].children["c"].children["b"].is_end
    assert trie.root.children["d"].children["e"].children["f"].is_end


def test_Trie_search() -> None:
    words = ["abc", "ab", "acb", "def"]
    vendors = ["v1", "v2", "v3", "v3"]
    trie = Trie()

    for ind in range(len(words)):
        trie.add(words[ind], vendors[ind])

    assert trie.search("a") == ""
    assert trie.search("") == ""
    assert trie.search("c") == ""
    assert trie.search("abcd") == "v2"
    assert trie.search("abc") == "v2"
    assert trie.search("def") == "v3"
