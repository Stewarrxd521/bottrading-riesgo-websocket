import json

from kline_ws import Candle, KlineStore, parse_kline_message


def _msg(symbol="BTCUSDT", t=60_000, o="1", c="2", closed=True):
    return json.dumps({
        "stream": f"{symbol.lower()}@kline_1m",
        "data": {"e": "kline", "s": symbol, "k": {
            "t": t, "T": t + 59_999, "s": symbol, "i": "1m",
            "o": o, "h": "3", "l": "0.5", "c": c, "v": "10", "x": closed,
        }},
    }, separators=(",", ":"))


def test_parse_ignores_open_candle_and_acks():
    assert parse_kline_message(_msg(closed=False)) is None
    assert parse_kline_message('{"result":null,"id":1}') is None


def test_parse_closed_candle():
    sym, candle = parse_kline_message(_msg(o="1.5", c="1.2"))
    assert sym == "BTCUSDT"
    assert candle.open == 1.5 and candle.close == 1.2
    assert not candle.bullish


def test_store_keeps_last_n_and_dedupes():
    st = KlineStore(history=2)
    for i in range(1, 5):
        st.add("X", Candle(i * 60_000, i * 60_000 + 59_999, 1, 1, 1, 1, 1))
    st.add("X", Candle(4 * 60_000, 4 * 60_000 + 59_999, 2, 2, 2, 2, 2))   # duplicado
    st.add("X", Candle(2 * 60_000, 2 * 60_000 + 59_999, 9, 9, 9, 9, 9))   # tardío
    got = st.closed("X")
    assert [c.open_time for c in got] == [3 * 60_000, 4 * 60_000]
    assert got[-1].open == 2


def test_last_closed_freshness():
    st = KlineStore(history=2)
    c = Candle(0, 59_999, 1, 1, 1, 1, 1)
    st.add("X", c)
    assert st.last_closed("X", now_ms=60_500) == c          # minuto recién cerrado
    assert st.last_closed("X", now_ms=125_000) is None      # se perdió un cierre
    assert st.last_closed("X", fresh=False, now_ms=999_999) == c
    assert st.last_closed("Y") is None
