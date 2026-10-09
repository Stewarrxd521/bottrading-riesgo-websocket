import json
import time

import pytest

import state_store
from state_store import StateStore, StoreError, UpstashRedis
from tests.fake_upstash import HAVE_REDIS, FakeRedis, serve

needs_redis = pytest.mark.skipif(not HAVE_REDIS, reason="requiere redis-server")


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(state_store, "DEBOUNCE_S", 0.0)


@pytest.fixture
def r():
    if not HAVE_REDIS:
        pytest.skip("requiere redis-server")
    redis = FakeRedis()
    yield redis
    redis.close()


def make(backend, tmp_path, owner="A", doc=None, logs=None):
    doc = doc if doc is not None else {"positions": {}}
    fenced, box = [], []
    st = StateStore(backend, str(tmp_path / f"local-{owner}.json"), owner,
                    build_doc=lambda: (json.loads(json.dumps(doc)), box[0].peek_trades()),
                    log=(logs.append if logs is not None else (lambda m: None)),
                    on_fenced=lambda: fenced.append(owner))
    box.append(st)
    st._fenced_calls = fenced
    st._doc = doc
    return st


def own(st):
    """Toma el control, aplica el estado (como el bot) y habilita escrituras."""
    assert st._try_acquire()
    restored = st.take_restore()
    assert st.enable_writes()
    return restored


def test_acquire_write_then_close_removes_position_and_logs_trade(r, tmp_path):
    doc = {"positions": {"XUSDT": {"symbol": "XUSDT", "trade_id": 7}}}
    st = make(r, tmp_path, doc=doc)
    assert st._try_acquire()
    assert not st.can_open() and not st.can_act()  # aún no aplicó el estado
    assert st.take_restore() == (None, [], "vacío (sin estado previo)")
    assert st.enable_writes()
    assert st._flush()
    assert r.state()["positions"]["XUSDT"]["trade_id"] == 7
    assert r.owner().startswith("A|") and 0 < r.cmd("PTTL", "botshort:owner") <= 120_000
    assert st.can_open() and st.can_act()
    doc["positions"].clear()                       # se cierra la posición
    st.append_trade({"trade_id": 7, "closed_at_ts": 1.0, "pnl": 1.5})
    assert st._flush()
    assert r.state()["positions"] == {}
    assert [t["trade_id"] for t in r.trades()] == [7]


def test_clean_shutdown_releases_and_next_instance_recovers_at_once(r, tmp_path):
    a = make(r, tmp_path, "A", doc={"positions": {"X": {"symbol": "X"}}, "trade_id_seq": 9})
    own(a)
    a.append_trade({"trade_id": 3, "closed_at_ts": 1.0})
    a._flush()
    assert a.close()                               # SIGTERM: guardado final + libera
    assert r.owner().endswith("|released") and not a.can_act()
    b = make(r, tmp_path, "B")
    doc, trades, source = own(b)
    assert doc["trade_id_seq"] == 9 and "X" in doc["positions"]
    assert trades == [{"trade_id": 3, "closed_at_ts": 1.0}] and source == "upstash"


def test_new_instance_waits_while_old_one_holds_control(r, tmp_path):
    a = make(r, tmp_path, "A", doc={"positions": {"X": {"symbol": "X"}}})
    own(a)
    a._flush()
    b = make(r, tmp_path, "B")
    assert b._try_acquire() is False               # deploy: la vieja sigue operando
    assert b.status()["mode"] == "esperando control" and b.holder == "A"
    assert not b.can_act() and not b.can_open() and b.take_restore() is None
    assert b._next_try - time.time() <= state_store.WAIT_POLL_S + 0.1
    a._doc["positions"]["Y"] = {"symbol": "Y"}     # la vieja abre algo mientras la nueva espera
    a.mark_dirty()
    assert a._flush() and a.can_act()
    assert a.close()
    doc, _, _ = own(b)
    assert set(doc["positions"]) == {"X", "Y"}     # nada de lo que hizo la vieja se pierde


def test_lease_expiry_standby_and_recovery(r, tmp_path, monkeypatch):
    monkeypatch.setattr(state_store, "OWNER_LEASE_S", 0.3)
    a = make(r, tmp_path, "A", doc={"positions": {"X": {"symbol": "X"}}})
    own(a)
    a._flush()
    time.sleep(0.4)                                # A deja de renovar (colgada / sin red)
    b = make(r, tmp_path, "B", doc={"positions": {"Z": {"symbol": "Z"}}})
    doc, _, _ = own(b)
    assert "X" in doc["positions"]
    a._doc["positions"]["LATE"] = {"symbol": "LATE"}
    a.mark_dirty()
    assert a._flush() is False                     # A se entera en su siguiente escritura
    assert a.fenced and not a.can_act() and a.status()["mode"] == "standby"
    assert a._fenced_calls == ["A"]
    assert b._flush() and "Z" in r.state()["positions"]
    time.sleep(0.4)                                # ahora muere B
    assert a._try_acquire()                        # A retoma el control
    doc, _, _ = a.take_restore()
    assert set(doc["positions"]) == {"Z"}          # con lo que dejó B
    assert a.enable_writes() and a.can_act() and not a.fenced


def test_expired_lease_nobody_took_is_renewed_by_its_owner(r, tmp_path, monkeypatch):
    monkeypatch.setattr(state_store, "OWNER_LEASE_S", 0.3)
    st = make(r, tmp_path)
    own(st)
    st._flush()
    time.sleep(0.4)                                # Upstash no respondió un rato
    assert r.owner() is None
    st._doc["positions"]["Y"] = {"symbol": "Y"}
    st.mark_dirty()
    assert st._flush() and st.can_act() and r.owner().startswith("A|")


def test_delayed_older_write_is_ignored(r, tmp_path):
    st = make(r, tmp_path, doc={"positions": {"NEW": {}}})
    own(st)
    st._flush()
    old_seq = st._seq - 1
    res = r.cmd("EVAL", state_store._WRITE_LUA, 3, "botshort:owner", "botshort:state", "botshort:trades",
                "A", old_seq, 60000, "0", '{"positions":{"OLD":{}}}', 5000)
    assert res == 2
    assert "NEW" in r.state()["positions"]


def test_other_owner_rejects_writes(r, tmp_path):
    st = make(r, tmp_path)
    own(st)
    r.cmd("SET", "botshort:owner", "B|1", "PX", 60000)
    st.mark_dirty()
    assert st._flush() is False and st.fenced
    assert st.enable_writes() is False             # sin control no vuelve a operar
    assert r.owner() == "B|1"


def test_failures_keep_changes_and_stop_entries_until_saved(r, tmp_path):
    doc = {"positions": {"X": {"symbol": "X"}}}
    st = make(r, tmp_path, doc=doc)
    own(st)
    r.fail = True
    st.append_trade({"trade_id": 1, "closed_at_ts": 1.0})
    with pytest.raises(StoreError):
        st._flush()
    assert st._pending_critical() and len(st._trades) == 1
    st._crit_since -= state_store.CRIT_STALL_S + 1
    assert not st.can_open() and st.can_act()      # no abre, pero sí sigue cerrando
    r.fail = False
    assert st._flush()
    assert not st._pending_critical() and not st._trades and st.can_open()
    assert r.state()["positions"]["X"]["symbol"] == "X"
    assert [t["trade_id"] for t in r.trades()] == [1]


def test_local_copy_wins_only_if_newer_and_from_same_writer(r, tmp_path):
    r.cmd("SET", "botshort:state", json.dumps({"saved_at": 100, "owner": "old", "positions": {"OLD": {}}}))
    st = make(r, tmp_path)
    (tmp_path / "local-A.json").write_text(
        json.dumps({"saved_at": 200, "owner": "old", "positions": {"NEW": {}}}))
    doc, _, source = own(st)
    assert "NEW" in doc["positions"] and "copia local" in source
    st.close()

    st2 = make(r, tmp_path, "C")
    (tmp_path / "local-C.json").write_text(
        json.dumps({"saved_at": 9e9, "owner": "otra", "positions": {"STALE": {}}}))
    doc, _, source = own(st2)
    assert "STALE" not in doc["positions"] and source == "upstash"


def test_soft_changes_ride_the_heartbeat(r, tmp_path):
    st = make(r, tmp_path)
    own(st)
    st._flush()
    writes, calls = st.writes, r.calls
    st._doc["mfe"] = 1.0
    st.mark_dirty(critical=False)
    st._flush()
    assert st.writes == writes and r.calls == calls        # MFE/MAE no se escribe enseguida
    st.last_ok -= state_store.HEARTBEAT_S + 1
    st._flush()
    assert st.writes == writes + 1 and r.state()["mfe"] == 1.0
    saved_at = r.state()["saved_at"]
    st.mark_dirty(critical=False)                  # sin cambios reales: solo latido
    st.last_ok -= state_store.HEARTBEAT_S + 1
    calls = r.calls
    st._flush()
    assert r.calls == calls + 1 and r.state()["saved_at"] == saved_at
    assert time.time() - st.last_ok < 1


def test_can_open_requires_recent_confirmation(r, tmp_path):
    st = make(r, tmp_path)
    own(st)
    st._flush()
    assert st.can_open()
    st.last_ok = time.time() - state_store.OWNER_LEASE_S / 2 - 1
    assert not st.can_open() and st.can_act()      # sin Upstash: no abre, pero sí cierra


def test_close_releases_even_before_state_is_applied(r, tmp_path):
    st = make(r, tmp_path)
    assert st._try_acquire()
    assert st.close() and r.owner().endswith("|released")


def test_delayed_write_after_release_is_rejected(r, tmp_path):
    a = make(r, tmp_path, "A", doc={"positions": {"X": {"symbol": "X"}}})
    own(a)
    a._flush()
    late_seq = a._seq + 1                          # una escritura que se quedó en la red
    a._doc["positions"].clear()                    # A cierra X y se apaga
    a.mark_dirty()
    assert a.close()
    res = r.cmd("EVAL", state_store._WRITE_LUA, 3, "botshort:owner", "botshort:state", "botshort:trades",
                "A", late_seq, 120000, "0", '{"positions":{"X":{}}}', 5000)
    assert res == 3 and r.state()["positions"] == {}
    b = make(r, tmp_path, "B")
    doc, _, _ = own(b)                             # B no espera a una instancia muerta
    assert doc["positions"] == {}
    res = r.cmd("EVAL", state_store._WRITE_LUA, 3, "botshort:owner", "botshort:state", "botshort:trades",
                "A", late_seq + 1, 120000, "0", '{"positions":{"X":{}}}', 5000)
    assert res == 0 and r.owner().startswith("B|")


def test_final_save_retries_while_upstash_fails(r, tmp_path, monkeypatch):
    import threading
    a = make(r, tmp_path, "A", doc={"positions": {"X": {"symbol": "X"}}})
    own(a)
    a._flush()
    a._doc["positions"].clear()
    a.append_trade({"trade_id": 1, "closed_at_ts": 1.0})
    r.fail = True
    threading.Timer(1.5, lambda: setattr(r, "fail", False)).start()
    assert a.close()                               # falla, reintenta y acaba guardando
    assert r.state()["positions"] == {} and [t["trade_id"] for t in r.trades()] == [1]
    assert r.owner().endswith("|released")


def test_history_duplicates_are_dropped_on_read(r, tmp_path):
    row = json.dumps({"trade_id": 5, "closed_at_ts": 2.0})
    r.cmd("RPUSH", "botshort:trades", row, row, json.dumps({"trade_id": 6, "closed_at_ts": 3.0}))
    st = make(r, tmp_path)
    _, trades, _ = own(st)
    assert [t["trade_id"] for t in trades] == [5, 6]


def test_local_only_mode_restores_local_copy(tmp_path):
    (tmp_path / "local-A.json").write_text(json.dumps({"saved_at": 1, "positions": {"KEEP": {}}}))
    st = make(None, tmp_path, doc={"positions": {"KEEP": {}, "MORE": {}}})
    st.start()
    assert st.acquired.wait(1)
    doc, _, source = st.take_restore()
    assert "KEEP" in doc["positions"] and source == "copia local"
    assert st.enable_writes() and st.can_open()
    assert st.close()
    saved = json.loads((tmp_path / "local-A.json").read_text())
    assert set(saved["positions"]) == {"KEEP", "MORE"} and saved["owner"] == "A"


@needs_redis
def test_http_client_against_fake_upstash(r, tmp_path):
    srv = serve(r, 0)
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        client = UpstashRedis(url, "test-token")
        st = make(client, tmp_path, doc={"positions": {"X": {"symbol": "X"}}})
        own(st)
        assert st._flush()
        assert r.state()["positions"]["X"]["symbol"] == "X"
        b = make(client, tmp_path, "B")
        assert b._try_acquire() is False and b.holder == "A"
        with pytest.raises(StoreError, match="401"):
            UpstashRedis(url, "wrong").cmd("GET", "k")
    finally:
        srv.shutdown()
