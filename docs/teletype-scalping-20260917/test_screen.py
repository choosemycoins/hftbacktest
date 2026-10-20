import numpy as np
import pytest

from screen import evaluate_order, returns_bps, levels, envelope_complete, next_decision, replay


def tape(n=30):
    c = np.full(n, 100.0)
    return c, c.copy(), c.copy()


def run(c, h, l, **kw):
    args = dict(side=1, limit=99., take=101., stop=98., active=2,
                ttl=3, hold=5, lag=1, eps=0.0001)
    args.update(kw)
    return evaluate_order(c, h, l, **args)


def test_signal_bar_cannot_fill_and_latency_matters():
    c,h,l=tape(); l[1]=98.5
    assert run(c,h,l)[0] == -1
    l[2]=98.5
    assert run(c,h,l)[0] == 2
    assert run(c,h,l,active=3)[0] == -1


def test_touch_is_not_a_penetration():
    c,h,l=tape(); l[2]=99
    assert run(c,h,l)[0] == -1
    l[2]=98.995
    assert run(c,h,l)[0] == -1
    assert run(c,h,l,eps=0)[0] == 2


def test_entry_bar_profit_is_not_credited():
    c,h,l=tape(); l[2]=98.5; h[2]=102
    r=run(c,h,l)
    assert r[2] == 3
    assert r[1] == 8


def test_stop_wins_ambiguous_bar_and_uses_delayed_actual_price():
    c,h,l=tape(); l[2]=98.5; l[3]=97; h[3]=102; c[4]=96
    r=run(c,h,l)
    assert r[2] == 2
    assert r[1] == 4
    assert r[3] == 96
    assert r[5] == 1


def test_stop_checked_on_entry_bar():
    c,h,l=tape(); l[2]=97; c[3]=96
    r=run(c,h,l)
    assert r[0] == 2 and r[1] == 3 and r[2] == 2


def test_take_fills_at_limit_not_favourable_extreme():
    c,h,l=tape(); l[2]=98.5; h[3]=103
    r=run(c,h,l)
    assert r[2] == 1 and r[3] == 101


def test_short_mirror_and_gap_risk():
    c,h,l=tape(); h[2]=101.5; h[3]=103; c[4]=105
    r=run(c,h,l,side=-1,limit=101,take=99,stop=102)
    assert r[2] == 2 and r[3] == 105


def test_fees_are_paid_on_both_actual_notionals():
    gross,net=returns_bps(1,100,110,1,.001,.0001)
    assert gross == pytest.approx(1000)
    assert net == pytest.approx(979)
    _,net_slip=returns_bps(1,100,110,3,.001,.0001)
    assert net_slip == pytest.approx((109.989/100-1-.001*(1+109.989/100))*1e4)
    assert returns_bps(1,100,110,3,0,0)[1] == pytest.approx(gross)


def test_gap_envelope_and_nonoverlap():
    bad=np.zeros(101,dtype=np.int64); bad[50:]=1
    assert not envelope_complete(bad,10,60)
    assert envelope_complete(bad,10,40)
    assert next_decision(120,125,1020) == 1080


def test_levels_only_use_closed_bars():
    c,h,l=tape(4000)
    a=levels(c,h,l,1800,0,1)
    h[1800:]=10000; l[1800:]=1; c[1800:]=1000
    assert levels(c,h,l,1800,0,1) == a
    assert a == pytest.approx((99,99.99,98.01,900))


def test_range_bounds_and_trend_filter():
    c,h,l=tape(4000);h[:1800]=102;l[:1800]=98
    a=levels(c,h,l,1800,1,1)
    assert a == pytest.approx((98.4,100,96.4,1800))
    c[1799]=102
    assert levels(c,h,l,1800,1,1)[0] == 0


def oracle(c,h,l,side,limit,take,stop,active,ttl,hold,lag,eps):
    fills=[i for i in range(active,active+ttl) if
           (l[i]<limit*(1-eps) if side==1 else h[i]>limit*(1+eps))]
    if not fills:return (-1,-1,0,0.,0.,0)
    fi=fills[0]
    for j in range(fi,fi+hold+1):
        sl=(l[j]<=stop if side==1 else h[j]>=stop)
        tp=j>fi and (h[j]>take*(1+eps) if side==1 else l[j]<take*(1-eps))
        if sl:
            return(fi,j+max(1,lag),2,c[j+max(1,lag)],c[fi+hold],int(tp))
        if tp:return(fi,j,1,take,c[fi+hold],0)
    return(fi,fi+hold+max(1,lag),3,c[fi+hold+max(1,lag)],c[fi+hold],0)


def test_independent_oracle_on_seeded_paths():
    rng=np.random.default_rng(9)
    for side in (1,-1):
        for _ in range(100):
            c=100+np.cumsum(rng.normal(0,.4,30));h=c+rng.uniform(0,1,30);l=c-rng.uniform(0,1,30)
            args=(side,100-side,100+side,100-3*side,2,3,5,2,.0001)
            assert evaluate_order(c,h,l,*args) == pytest.approx(oracle(c,h,l,*args))


def test_replay_rejects_a_real_missing_bar_and_never_overlaps():
    c,h,l=tape(7200);l[:]=98.5
    d=np.arange(1800,4800,60,dtype=np.int64)
    params=np.tile(np.array([99.,101.,98.,900.]),(len(d),1))
    bad=np.zeros(7201,dtype=np.int64)
    rows,_,_,_=replay(c,h,l,0,d,params,bad,0,1,1,1,.0001)
    assert len(rows)>1
    assert np.all(rows[1:,4]>np.maximum(rows[:-1,5],rows[:-1,4]+900))
    bad[2201:]=1
    rows2,_,skipped,_=replay(c,h,l,0,d,params,bad,0,1,1,1,.0001)
    assert skipped>0
    assert not any(row[3]-1800 <= 2200 <= row[3]+962 for row in rows2)
