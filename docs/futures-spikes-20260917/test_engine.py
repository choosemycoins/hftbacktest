import numpy as np
import pytest
from engine import atr_features,quote_level,order_size,first_fill,exit_order,cashflows,funding_safe,simulate,NS

def test_atr_uses_completed_bars_only():
    t=np.arange(86400,dtype=np.int64)*10**9
    p=100+np.sin(np.arange(86400)/1000)
    a=atr_features(t,p,np.zeros(len(t),dtype=bool))
    p[18000:]*=100
    b=atr_features(t,p,np.zeros(len(t),dtype=bool))
    np.testing.assert_equal(a[:301],b[:301])

def test_true_range_includes_gap_from_previous_close():
    t=np.arange(86400,dtype=np.int64)*10**9;p=np.ones(86400)*100;p[1800:]=110
    f=atr_features(t,p,np.zeros(len(t),dtype=bool))
    assert f[31,0]==pytest.approx(10/14)

def test_rounding_is_passive_and_quantity_respects_limits():
    assert quote_level(100,.011,1,.5)==98.5
    assert quote_level(100,.011,-1,.5)==101.5
    assert order_size(100,10,20,.1,5,.1,1000)==pytest.approx(.6)
    assert order_size(100,.01,.01,.1,5,1,1000)==0

def test_trade_requires_correct_side_volume_penetration_and_exchange_time():
    t=np.array([10,11,12,13,14],np.int64);ex=np.array([1,11,12,13,14],np.int64)
    p=np.array([98.,98.,99.,98.,98.]);q=np.array([10.,10.,10.,.5,10.]);s=np.array([-1,1,-1,-1,-1])
    assert first_fill(t,ex,p,q,s,10,15,1,99.,1.,1.)==4
    assert first_fill(t,ex,p,q,s,10,14,1,99.,1.,1.)==-1

def test_exit_waits_for_lag_and_executes_actual_touch():
    t=np.arange(0,20,dtype=np.int64)*10**9
    bid=np.ones(20)*100;ask=bid+1
    bid[5:]=97;ask[5:]=98
    q=np.ones(20)*10
    ex,reason=exit_order(t,t,bid,ask,2*10**9,1,100,10*10**9,2*10**9)
    assert ex==7 and reason==2

def test_short_stop_and_timeout():
    t=np.arange(20,dtype=np.int64)*10**9;b=np.ones(20)*99;a=b+1
    a[5:]=103
    assert exit_order(t,t,b,a,2*10**9,-1,100,10*10**9,10**9)==(6,2)
    a[:]=100
    assert exit_order(t,t,b,a,2*10**9,1,99.5,10*10**9,10**9)==(13,3)

def test_fees_on_actual_notionals_and_zero_cost_identity():
    gross,net,stress,zero=cashflows(1,100,102,102.5,2,.1)
    assert gross==4
    assert net==pytest.approx(2*(101.9-100)-2*100*.0002-2*101.9*.0005)
    assert zero==5 and stress<net<gross

def test_funding_unknown_stale_or_crossing_forbids_entry():
    assert not funding_safe(100,99,101,10)
    assert not funding_safe(10**13,0,10**14,10)
    assert funding_safe(100*10**9,99*10**9,10000*10**9,10**9)

def replay_fixture(marketable=False,old_print=False,funding_cross=False,holding=False):
    bt=np.arange(18000,dtype=np.int64)*NS;be=bt.copy()
    b=np.full(18000,99.99);a=np.full(18000,100.01);q=np.full(18000,100.)
    d=225*60
    b[d+2:d+3]=99.69;a[d+2:d+3]=99.71
    b[d+3:d+6]=101.1;a[d+3:d+6]=101.12
    if marketable:b[d+1]=99.69;a[d+1]=99.71
    tt=np.array([(d+2)*NS],np.int64);te=tt.copy()
    prices=np.array([99.7]);quantities=np.array([10.]);sides=np.array([-1])
    if holding:
        b[d+3:]=99.79;a[d+3:]=99.81
        tt=np.array([(d+2)*NS,(d+62)*NS],np.int64);te=tt.copy()
        prices=np.array([99.7,99.5]);quantities=np.array([10.,10.]);sides=np.array([-1,-1])
    if old_print:te[0]=d*NS
    features=np.full((1440,3),.1)
    ft=np.array([(d-1)*NS],np.int64);fn=np.array([(d+100 if funding_cross else 90000)*NS],np.int64)
    args=(bt,be,b,q,a,q,tt,te,prices,quantities,sides,ft,fn,features,np.zeros(len(bt)+1,np.int64),.01,.1,.1,5.,1000.,5,2.,NS,4,1,0)
    return simulate(*args)

def test_full_executor_postonly_receipt_causality_and_funding_gate():
    rows,_=replay_fixture()
    assert len(rows)==1 and rows[0,5]==pytest.approx(99.8)
    assert rows[0,3]>rows[0,2]+NS and rows[0,4]>rows[0,3]
    assert rows[0,10]==1 and rows[0,12]>0
    assert len(replay_fixture(marketable=True)[0])==0
    assert len(replay_fixture(old_print=True)[0])==0
    assert len(replay_fixture(funding_cross=True)[0])==0

def test_second_quote_cannot_open_while_position_is_held():
    rows,_=replay_fixture(holding=True)
    assert len(rows)==1 and rows[0,10]==3
