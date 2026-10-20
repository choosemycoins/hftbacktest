"""Reproduce parameter-change and concurrent-notional diagnostics."""
from pathlib import Path
import datetime as dt
import gzip
import json

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent


def parameters():
    frozen = json.loads((ROOT / 'data/params.json').read_text())
    sources = json.loads((ROOT / 'data/param_sources.json').read_text())
    count, changes = 0, []
    for source in sources:
        with gzip.open(source['path'], 'rt') as stream:
            for line in stream:
                row = json.loads(line)
                if row['kind'] != 'snapshot':
                    continue
                day = dt.datetime.fromisoformat(row['ts']).strftime('%Y%m%d')
                for instrument in row['payload']['symbols']:
                    key = instrument['symbol'].lower() + '_' + day
                    if key not in frozen:
                        continue
                    count += 1
                    filters = {f['filterType']: f for f in instrument['filters']}
                    values = dict(
                        tick=float(filters['PRICE_FILTER']['tickSize']),
                        lot=float(filters['LOT_SIZE']['stepSize']),
                        minqty=float(filters['LOT_SIZE']['minQty']),
                        minnotional=float(filters['MIN_NOTIONAL']['notional']),
                        market_lot=float(filters['MARKET_LOT_SIZE']['stepSize']),
                        maxqty=min(float(filters['LOT_SIZE']['maxQty']),
                                   float(filters['MARKET_LOT_SIZE']['maxQty'])),
                    )
                    if (any(values[k] != frozen[key][k] for k in values)
                            or instrument['status'] != 'TRADING'):
                        changes.append(dict(key=key, ts=row['ts'], values=values,
                                            status=instrument['status']))
    return dict(checked_instrument_snapshots=count, intraday_changes=changes)


def utilization():
    trades = pd.read_csv(ROOT / 'out/trades.csv.gz')
    result = {}
    for variant in (4, 9):
        rows = trades[trades.variant == variant]
        notionals = (rows.entry * rows.qty).to_numpy()
        times = np.r_[rows['fill'].to_numpy(), rows['exit'].to_numpy()]
        changes = np.r_[notionals, -notionals]
        # Combine simultaneous events before evaluating aggregate exposure.
        exposure = pd.Series(changes, index=times).groupby(level=0).sum().cumsum()
        result[str(variant)] = dict(
            max_concurrent_entry_notional=float(exposure.max()),
            average_deployed_notional=float(
                (notionals * (rows['exit'] - rows['fill']).to_numpy()).sum()
                / (29 * 86400 * 10**9)),
            total_entry_notional=float(notionals.sum()),
        )
    return result


if __name__ == '__main__':
    for name, result in [('parameter_validation', parameters()),
                         ('utilization', utilization())]:
        previous = json.loads((ROOT / 'out' / (name + '.json')).read_text())
        if name == 'parameter_validation':
            assert result == previous
        else:
            for variant, values in result.items():
                for field, value in values.items():
                    assert abs(value - previous[variant][field]) < 1e-8
        print(name, 'reproduced')
