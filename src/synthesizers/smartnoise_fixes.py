from functools import partial

import numpy as np
import pandas as pd
from mbi import Dataset
from scipy import sparse


def patectgan_set_device(self, device):
    self._device = device
    if self._generator is not None:
        self._generator.to(self._device)


def mst_compress_domain(self, data, measurements):
    supports = {}
    new_measurements = []
    for Q, y, sigma, proj in measurements:
        col = proj[0]
        sup = y >= 3 * sigma
        supports[col] = sup
        if supports[col].sum() == y.size:
            new_measurements.append((Q, y, sigma, proj))
        else:
            y2 = np.append(y[sup], y[~sup].sum())
            I2 = np.ones(y2.size)
            I2[-1] = 1.0 / np.sqrt(y.size - y2.size + 1.0)
            y2[-1] /= np.sqrt(y.size - y2.size + 1.0)
            I2 = sparse.diags(I2)
            new_measurements.append((I2, y2, sigma, proj))

    undo_compress_fn = partial(self.reverse_data, supports=supports)

    return self.transform_data(data, supports), new_measurements, undo_compress_fn


def graphical_model_synthetic_data(self, rows=None, method='round'):
    total = int(self.total) if rows is None else rows
    cols = self.domain.attrs
    data = np.zeros((total, len(cols)), dtype=int)
    df = pd.DataFrame(data, columns=cols)
    cliques = [set(cl) for cl in self.cliques]

    def synthetic_col(counts, total):
        if method == 'sample':
            probas = counts / counts.sum()
            return np.random.choice(counts.size, total, True, probas)
        counts = counts if counts.sum() != 0 else np.ones(counts.size, dtype=int)
        counts *= total / counts.sum()
        frac, integ = np.modf(counts)
        integ = integ.astype(int)
        extra = total - integ.sum()
        if extra > 0:
            idx = np.random.choice(counts.size, extra, False, frac / frac.sum())
            integ[idx] += 1
        vals = np.repeat(np.arange(counts.size), integ)
        np.random.shuffle(vals)
        return vals

    order = self.elimination_order[::-1]
    col = order[0]
    marg = self.project([col]).datavector(flatten=False)
    df.loc[:, col] = synthetic_col(marg, total)
    used = {col}

    for col in order[1:]:
        relevant = [cl for cl in cliques if col in cl]
        relevant = used.intersection(set.union(*relevant))
        proj = tuple(relevant)
        used.add(col)
        marg = self.project(proj + (col,)).datavector(flatten=False)

        def foo(group):
            idx = group.name
            vals = synthetic_col(marg[idx], group.shape[0])
            group[col] = vals
            return group

        if len(proj) >= 1:
            df = df.groupby(list(proj), group_keys=False).apply(foo)
        else:
            df[col] = synthetic_col(marg, df.shape[0])

    return Dataset(df, self.domain)