from functools import partial

import numpy as np
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
