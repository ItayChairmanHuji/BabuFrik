import torch
from pyvacy.analysis.rdp_accountant import compute_rdp, get_privacy_spent


def comp_epsilon(qs, sigmas, iterations, delta):
    optimal_order = _ternary_search(
        lambda order: _apply_analysis(qs, sigmas, iterations, delta, [order]), 1, 512, 72)

    return _apply_analysis(qs, sigmas, iterations, delta, [optimal_order])


def _apply_analysis(qs, sigmas, iterations, delta, orders):
    """
    Compute the overall privacy cost
    :param qs a list of sample ratios
    :param sigmas a list of noise scale
    :param iterations
    :param delta
    :param orders a list of order
    """

    total_rdp = 0
    for idx in range(len(qs)):
        total_rdp += compute_rdp(qs[idx], sigmas[idx], iterations[idx], orders)

    epsilon, _, _ = get_privacy_spent(orders, total_rdp, target_delta=delta)

    return epsilon


def epsilon(N, batch_size, noise_multiplier, iterations, delta=1e-5, gaussian_std=[]):
    """Calculates epsilon for stochastic gradient descent.

    Args:
        N (int): Total numbers of examples
        batch_size (int): Batch size
        noise_multiplier (float): Noise multiplier for DP-SGD
        delta (float): Target delta

    Returns:
        float: epsilon

    Example::
        >>> epsilon(10000, 256, 0.3, 100, 1e-5)
    """
    q = batch_size / N
    optimal_order = _ternary_search(lambda order: _apply_kamino_analysis(q, noise_multiplier, iterations, [order], delta), 1, 512, 72)
    return _apply_kamino_analysis(q, noise_multiplier, iterations, [optimal_order], delta, gaussian_std)


def _apply_kamino_analysis(q, sigma, iterations, orders, delta, gaussian_std=[]):
    """Calculates epsilon for kamino
    Args:
        q (float): Sampling probability, generally batch_size / number_of_samples
        sigma (float): Noise multiplier
        gaussian_sigma(list): Std dev. for gaussian noise additions
        iterations (float): Number of iterations mechanism is applied
        orders (list(float)): Orders to try for finding optimal epsilon
        delta (float): Target delta
    """
    total_rdp = compute_rdp(q, sigma, iterations, orders)
    for gaussian_sigma in gaussian_std:
        total_rdp += compute_rdp(1, gaussian_sigma, 1, orders)
    eps, _, opt_order = get_privacy_spent(orders, total_rdp, target_delta=delta)
    return eps


def _apply_dp_sgd_analysis(q, sigma, iterations, orders, delta):
    """Calculates epsilon for stochastic gradient descent.

    Args:
        q (float): Sampling probability, generally batch_size / number_of_samples
        sigma (float): Noise multiplier
        iterations (float): Number of iterations mechanism is applied
        orders (list(float)): Orders to try for finding optimal epsilon
        delta (float): Target delta

    Returns:
        float: epsilon

    Example::
        >>> epsilon(10000, 256, 0.3, 100, 1e-5)
    """
    rdp = compute_rdp(q, sigma, iterations, orders)
    eps, _, opt_order = get_privacy_spent(orders, rdp, target_delta=delta)
    return eps


def _ternary_search(f, left, right, iterations):
    """Performs a search over a closed domain [left, right] for the value which minimizes f."""
    for i in range(iterations):
        left_third = left + (right - left) / 3
        right_third = right - (right - left) / 3
        if f(left_third) < f(right_third):
            right = right_third
        else:
            left = left_third
    return (left + right) / 2

def make_optimizer_class(cls):
    class DPOptimizerClass(cls):
        def __init__(self, l2_norm_clip, noise_multiplier, minibatch_size, microbatch_size, *args, **kwargs):
            super(DPOptimizerClass, self).__init__(*args, **kwargs)

            self.l2_norm_clip = l2_norm_clip
            self.noise_multiplier = noise_multiplier
            self.microbatch_size = microbatch_size
            self.minibatch_size = minibatch_size

            for group in self.param_groups:
                group['accum_grads'] = [torch.zeros_like(param.data) if param.requires_grad else None for param in group['params']]

        def zero_microbatch_grad(self):
            super(DPOptimizerClass, self).zero_grad()

        def microbatch_step(self):
            total_norm = 0.
            for group in self.param_groups:
                for param in group['params']:
                    if param.requires_grad and param.grad is not None:
                        total_norm += param.grad.data.norm(2).item() ** 2.
            total_norm = total_norm ** .5
            clip_coef = min(self.l2_norm_clip / (total_norm + 1e-6), 1.)

            for group in self.param_groups:
                for param, accum_grad in zip(group['params'], group['accum_grads']):
                    if param.requires_grad and param.grad is not None:
                        accum_grad.add_(param.grad.data.mul(clip_coef))

        def zero_grad(self):
            for group in self.param_groups:
                for accum_grad in group['accum_grads']:
                    if accum_grad is not None:
                        accum_grad.zero_()

        def step(self, *args, **kwargs):
            for group in self.param_groups:
                for param, accum_grad in zip(group['params'], group['accum_grads']):
                    if param.requires_grad:

                        if param.grad is None:
                            continue

                        param.grad.data = accum_grad.clone()
                        param.grad.data.add_(self.l2_norm_clip * self.noise_multiplier * torch.randn_like(param.grad.data))
                        param.grad.data.mul_(self.microbatch_size / self.minibatch_size)
            super(DPOptimizerClass, self).step(*args, **kwargs)

    return DPOptimizerClass