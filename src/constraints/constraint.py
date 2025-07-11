import itertools
from dataclasses import dataclass
from typing import Iterator

from pandas import DataFrame

from src.constraints.predicate import Predicate


@dataclass
class Constraint:
    num_of_tuples: int
    predicates: list[Predicate]

    def find_violations(self, data: DataFrame) -> list[tuple[int, ...]]:
        violations = set()
        blocks = self.split_data_to_blocks(data)
        for block in blocks:
            violations.update(self.find_block_violations(block))
        return list(violations)

    def split_data_to_blocks(self, data: DataFrame) -> Iterator[DataFrame]:
        blocks_attrs = [attr for p in self.predicates
                        if p.is_equality and len(p.attrs) == 2
                        for attr in p.attrs]
        if blocks_attrs:
            yield from self.get_valid_blocks(data, blocks_attrs)
        else:
            yield data

    # TODO: optimize
    def get_valid_blocks(self, data: DataFrame, blocks_attrs: list[str]) -> Iterator[DataFrame]:
        for block_values, block in data.groupby(blocks_attrs, sort=False):
            if self.is_block_valid(block_values):
                yield block

    @staticmethod
    def is_block_valid(block_values) -> bool:
        return all(block_values[i] == block_values[i + 1]
                   for i in range(len(block_values) - 1)) if block_values else True

    def find_block_violations(self, block: DataFrame) -> Iterator[tuple[int, ...]]:
        predicates = [p for p in self.predicates if not p.is_equality or len(p.attrs) == 1]
        for violation in itertools.combinations(block.index, self.num_of_tuples):
            if self.is_violation(block, violation, predicates):
                yield violation

    @staticmethod
    def is_violation(data: DataFrame, potential_violation: tuple[int, ...], predicates: list[Predicate]) -> bool:
        tuples = data.loc[potential_violation].reset_index(drop=True)
        return any(predicate.evaluate(tuples) for predicate in predicates)
