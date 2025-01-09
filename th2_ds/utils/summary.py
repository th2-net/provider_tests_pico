import time
from random import choice
from typing import List, Tuple, Union, Dict, Sequence, TypeVar
from prettytable import PrettyTable
from collections import Counter
import itertools

"""
Let's say there are 3 metrics
A, B, C

A happens to be In, Out

B - any value

C - also any

Combinations need to be formed

A
AB
AC

B
BC

C

ABC

The order only affects display.

example
1. A: in, B: 0, C: x

a += 1
ab = {in/0: +=1}


"""


class Metric:
    def __init__(self, name: str, get_func):
        self.name: str = name
        self.get_func = get_func

    def __repr__(self):
        return f"Metric<{self.name}>"


Combination = TypeVar('Combination', Sequence[str], Sequence[Metric])


def get_all_metric_combinations(metrics: Union[List[str], List[Metric]]):
    """Returns all combinations of provided metrics."""
    metric_combinations = []

    for L in range(len(metrics) + 1):
        for subset in itertools.combinations(metrics, L):
            if subset:
                metric_combinations.append(subset)

    return metric_combinations


def get_message_type(message):
    metadata = message['body'][0]['metadata'] if isinstance(message['body'], list) else message['body']['metadata']
    return metadata['messageType']


# First you need to pull out all the metrics, and only then connect them

class SummaryCalculator:
    def __init__(self,
                 metrics: List[Metric],
                 combinations: Union[List[Sequence[str]], List[Sequence[Metric]]]):
        """
        Calculate, aggregate to tables and print metrics combinations.

        This class allows you to calculate any metrics and their combinations in stream-like way.

        It does not accumulate all data in memory.
        Keeps calculated metrics only.

        metrics: [Metric('a'), Metric(b)]
        combinations: e.g. [(a,),(a,b)].  You can put metric objects or metrics names.

        """
        self.metrics = metrics
        self.combinations: List[Tuple[str]] = []

        metric: Metric
        for comb in combinations:
            new_comb = self._prepare_combination(comb)
            self.combinations.append(new_comb)

        self.counter_field_name = 'cnt'
        self._counters: Dict[Tuple[str], Counter] = {}

        for v in self.combinations:
            self._counters[v] = Counter()

    def _get_counter(self, combination: Tuple[str]) -> Counter:
        """Returns counter for the combination.

        Expects prepared combination (after _prepare_combination method).
        """
        try:
            return self._counters[combination]
        except KeyError:
            raise Exception(
                f"Unknown combination. The following combination '{combination}' is not provided to constructor.")

    def _prepare_combination(self, combination: Combination) -> Tuple[str]:
        """Returns combination in the required view."""
        new_comb = []
        for cv in combination:
            if isinstance(cv, str):
                new_comb.append(cv)
            elif isinstance(cv, Metric):
                new_comb.append(cv.name)
            else:
                raise ValueError(f'Unexpected combination value, {combination}')

        new_comb.sort()  # Because ABC == CAB == BAC == CBA ...

        return tuple(new_comb)

    def append(self, m: dict):
        """Put some object to take it into account."""
        metric_values = {}
        for metric in self.metrics:
            metric_values[metric.name] = metric.get_func(m)

        # concat them  == values

        for v in self.combinations:  # v = ['session', 'direction']
            val_for_counter = tuple([metric_values[metric] for metric in v])
            self._counters[v].update([val_for_counter])

    def get_table(self, combination: Combination, add_total=False) -> PrettyTable:
        """Returns a PrettyTable class for certain combination.

        Args:
            combination: Union[Tuple[str], List[str]]
            add_total: If True, adds total value in the last line.

        """
        combination = self._prepare_combination(combination)

        t = PrettyTable()
        t.field_names = [*combination, self.counter_field_name]
        c = self._get_counter(combination)

        if add_total:
            total_val = 0

            for key, cnt in c.items():
                t.add_row([*key, cnt])
                total_val += cnt

            # Add total row.
            t.add_row([*['' for _ in combination], total_val])

        else:
            for key, cnt in c.items():
                t.add_row([*key, cnt])

        return t

    def show(self):
        """Prints all tables.

        Sorted by cnt.
        """
        for combination in self.combinations:
            t = self.get_table(combination)
            t.reversesort = True
            t.sortby = self.counter_field_name
            print(t)


if __name__ == '__main__':
    messages = []
    for i in range(1_000):
        dir = choice(['IN', 'OUT'])
        session = choice(['s1', 's2', 's3', 's4'])
        messageType = choice(['NewOrderSingle', 'Cancel', 'Amend'])
        messages.append({'direction': dir, 'sessionId': session, 'messageType': messageType})

    t1 = time.time()
    direction_m = Metric('direction', lambda m: m['direction'])
    session_m = Metric('session', lambda m: m['sessionId'])
    message_type_m = Metric('messageType', lambda m: m['messageType'])

    metrics_list = [
        direction_m,
        session_m,
        message_type_m,
    ]

    all_metrics_combinations = get_all_metric_combinations(metrics_list)
    print(all_metrics_combinations)
    sc = SummaryCalculator(metrics_list, all_metrics_combinations)

    for m in messages:
        sc.append(m)

    sc.show()

    print(time.time() - t1)

    print(sc.get_table(['direction', 'messageType', session_m], add_total=True))
    print(time.time() - t1)
