"""Real-time running average aggregation."""


class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, value: float) -> float:
        self.count += 1
        self.total += value
        return self.average

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


class ProductAggregator:
    """Tracks an overall running average plus one running average per product."""

    def __init__(self):
        self.overall = RunningAverage()
        self.per_product: dict[str, RunningAverage] = {}

    def update(self, product: str, price: float):
        overall_avg = self.overall.update(price)
        product_avg = self.per_product.setdefault(product, RunningAverage()).update(price)
        return overall_avg, product_avg
