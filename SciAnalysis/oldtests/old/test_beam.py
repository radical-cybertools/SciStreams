import random
import apache_beam as beam

def run_trials(count):
  """Throw darts into unit square and count how many fall into unit circle."""
  inside = 0
  for _ in xrange(count):
    x, y = random.uniform(0, 1), random.uniform(0, 1)
    inside += 1 if x*x + y*y <= 1.0 else 0
  return count, inside

def combine_results(results):
  """Given all the trial results, estimate pi."""
  total, inside = sum(r[0] for r in results), sum(r[1] for r in results)
  return total, inside, 4 * float(inside) / total if total > 0 else 0

p = beam.Pipeline()
(p | beam.Create([500] * 10)  # Create 10 experiments with 500 samples each.
   | beam.Map(run_trials)     # Run experiments in parallel.
   | beam.CombineGlobally(combine_results)      # Combine the results.
   | beam.io.WriteToText('./pi_estimate.txt'))  # Write PI estimate to a file.

p.run()
