# aggregator

Python module to create multikey transaction accounting maps.

## History

With lots of transactions to manipulate for financial reporting, I had a
general problem to solve: I needed a rapid way of assembling transactions on
the basis of a set of arbitrary values. As the needed totals were clear (total
numbers of transactions and the sum of those transactions), what was missing
was a way of quickly aggregating these values in a way that would let me put
in, and pull out only the data I needed.

Since what I needed in most cases was a dict() built up with each set of
attributes, the solution finally coalesced after some trial and error. The
result was a single multi-key, dict-like class with a fixed value type.

To maintain fixed sets of keys, we needed a methodology for working with
tuples. The original design worked with arbitrary keys, and various
implmentations employed `namedtuple`s heavily in order to clarify the category
of key in use. After repeated trials with this enforced implementation, it
became clear that it would be far better to set the fields to be used from the
beginning, and enforce those fields for the object.

## Design

In its simplest form, an Aggregator is a wrapper around two Counter objects,
from the standard library collections module. Each counter is a dict in its own
right, with some extra methods bundled in. The keys in use are employed at each
level of the object, which is complex to manage at the base level but makes
adding and retrieving new data very rapid.

As a logical object, it looks a little like this below, with the following
definitions:

- TupleKey: an arbitrary list of keys in a strict ordering according to the "fields" set during instantiation.
- CountKey: a collections.namedtuple instance of ("Key", ["count", "amount]) where the count and amount values are each a collections.Counter object.
- Counter: in this case, a collections.Counter() object with a one-to-one match between its own keys, and the keys of the Aggregator object.

```
Aggregator
  { TupleKey:
      CountKey("count": Counter(TupleKey: total count),
               "amount": Counter(TupleKey: total amount)
      ),
    TupleKey:
      CountKey("count": Counter(TupleKey: total count),
               "amount": Counter(TupleKey: total amount)
      ),
    ...
  }
```

## Use

An example:

```python
>>> agg = aggregator.Aggregator(['field1', 'field2', 'field3'])
>>> agg.update({ ('foo', 'bar', 'koi'): 34.67 })
>>> agg.update({ ('foo', 'bee', 'koi'): 817.02 })
>>> agg += { ('foo', 'bar', 'koi'):  12.11 } # short version overloads radd
>>> agg
Key(field1='foo', field2='bee', field3='koi'): Total(count=1, amount=817.02)
>>> agg.filter('bar')
Key(field1='foo', field2='bar', field3='koi'): Total(count=2, amount=46.78)Key(field1='foo', field2='bar', field3='koi'): Total(count=2, amount=46.78)
>>> agg.collapse('field2')
Key(field1='foo', field3='koi'): Total(count=3, amount=863.8)
```

