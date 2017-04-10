#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import itertools
import collections
import types
from datetime import date

###############################################################################
## Classes

Count = collections.namedtuple('Count', ['count','amount'])
DictKeyTypes = (dict,collections.namedtuple,collections.OrderedDict)

class Aggregator(dict):
    '''Takes an initial parameter to pass to a <collections.nametuple> constructor.
    This initial paramter must be a list or tuple of "fields" to use for keys in the
    Aggregator object. Aggregator may then be initialized with any number of
    arguments matching the fields. Each argument then becomes an empty key.'''
    def __init__(self, fieldnames, *args, **kwargs):
        self.count = collections.Counter()
        self.amount = collections.Counter()
        self._fields = tuple(fieldnames)
        self._keywrapper = collections.namedtuple('Key', fieldnames, **kwargs)
        for key in args:
            self[self._keywrapper(*key)] = Count(0, 0.0)

    def __repr__(self):
        return 'Aggregate:\n%s' % '\n'.join('  %s: %s' % (k,v) for k,v in self.iteritems())

    def __len__(self):
        return len(self.count)

    def __setitem__(self, key, amount):
        key = self._keywrapper(*key)
        count, amount = getCountObjectFromPair(amount)
        if not self.count.get(key):
            self.count[key], self.amount[key] = count, amount
        else:
            self.count = collections.Counter({key: count})
            self.amount = collections.Counter({key: amount})
        super(Aggregator, self).__setitem__(key, Count(self.count[key], self.amount[key]))

    def __getitem__(self, key):
        key = self._keywrapper(*key)
        return Count(self.count[key], float(self.amount[key]))

    def __radd__(self, aggregate):
        pass

    def add(self, key, value):
        key = self._keywrapper(*key)
        self.count.update({key: 1})
        self.amount.update({key: float(value)})
        super(Aggregator, self).__setitem__(key, Count(self.count[key], self.amount[key]))

    def update(self, *args, **kwargs):
        '''Expects either a dict of keys and amounts as values, or set as keywords.'''
        if args:
            if len(args) > 1:
                raise TypeError("update expected at most 1 arguments, got %d" % len(args))
            for key, value in dict(args[0]).iteritems():
                self.add(key, value)
        for key, value in kwargs.iteritems():
            self.add(key, value)

    def copy(self):
        shallow_copy = Aggregator(self._fields)
        for key in self:
            shallow_copy[key] = self[key]
        return shallow_copy

    def fieldkeys(self, field):
        index = self._fields.index(field)
        return set( fk[index] for fk in self.keys() )

    def filter(self, *args):
        filtered_copy = Aggregator(self._fields)
        for k in args:
            found = [key for key in self if k in key]
            for key in found:
                filtered_copy[key] = self[key]
        return filtered_copy

#   def collapse(self, collapse_field):
#       '''Remove a sub-key element and merge values lacking the key, returning a new Aggregator.'''
#       collapse_index = self._fields.index(collapse_field)
#       fields = list(self._fields)
#       fields.remove(collapse_field)
#       collapsed_copy = Aggregator(fields)
#       for key, value in self.iteritems():
#           newkey = list(key)
#           collapsedon = newkey.pop(collapse_index)
#           collapsed_copy.count.update({ tuple(newkey): value.count})
#           collapsed_copy.amount.update({ tuple(newkey): value.amount})
#           super(Aggregator, collapsed_copy).__setitem__(tuple(newkey), Count(collapsed_copy.count[key], collapsed_copy.amount[key]))
#       return collapsed_copy

    def splice(self, *aggregates):
        '''Squeeze in additional aggregators, and add any keys to key tuple given if not present.'''
        for agg in aggregates:  # first, consistency checks
            if type(agg) is not type(self):
                raise ValueError("Can only splice other %s objects." % type(self))
            if agg._fields != self._fields:
                raise ValueError("Can only splice with fieldlist: %s." % repr(list(self._fields)))
            for key, aggCount in agg.iteritems():
                self.count.update({key: aggCount.count})
                self.amount.update({key: aggCount.amount})
                super(Aggregator, self).__setitem__(key, Count(self.count[key], self.amount[key]))

    def value_sorted(self, by_count=False, reverse=False):
        return sorted(self.iteritems(), key=lambda (k,v): (v.count, v.amount) if by_count else (v.amount, v.count), reverse=reverse)


###############################################################################
##                               Functions
###############################################################################

def getCountObjectFromPair(tup):
    try:
        count, amount = 1, float(tup)
        return Count(count, amount)
    except:
        if len(tup) > 2:
            raise TypeError("Expected length 2, got %d" % len(tup))
        count, amount = int(tup[0]), float(tup[1])
        return Count(count, amount)

def getComplexCountFromSqlQuery(query, cursor, keylist):
    SqlMap = collections.Counter()
    cursor.execute(query)
    key_indices = dict( (cursor.fieldnames().index(k), k) for k in keylist)
    for row in cursor.fetchall():
        count = None
        key = keylist[:]
        for i, value in enumerate(row):
            if i in key_indices:
                key[keylist.index(key_indices[i])] = value
                continue
            count = int(value) if type(value) in (int,long) or (type(value) in (str,unicode) and value.isdigit()) else float(value)
        SqlMap[tuple(key)] = count
    return SqlMap

def getSetDictFromSqlQuery(query, cursor, keylist):
    SqlMap = dict()
    cursor.execute(query)
    key_indices = dict( (cursor.fieldnames().index(k), k) for k in keylist)
    for row in cursor.fetchall():
        item = None
        key = keylist[:]
        for i, value in enumerate(row):
            if i in key_indices:
                key[keylist.index(key_indices[i])] = value
                continue
            item = set([value]) if type(value) is not set else value
        if not SqlMap.get(tuple(key)):
            SqlMap[tuple(key)] = item
        else:
            SqlMap[tuple(key)].update(item)
    return SqlMap

def getAggregatesFromSqlQuery(query, cursor, keylist):
    SqlMap = Aggregator(keylist)
    cursor.execute(query)
    key_indices = dict( (cursor.fieldnames().index(k), k) for k in keylist)
    for row in cursor.fetchall():
        count, amount = None, None
        key = keylist[:]
        for i, value in enumerate(row):
            if i in key_indices:
                key[keylist.index(key_indices[i])] = value
                continue
            # assume count is followed by amount, always
            if count:
                amount = float(value)
            else:
                count = int(value)
        SqlMap[tuple(key)] = Count(count, amount)
    return SqlMap
