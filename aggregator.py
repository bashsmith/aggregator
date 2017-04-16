#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import itertools
import collections
import types
from datetime import date

#################################################
## Classes

Total = collections.namedtuple('Total', ['count','amount'])
DictKeyTypes = (dict,collections.namedtuple,collections.OrderedDict)

class Aggregator(dict):
    '''Takes an initial list of fields to use for tracking keys in the
       Aggregator object. Aggregator may then be initialized with any number of
       arguments matching the fields. Each argument then becomes an empty key.'''

    def __init__(self, fieldnames, *args, **kwargs):
        self.count = collections.Counter()
        self.amount = collections.Counter()
        self._fields = tuple(fieldnames)
        self._keywrapper = collections.namedtuple('Key', fieldnames, **kwargs)
        for key in args:
            self[tuple(key)] = Total(0, 0.0)
            
    def __repr__(self):
        return 'Aggregate:\n%s' % '\n'.join('  %s: %s' % (self._keywrapper(*k),v) for k,v in self.iteritems())
        
    def __len__(self):
        return len(self.count)
        
    def __setitem__(self, key, amount):
        key = tuple(key)
        count, amount = getTotalObjectFromPair(amount)
        if not self.count.get(key):
            self.count[key], self.amount[key] = count, amount
        else:
            self.count = collections.Counter({key: count})
            self.amount = collections.Counter({key: amount})
        super(Aggregator, self).__setitem__(key, Total(self.count[key], self.amount[key]))
        
    def __getitem__(self, key):
        key = tuple(key)
        return Total(self.count[key], float(self.amount[key]))
        
    def __radd__(self, aggregate):
        self.splice(aggregate)
        
    def __add__(self, aggregate):
        self.splice(aggregate)
        return self
        
    def _addTotals(t0, t1):
        return Total(int(t0[0] + t1[0]), float(t0[1] + t1[1]))

    def _addToTotal(amt):
        return Total(1, t0[1] + amt)

    def add(self, key, value):
        self.count.update({key: 1})
        self.amount.update({key: float(value)})
        super(Aggregator, self).__setitem__(key, Total(self.count[key], self.amount[key]))
        
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
        for key in self.iterkeys():
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
        
    def collapse(self, collapse_field): 
        '''Remove a sub-key element and merge values lacking the key, returning a new Aggregator.'''
        collapse_index = self._fields.index(collapse_field)
        fields = list(self._fields)
        fields.remove(collapse_field)
        collapsed_copy = Aggregator(fields)
        for key, value in self.iteritems():
            key = list(key)
            toss = key.pop(collapse_index)
            key = tuple(key)
            if collapsed_copy.get(key):
                collapsed_copy.count.update({key: value.count})
                collapsed_copy.amount.update({key: value.amount})
                super(Aggregator, collapsed_copy).__setitem__(key, Total(collapsed_copy.count[key], collapsed_copy.amount[key]))
            else:
                collapsed_copy[key] = value
        return collapsed_copy

    def splice(self, aggregate):
        '''Squeeze in additional aggregators, and add any keys to key tuple given if not present.'''
        # first, consistency checks
        if type(agg) is not type(self):
            raise ValueError("Can only splice other %s objects." % type(self))
        if agg._fields != self._fields:
            raise ValueError("Can only splice with fieldlist: %s." % repr(list(self._fields)))
        for key, aggTotal in agg.iteritems():
            self.count.update({key: aggTotal.count})
            self.amount.update({key: aggTotal.amount})
            super(Aggregator, self).__setitem__(key, Total(self.count[key], self.amount[key]))
                
    def value_sorted(self, by_count=False, reverse=False):
        return sorted(self.iteritems(), key=lambda (k,v): (v.count, v.amount) if by_count else (v.amount, v.count), reverse=reverse)
        
    def field_sorted(self, *field_keys, **kwargs):
        r = kwargs.get('reverse') or False
        return sorted(self.iteritems(), key=lambda (k,v): [k._asdict[fk] for fk in field_keys], reverse=r)

    def getcsv(self, *sort_keys, **kwargs):
        csv_fd = StringIO()
        r = kwargs.pop('reverse') if kwargs.get('reverse') else False
        # leaving this open to **kwargs for passing in alternate dialects
        cw = csv.writer(csv_fd, **kwargs)
        cw.writerow(self._fields + ('count', 'amount'))
        for pair in self.field_sorted(*sort_keys, reverse=r):
            # each row is a (Key, Total) pair.
            cw.writerow([i._asdict.values() for i in pair])
        csv_fd.seek(0)
        return csv_fd

        
#################################################
##                Functions
#################################################

def getTotalObjectFromPair(tupOrVal):
    try:
        count, amount = 1, float(tupOrVal)
        return Total(count, amount)
    except:
        if len(tupOrVal) > 2:
            raise TypeError("Expected length 2, got %d" % len(tupOrVal))
        count, amount = int(tupOrVal[0]), float(tupOrVal[1])
        return Total(count, amount)

        
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
    
    
if __name__ == '__main__':
    import random
    agg = Aggregator(['field1', 'field2', 'field3'])
    
    ccy = ('EUR', 'GBP', 'PLN', 'USD')
    land = ('de', 'es', 'fr', 'it', 'nl', 'pt')
    method = ('pos','atm')

    for n in xrange(20):
        f1 = ccy[random.randrange(0,len(ccy))]
        f2 = land[random.randrange(0,len(land))]
        f3 = method[random.randrange(0,len(method))]
        agg.update({(f1,f2,f3): n*float('%01.2f' % random.random())})

    print(agg)
    print(agg.collapse('field2'))
    print(agg.filter('USD'))

    agg2 = agg.filter('USD')
    print(agg + agg2)

