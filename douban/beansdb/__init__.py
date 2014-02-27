#!/usr/bin/env python
# encoding: utf-8
"""
__init__.py

Created by geng xinyue on 2011-02-14.
Copyright (c) 2011 douban.com. All rights reserved.
Last Update by hurricane <lilinghui@douban.com>.
"""

import cmemcached
import sys
import time
import random
import socket
from warnings import warn


def fnv1a(s):
    from fnv1a import get_hash
    return get_hash(s) & 0xffffffff


from douban.utils import ThreadedObject
from douban.utils.config import read_config
from douban.utils.slog import log as slog

MAX_KEYS_IN_GET_MULTI = 200
ONE_DAY = 24 * 3600
ONE_MINUTE = 60

log = lambda message: slog('beansdb', message)


class WriteFailedError(IOError):

    def __init__(self, key, servers='unknown'):
        IOError.__init__(self)
        self.key = key
        self.servers = servers

    def __repr__(self):
        return 'write %r failed(%r)' % (self.key, self.servers)

    def __str__(self):
        return repr(self)


class ReadFailedError(IOError):

    def __init__(self, key, servers='unknown'):
        IOError.__init__(self)
        self.key = key
        self.servers = servers

    def __repr__(self):
        return 'read %r failed(%r)' % (self.key, self.servers)

    def __str__(self):
        return repr(self)


class DeleteFailedError(IOError):

    def __init__(self, key, servers='unknown'):
        IOError.__init__(self)
        self.key = key
        self.servers = servers

    def __repr__(self):
        return 'delete %r failed(%r)' % (self.key, self.servers)

    def __str__(self):
        return repr(self)


def connect(server, **kwargs):
    c = cmemcached.Client([server], do_split=0, **kwargs)
    c.set_behavior(cmemcached.BEHAVIOR_CONNECT_TIMEOUT, 300)  # 0.3 s
    c.set_behavior(cmemcached.BEHAVIOR_POLL_TIMEOUT, 3000)  # 3 s
    c.set_behavior(cmemcached.BEHAVIOR_SERVER_FAILURE_LIMIT, 4)
    c.set_behavior(cmemcached.BEHAVIOR_RETRY_TIMEOUT, 5)  # 5 s
    c.set_behavior(cmemcached.BEHAVIOR_SUPPORT_CAS, 0)
    return c


class MCStore(object):

    def __init__(self, addr, threaded=True, **kwargs):
        self.addr = addr
        if threaded:
            self.mc = ThreadedObject(connect, addr, **kwargs)
        else:
            self.mc = connect(addr, **kwargs)

    def __repr__(self):
        return '<MCStore(addr=%s)>' % repr(self.addr)

    def __str__(self):
        return self.addr

    def set(self, key, data, rev=0):
        return bool(self.mc.set(key, data, rev))

    def set_raw(self, key, data, rev=0, flag=0):
        if rev < 0:
            raise str(rev)
        return self.mc.set_raw(key, data, rev, flag)

    def set_multi(self, values, return_failure=False):
        return self.mc.set_multi(values, return_failure=return_failure)

    def get(self, key):
        try:
            r = self.mc.get(key)
            if r is None and self.mc.get_last_error() != 0:
                raise IOError(self.mc.get_last_error())
            return r
        except ValueError:
            self.mc.delete(key)

    def get_raw(self, key):
        r, flag = self.mc.get_raw(key)
        if r is None and self.mc.get_last_error() != 0:
            raise IOError(
                self.mc.get_last_error(), self.mc.get_last_strerror())
        return r, flag

    def get_multi(self, keys):
        r = self.mc.get_multi(keys)
        if self.mc.get_last_error() != 0:
            raise IOError(
                self.mc.get_last_error(), self.mc.get_last_strerror())
        return r

    def delete(self, key):
        return bool(self.mc.delete(key))

    def delete_multi(self, keys, return_failure=False):
        return self.mc.delete_multi(keys, return_failure=return_failure)

    def exists(self, key):
        return bool(self.mc.get('?' + key))

    def incr(self, key, value):
        return self.mc.incr(key, int(value))


class BeansdbClient(object):

    store_cls = MCStore

    def __init__(self, addrs, update_period=10, **kwargs):
        self.addrs = addrs
        self.servers = [self.store_cls(s, **kwargs) for s in addrs]
        self.update_period = update_period
        self.buckets = []
        self.last_update = 0
        self.stat = [None] * len(addrs)
        self.W = 2
        self.N = 3

    def update(self):
        def listdir(s):
            try:
                return [int(l.split(' ')[2])
                        for l in s.get('@').strip().split('\n')]
            except Exception:
                pass
        for i, s in enumerate(self.servers):
            if not self.stat[i]:
                self.stat[i] = listdir(s)

        self.buckets = []
        for i in range(16):
            ss = sorted([(st[i], j) for j, st in enumerate(self.stat) if st],
                        reverse=True)[:self.N]
            top = ss[0][0]
            ss = [self.servers[j] for n, j in ss if n >= top * 0.9]
            random.shuffle(ss)
            self.buckets.append(ss)

    def _get_servers(self, key):
        now = time.time()
        if self.last_update + self.update_period < now:
            self.update()
            self.last_update = now

        return self.buckets[(fnv1a(key) * 16) >> 32]

    def get(self, key, default=None):
        successful = False
        ss = self._get_servers(key)
        for s in ss:
            try:
                r = s.get(key)
                successful = True
                if r is not None:
                    return r
            except Exception:
                pass

        if not successful: # even just one host get successful, should not raise exception
            raise ReadFailedError(key, ss)
        return default

    def _dispatch(self, keys):
        ss = {}
        servers = {}
        for key in keys:
            for s in self._get_servers(key):
                ss.setdefault(s.addr, []).append(key)
                servers[s.addr] = s
        sc = sorted([(len(ks), addr) for addr, ks in ss.items()])
        return [(servers[addr], ss[addr]) for _, addr in sc]

    def get_multi(self, keys, default=None):
        if len(keys) > MAX_KEYS_IN_GET_MULTI:
            r = self.get_multi(keys[:-MAX_KEYS_IN_GET_MULTI], default)
            r.update(self.get_multi(keys[-MAX_KEYS_IN_GET_MULTI:], default))
            return r
        rs = {}
        for s, ks in self._dispatch(keys):
            try:
                r = s.get_multi([k for k in ks if k not in rs])
                rs.update(r)
            except IOError, e:
                log("beansdb client get_multi() failed %s %s" % (s, e))
        for k in keys:
            if k not in rs:
                rs[k] = default
        return rs

    def exists(self, key):
        pos = '@%08x' % fnv1a(key)
        for s in self._get_servers(key):
            r = s.get(pos) or ''
            for l in r.split('\n'):
                parts = l.split(' ')
                if not parts:
                    continue
                if key == parts[0]:
                    return int(parts[-1]) > 0
        # for s in self._get_servers(key):
        #    if s.get('?'+key):
        #        return True
        return False

    def set(self, key, value):
        if value is not None:
            ss = self._get_servers(key)
            success_count = sum(1 if s.set(key, value) else 0 for s in ss[:self.N])
            if success_count < self.W:
                raise WriteFailedError(key, ss)
            return True
        else:
            return self.delete(key)

    def set_multi(self, values):
        to_delete = [k for k, v in values.iteritems() if v is None]
        self.delete_multi(to_delete)
        all_failures = []
        values = dict((k, v) for k, v in values.iteritems() if v is not None)
        dispatch_result = self._dispatch(values.keys())
        for s, ks in dispatch_result:
            vs = dict((k, values[k]) for k in ks)
            r, failures = s.set_multi(vs, return_failure=True)
            if not r:
                all_failures += failures
        if len(all_failures) != 0:
            raise WriteFailedError(
                all_failures, [s for s, _ in dispatch_result])
        return True

    def delete(self, key):
        ss = self._get_servers(key)
        if not all([s.delete(key) for s in ss]):
            raise WriteFailedError(key, ss)
        return True

    def delete_multi(self, keys):
        all_failures = []
        dispatch_result = self._dispatch(keys)
        for s, ks in dispatch_result:
            r, failures = s.delete_multi(ks, return_failure=True)
            if not r:
                all_failures += failures
        if len(all_failures) != 0:
            raise DeleteFailedError(
                all_failures, [s for s, _ in dispatch_result])
        return True

    def incr(self, key, incr=1):
        v = 0
        for s in self._get_servers(key):
            v = max(v, s.incr(key, incr))
        return v


class BeansDBProxy(object):
    store_cls = MCStore
    threaded = True

    def __init__(self, proxies, rechoose_period=60, **kwargs):
        """Init.

        rechoose_period:
            Seconds to re-choose a proxy to communicate, to keep connection to
            two proxies.  Otherwise when one proxy fails, too many connect
            requests will overwhelm remaining proxies.

        """
        self.servers = [self.store_cls(i, threaded=self.threaded, **kwargs)
                        for i in proxies]
        # make the servers to be a random sequence
        random.shuffle(self.servers)
        self.rechoose_period = rechoose_period
        self._time_to_rechoose = time.time() + rechoose_period

    def _get_servers(self, key):
        now = time.time()
        if now > self._time_to_rechoose:
            # keep connection with the first two servers so that we do not
            # need to send SYN packet when the first server fails.
            self.servers = self.servers[:2][::-1] + self.servers[2:]
            self._time_to_rechoose = now + self.rechoose_period
        return self.servers

    def get(self, key, default=None):
        servers = self._get_servers(key)
        for s in servers:
            try:
                r = s.get(key)
                if r is None:
                    r = default
                return r
            except IOError:
                self.servers = self.servers[1:] + self.servers[:1]

        log('all backends read failed, ' + key)
        raise ReadFailedError(key, servers)

    def exists(self, key):
        for s in self._get_servers(key):
            try:
                return s.exists(key)
            except IOError:
                self.servers = self.servers[1:] + self.servers[:1]
        return False

    def get_multi(self, keys, default=None):
        if len(keys) > MAX_KEYS_IN_GET_MULTI:
            r = self.get_multi(keys[:-MAX_KEYS_IN_GET_MULTI], default)
            r.update(self.get_multi(keys[-MAX_KEYS_IN_GET_MULTI:], default))
            return r
        for s in self._get_servers(''):
            try:
                rs = s.get_multi(keys)
                for k in keys:
                    if k not in rs:
                        rs[k] = default
                return rs
            except IOError:
                self.servers = self.servers[1:] + self.servers[:1]

        log('all backends read failed, with %s' % str(keys))
        raise ReadFailedError(keys, self.servers)

    def set(self, key, value):
        if value is None:
            return False
        for i, s in enumerate(self._get_servers('')):
            if s.set(key, value):
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
        log('all backends set failed, with %s' % str(key))
        raise WriteFailedError(key)

    def set_multi(self, values):
        """
        set_multi will try every proxy until all keys have been set
        if all of proxies have been tried, but there are some keys are failed
        yet, record them in a exception and raise it.
        """
        for i, s in enumerate(self._get_servers('')):
            r, failures = s.set_multi(values, return_failure=True)
            if r:
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
            else:
                values = dict((k, values[k]) for k in failures)
        if failures:
            raise WriteFailedError(failures)

    def delete(self, key):
        for i, s in enumerate(self._get_servers('')):
            if s.delete(key):
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
        log('all backends delete failed, with %s' % str(key))
        #raise DeleteFailedError(key)
        return False

    def delete_multi(self, keys):
        """
        delete_multi will try every proxy until all keys have been deleted.
        if all of proxies have been tried, but there are some keys are failed
        yet, record them in a exception and raise it.
        """
        for i, s in enumerate(self._get_servers('')):
            r, failures = s.delete_multi(keys, return_failure=True)
            if r:
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
            else:
                keys = failures
        if failures:
            #raise DeleteFailedError(failures)
            return False

    def incr(self, key, value):
        if value is None:
            return
        for i, s in enumerate(self._get_servers(key)):
            v = s.incr(key, value)
            if v:
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return v

_empty_slot = '__empty_slot__##'


class CacheWrapper(object):

    """a cached wrapper of BeansDBProxy"""

    def __init__(self, db, mc, delay_cleaner=None):
        self.db = db
        self.mc = mc
        self.delay_cleaner = delay_cleaner


    def __delete_multi_with_delay(self, keys):
        """
        delete_multi maybe useless in concurrence environment.
        so we need delay delete_multi to make sure it work.
        """
        self.mc.delete_multi(keys)
        if self.delay_cleaner:
            for k in keys:
                self.delay_cleaner(k)
        else:
            # dealy delete_multi will conform deleting work
            # it will cover conflict situation.
            self.mc.delete_multi(keys, time=ONE_MINUTE)

    def __set_multi_with_expire(self, values):
        """
        similar with __delete_multi_with_delay
        """
        self.mc.set_multi(values, time=ONE_MINUTE)
        if self.delay_cleaner:
            for k in values:
                self.delay_cleaner(k)
        else:
            self.mc.delete_multi(values.keys(), time=ONE_MINUTE)

    def __set_with_expire(self, key, value):
        """
        set(k, v, time) is the correct answer if no conflict occurs
        if another process/backend set with other expire time,
        set(k, v, time) will not work, so we need a delete(key, time) to
        cover this situation.
        """
        self.mc.set(key, value, time=ONE_MINUTE)
        if self.delay_cleaner:
            self.delay_cleaner(key)
        else:
            # to confirm the expire must work
            # even set with expire was overwriten by an another set action,
            # the delay delete will do the same thing
            self.mc.delete(key, time=ONE_MINUTE)

    def __delete_with_delay(self, key):
        """
        similar with __set_with_expire
        """
        self.mc.delete(key)
        if self.delay_cleaner:
            self.delay_cleaner(key)
        else:
            # to confirm the expire must work
            # even set with expire was overwriten by an another set action,
            # the delay delete will do the same thing
            self.mc.delete(key, time=ONE_MINUTE)

    def get(self, key, default=None):
        """
        _empty_slot is a legacy value, it means mc do not has the key,
        we just clear it. and treat it as mc do not has key's situation.
        if mc has the key, return the value in mc.
        else set a new value into mc, and set expiration is a day duration.
        """
        r = self.mc.get(key)
        if r is not None and r != _empty_slot:
            return r
        else:
            value = self.db.get(key)
            if value is not None:
                self.mc.set(key, value, time=ONE_DAY)
            else:
                value = default
                if r is not None:
                    self.mc.delete(key) #delete _empty_slot from mc
            return value

    def exists(self, key):
        """
        exists is used to test whether the db has the key
        equal to db.get() is not None
        """
        r = self.mc.get(key)
        if r not in (None, _empty_slot):
            return True
        else:
            return self.db.exists(key)

    def get_multi(self, keys, default=None):
        """
        just get the values, do not do anything to mc
        """
        rs = self.mc.get_multi(keys)
        non_exist_keys = [
            k for k in keys if rs.get(k) in (None, _empty_slot)]

        if non_exist_keys:
            nrs = self.db.get_multi(non_exist_keys)
            rs.update((k, v if v is not None else default)
                      for k, v in nrs.iteritems())
            self.mc.set_multi(nrs, time=ONE_DAY)

        return rs

    def set(self, key, value):
        """
        if value is None, it means delete.
        set will cause a set with expire, and a delayed delete.
        if db.set failed, should clean mc twice
        """
        try:
            if value is None:
                log("%s is deleted in both mc and db explicitly" % key)
                self.db.delete(key)
            else:
                self.db.set(key, value)
            self.__set_with_expire(key, value)
            return True
        except:
            self.__delete_with_delay(key)
            raise

    def set_multi(self, values):
        try:
            self.db.set_multi(values)
            self.__set_multi_with_expire(values)
            # because BeansDBProxy's set_multi will return True or raise
            return True
        except:
            self.__delete_multi_with_delay(values.keys())
            raise

    def delete(self, key):
        try:
            return self.db.delete(key)
        finally:
            self.__delete_with_delay(key)

    def delete_multi(self, keys):
        try:
            return self.db.delete_multi(keys)
        finally:
            self.__delete_multi_with_delay(keys)

    def incr(self, key, value):
        if value is None:
            return
        try:
            r = self.db.incr(key, value)
        finally:
            self.__delete_with_delay(key)
        return r

    def clear_thread_ident(self):
        self.mc.clear_thread_ident()


def beansdb_from_config(config, mc=None, direct=False, delay_cleaner=None, **kwargs):
    if isinstance(config, basestring):
        config = read_config(config, 'beansdb')

    # to be compatible
    if isinstance(config, list):
        nodes = config
    else:
        nodes = config.get('servers') if direct else config.get('proxies')

    db = BeansdbClient(
        nodes, **kwargs) if direct else BeansDBProxy(nodes, **kwargs)

    if mc:
        db = CacheWrapper(db, mc, delay_cleaner=delay_cleaner)

    return db
