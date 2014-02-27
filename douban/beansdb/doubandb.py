#!/usr/bin/env python
# encoding: utf-8

from fnv1a import get_hash as _fnv1a

BUCKETS_COUNT = 16

from douban.beansdb import MCStore

def fnv1a(s):
    return 0xffffffff & _fnv1a(s)
    
class DoubanDB(object):
    store_cls = MCStore
    hash_space = 1<<32
    cached = True
    def __init__(self, servers, buckets_count=BUCKETS_COUNT, N=3, W=1, R=1):
        self.buckets_count = buckets_count
        self.bucket_size = self.hash_space / buckets_count
        self.servers = {}
        self.server_buckets = {}
        self.buckets = [[] for i in range(buckets_count)]
        for s,bs in servers.items():
            server = self.store_cls(s)
            self.servers[s] = server
            self.server_buckets[s] = bs
            for b in bs:
                self.buckets[b].append(server)
        for b in range(self.buckets_count):
            self.buckets[b].sort(key=lambda x:fnv1a("%d:%s:%d"%(b,x,b)))
        self.N = N
        self.W = W
        self.R = R

    def _get_servers(self, key):
        hash = fnv1a(key)
        b = hash / self.bucket_size
        return self.buckets[b]

    def __getattr__(self, name):
        if name in ['exists', 'gets']:
            def _(key, *args, **kwargs):
                for s in self._get_servers(key):
                    r = getattr(s, name)(key, *args, **kwargs)
                    if r:
                        return r
            return _
        raise AttributeError(name)

