#!/usr/bin/env python
# encoding: utf-8

import cmemcached
from douban.beansdb import MCStore, BeansdbClient, BeansDBProxy
from douban.utils import ThreadedObject
from douban.utils.config import read_config

def connect(server, **kwargs):
    c = cmemcached.Client([server], do_split=0, **kwargs)
    c.set_behavior(cmemcached.BEHAVIOR_CONNECT_TIMEOUT, 100)   # 0.1 s
    c.set_behavior(cmemcached.BEHAVIOR_POLL_TIMEOUT, 5*1000)  # 5 s
    c.set_behavior(cmemcached.BEHAVIOR_SERVER_FAILURE_LIMIT, 4)
    c.set_behavior(cmemcached.BEHAVIOR_RETRY_TIMEOUT, 10) # 10 s
    c.set_behavior(cmemcached.BEHAVIOR_SUPPORT_CAS, 0)
    return c

class FSStore(MCStore):
    def __init__(self, server, threaded=True, **kwargs):
        self.addr = server
        if threaded:
            self.mc = ThreadedObject(connect, server, **kwargs)
        else:
            self.mc = connect(server, **kwargs)

class DoubanFS(BeansDBProxy):
    store_cls = FSStore

    def rename(self, path, new_path):
        data = self.get(path)
        return data and self.set(new_path, data) and self.delete(path)

class OfflineDoubanFS(BeansdbClient):

    store_cls = FSStore

    def rename(self, path, new_path):
        data = self.get(path)
        return data and self.set(new_path, data) and self.delete(path)

def doubanfs_from_config(config, offline=False, **kwargs):
    if isinstance(config, basestring):
        config = read_config(config, 'doubanfs')

    if 'offline' in config:
        offline = config['offline']

    if not isinstance(config, list):
        config = config.get(offline and 'servers' or 'proxies')

    if offline:
        return OfflineDoubanFS(config, **kwargs)
    else:
        return DoubanFS(config, **kwargs)
