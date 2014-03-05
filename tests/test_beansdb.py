#!/usr/bin/env python
# encoding: utf-8
"""
test_beansdb.py
"""

import unittest
from mock import patch, Mock
from nose.tools import raises
from functools import wraps


from douban.beansdb import BeansDBProxy, CacheWrapper, ReadFailedError, \
    MCStore, WriteFailedError, _empty_slot, DeleteFailedError


from douban.mc.debug import LocalMemcache
from douban.utils import ThreadedObject
import douban.utils.slog

key = "k"
value = "hello"


class FakeMCStore(object):

    def __init__(self):
        pass

    def get_fail(self, *args):
        raise IOError()

    def get_multi(self, *args):
        raise IOError()

    def update_fail(self, *args):
        pass  # nothing to do

    def __getattr__(self, name):
        if name.startswith('get'):
            return self.get_fail
        elif name.startswith('set') or name.startswith('delete'):
            return self.update_fail


class LocalMCStore(MCStore):

    def __init__(self, threaded=True):
        if threaded:
            self.mc = ThreadedObject(LocalMemcache)
        else:
            self.mc = LocalMemcache()

    def clear(self):
        self.mc.clear()

    def exists(self, key):
        return bool(self.get(key))


class LocalBeansDBProxy(BeansDBProxy):
    store_cls = staticmethod(lambda i, **kw: LocalMCStore(**kw))

    def __init__(self, **kw):
        BeansDBProxy.__init__(self, [None], **kw)


class ThreadlessLocalBeansDBProxy(BeansDBProxy):
    threaded = False
    store_cls = staticmethod(lambda i, **kw: LocalMCStore(**kw))

    def __init__(self, **kw):
        BeansDBProxy.__init__(self, [None], **kw)


class FakeBeansDBProxy(BeansDBProxy):
    store_cls = staticmethod(lambda i, **kw: FakeMCStore())

    def __init__(self, **kw):
        BeansDBProxy.__init__(self, [None], **kw)


class BeansdbTest(unittest.TestCase):

    def setUp(self):
        self.db = LocalBeansDBProxy()

    def test_beansdb_can_set_value(self):
        r = self.db.set(key, value)
        self.assert_(self.db.set(key, value))
        assert self.db.exists(key)
        assert self.db.get(key) == value

    def test_beansdb_can_delete_value(self):
        self.db.set(key, value)
        self.db.delete(key)
        assert not self.db.exists(key)
        assert self.db.get(key) is None

    @raises(IOError)
    @patch('douban.mc.debug.LocalMemcache.get')
    def test_get_should_raise_exception_when_backends_failed(self, mock_get):
        mock_get.side_effect = IOError()
        self.db.get('break-server!')

    @raises(IOError)
    @patch('douban.mc.debug.LocalMemcache.get')
    def test_get_should_raise_exception_when_backends_failed_even_if_default_is_given(
            self, mock_get):
        mock_get.side_effect = IOError()
        self.db.get('break-server!', 'a default value')

    def test_get_multi(self):
        keys = ['test_key:%d' % i for i in range(10)]
        values = dict((k, 'value') for k in keys)
        assert self.db.set_multi(values)
        assert self.db.get_multi(keys) == values
        assert self.db.delete_multi(keys)
        assert self.db.get_multi(keys) == dict((k, None) for k in keys)

    def test_set_multi(self):
        keys = ['test_set_multi_key:%d' % i for i in range(10)]
        values = dict((k, k + ':value') for k in keys)
        assert self.db.set_multi(values)
        assert self.db.get_multi(keys) == values
        values = dict((k, k + ':value1') for k in keys)
        assert self.db.set_multi(values)
        assert self.db.get_multi(keys) == values
        assert self.db.delete_multi(keys)
        assert self.db.get_multi(keys) == dict((k, None) for k in keys)

    #@raises(DeleteFailedError)
    @patch('douban.mc.debug.LocalMemcache.delete_multi')
    def test_delete_multi_raise(self, mock_delete_multi):
        def side_effect(*args, **kw):
            return False, ['test_set_multi_key:%d' % i for i in (1, 2, 3)]
        keys = ['test_set_multi_key:%d' % i for i in range(10)]
        values = dict((k, k + ':value') for k in keys)
        assert self.db.set_multi(values)

        mock_delete_multi.side_effect = side_effect
        assert self.db.delete_multi(keys) is False

    @raises(WriteFailedError)
    @patch('douban.mc.debug.LocalMemcache.set_multi')
    def test_set_multi_raise(self, mock_set_multi):
        def side_effect(*args, **kw):
            return False, ['test_set_multi_key:%d' % i for i in (1, 2, 3)]
        mock_set_multi.side_effect = side_effect
        keys = ['test_set_multi_key:%d' % i for i in range(10)]
        values = dict((k, k + ':value') for k in keys)
        assert self.db.set_multi(values) is None

    @raises(IOError)
    @patch('douban.mc.debug.LocalMemcache.get_multi')
    def test_get_multi_on_bad_servers_should_raise_IOError(self, mock_get):
        mock_get.side_effect = IOError()
        keys = ['test_key:%d' % i for i in range(10)]
        self.db.get_multi(keys)


class ThreadlessBeansdbTest(BeansdbTest):

    def setUp(self):
        self.db = ThreadlessLocalBeansDBProxy()


class FakeBeansdbTest(unittest.TestCase):

    def setUp(self):
        self.db = FakeBeansDBProxy()

    def test_fake_beansdb_should_raise_exception(self):
        self.failUnlessRaises(ReadFailedError, self.db.get, key)
        # self.failUnlessRaises(WriteFailedError, self.db.set, key, value)
        self.failUnlessRaises(Exception, self.db.set, key, value)


class CachedBeansdbTest(BeansdbTest):

    def setUp(self):
        self.m = Mock()
        self.mc = LocalMemcache()
        self.db = CacheWrapper(LocalBeansDBProxy(), self.mc)

    @patch('douban.mc.debug.LocalMemcache.delete')
    @patch('douban.mc.debug.LocalMemcache.set')
    def test_set_should_set_in_tmp(self, mock_set, mock_delete):
        self.assertTrue(self.db.set(key, value))
        if self.db.delay_cleaner is None:
            mock_set.assert_called_with(key, value, time=60)
            mock_delete.assert_called_with(key, time=60)
        else:
            mock_set.assert_called_with(key, value, time=60)
            self.m.assert_called_with(key)
        self.assertEqual(mock_set.call_count, 2)
        #mock_db_set.assert_called_with(key, value)

    def test_should_replace_empty_to_None(self):
        non_exist_key = "non_exist_key"
        self.mc.set(non_exist_key, _empty_slot)
        self.assertTrue(not self.db.exists(non_exist_key))
        self.assertEqual(self.db.get(non_exist_key), None)
        self.assertTrue(self.mc.get(non_exist_key) is None)
        self.assertEqual(self.db.get(non_exist_key, []), [])
        self.assertEqual(self.db.get(non_exist_key), None)

        non_exist_keys = [non_exist_key + str(i) for i in xrange(10)]
        self.assertTrue(all(v is None
                            for k, v in
                            self.db.get_multi(non_exist_keys).items()))
        self.assertFalse(any(v is _empty_slot
                            for k, v in
                            self.mc.get_multi(non_exist_keys).items()))
        self.assertTrue(all(v == {}
                            for k, v in
                            self.db.get_multi(non_exist_keys, {}).items()))
        self.assertTrue(all(v is None
                            for k, v in
                            self.db.get_multi(non_exist_keys).items()))

    @patch('douban.mc.debug.LocalMemcache.set')
    @patch('douban.mc.debug.LocalMemcache.delete')
    def test_set_fail_will_cause_delete(self, mock_delete, mock_set):
        mock_set.side_effect = WriteFailedError(key)
        self.assertRaises(WriteFailedError, self.db.set, key, value)
        if self.db.delay_cleaner:
            mock_delete.assert_called_with(key)
            self.m.assert_called_with(key)
        else:
            mock_delete.assert_called_with(key, time=60)
            self.assertEqual(2, mock_delete.call_count)

    def test_set_value_if_origin_value_is_none(self):
        empty_key = "empty_key"
        actual_value = "actual_value"
        self.db.db.set(empty_key, actual_value)
        self.assertEqual(self.db.get(empty_key), actual_value)
        self.assertEqual(self.mc.get(empty_key), actual_value)

    def test_get_should_ignore_empty_slot(self):
        empty_key = "empty_key"
        actual_value = "actual_value"
        self.mc.set(empty_key, _empty_slot)
        self.db.db.set(empty_key, actual_value)
        self.assertEqual(self.db.get(empty_key), actual_value)
        self.assertEqual(self.mc.get(empty_key), actual_value)

    def test_get_return_default_while_mc_empty_or_empty_slot(self):
        empty_key = "empty_key"
        actual_value = "actual_value"
        self.assertEqual(
            self.db.get('empty_key', default='default'), 'default')
        self.mc.set(empty_key, _empty_slot)
        self.assertEqual(
            self.db.get('empty_key', default='default'), 'default')
        self.mc.set(empty_key, '2323')
        self.assertNotEqual(
            self.db.get('empty_key', default='default'), 'default')

    def test_get_will_set_with_expire(self):
        self.db.db.set(key, value)
        @patch('douban.mc.debug.LocalMemcache.set')
        def _(mock_set):
            self.assertEqual(self.db.get(key), value)
            mock_set.assert_called_with(key, value, time=24* 3600)
        _()

    @patch('douban.mc.debug.LocalMemcache.delete')
    def test_get_None_will_delete_mc(self, mock_delete):
        self.mc.set(key, _empty_slot)
        self.assertEqual(self.db.get(key), None)
        mock_delete.assert_called_with(key)
        self.assertEqual(mock_delete.call_count, 1)

    def test_set_None_clear_mc_and_db(self):
        empty_key = "empty_key"
        old_value = "old_value"
        self.mc.set(empty_key, old_value)
        self.db.db.set(empty_key, old_value)
        self.assertTrue(self.mc.get(empty_key), old_value)
        self.assertTrue(self.db.db.get(empty_key), old_value)
        self.db.set(empty_key, None)
        self.assertTrue(self.mc.get(empty_key) == None)
        self.assertTrue(self.db.db.get(empty_key) == None)
        self.assertTrue(self.db.get(empty_key) == None)

    def test_set_multi_success_result_correct(self):
        key1 = "test_key1"
        key2 = "test_key2"
        key3 = "test_key3"
        v1 = "value1"
        v2 = "value2"
        v3 = "value3"
        #self.mc.set(key1, v1)
        #self.mc.set(key2, v2)
        #self.mc.set(key3, v3)
        self.db.set_multi({key1: v1, key2: v2, key3: v3})
        if self.db.delay_cleaner:
            self.assertEqual(self.mc.get(key1), v1)
            self.assertEqual(self.mc.get(key2), v2)
            self.assertEqual(self.mc.get(key3), v3)
            self.assertEqual(self.m.call_count, 3)
        else:
            self.assertEqual(self.mc.get(key1), None)
            self.assertEqual(self.mc.get(key2), None)
            self.assertEqual(self.mc.get(key3), None)


    @patch('douban.mc.debug.LocalMemcache.set_multi')
    @patch('douban.mc.debug.LocalMemcache.delete_multi')
    def test_set_multi_success(self, mock_delete_multi, mock_set_multi):
        mock_set_multi.return_value = True, []
        key1 = "test_key1"
        key2 = "test_key2"
        key3 = "test_key3"
        v1 = "value1"
        v2 = "value2"
        v3 = "value3"
        values = {key1: v1, key2: v2, key3: v3}
        self.db.set_multi(values)
        mock_set_multi.assert_called_with(values, time=60)
        self.assertEqual(mock_set_multi.call_count, 2)
        if self.db.delay_cleaner:
            self.assertFalse(mock_delete_multi.called)
            self.assertEqual(self.m.call_count, len(values))
        else:
            mock_delete_multi.assert_called_with(values.keys(), time=60)

    @patch('douban.mc.debug.LocalMemcache.set_multi')
    @patch('douban.mc.debug.LocalMemcache.delete_multi')
    def test_set_multi_failed(self, mock_delete_multi, mock_set_multi):
        key1 = "test_key1"
        key2 = "test_key2"
        key3 = "test_key3"
        v1 = "value1"
        v2 = "value2"
        v3 = "value3"
        mock_set_multi.return_value = False, [key1]
        values = {key1: v1, key2: v2, key3: v3}
        self.assertRaises(WriteFailedError, self.db.set_multi, values)
        if self.db.delay_cleaner:
            self.assertEqual(mock_delete_multi.call_count, 1)
            self.assertEqual(self.m.call_count, len(values))
            mock_delete_multi.assert_called_with(values.keys())
        else:
            self.assertEqual(mock_delete_multi.call_count, 2)
            mock_delete_multi.assert_called_with(values.keys(), time=60)

    def test_delete_multi_should_clear_mc_first(self):
        key1 = "test_key1"
        key2 = "test_key2"
        key3 = "test_key3"
        v1 = "value1"
        v2 = "value2"
        v3 = "value3"
        self.mc.set(key1, v1)
        self.mc.set(key2, v2)
        self.mc.set(key3, v3)
        self.db.delete_multi([key1, key2, key3])
        self.assertTrue(self.mc.get(key1) is None)
        self.assertTrue(self.mc.get(key2) is None)
        self.assertTrue(self.mc.get(key3) is None)

    def test_get_multi_handle_empty_slot(self):
        keys = ['key1', 'key2', 'key3', 'key4', 'key5']
        self.mc.set('key1', 'value1')
        self.mc.set('key3', _empty_slot)
        self.mc.set('key4', 'value4')
        self.db.db.set('key5', 'value5')
        self.db.db.set('key4', 'value44')
        self.db.db.set('key2', 'value2')
        rs = self.db.get_multi(keys)
        self.assertTrue(rs == {'key1': 'value1', 'key2': 'value2',
                               'key3': None, 'key4': 'value4', 'key5': 'value5'})

    def test_exist_ignore_empty_slot(self):
        keys = ['key1', 'key2', 'key3', 'key4', 'key5']
        self.mc.set('key1', 'value1')
        self.mc.set('key3', _empty_slot)
        self.mc.set('key4', 'value4')
        self.db.db.set('key5', 'value5')
        self.db.db.set('key4', 'value44')
        self.db.db.set('key3', 'value3')
        self.assertTrue(self.db.exists('key1'))
        self.assertTrue(not self.db.exists('key2'))
        self.assertTrue(self.db.exists('key3'))
        self.assertTrue(self.db.exists('key4'))
        self.assertTrue(self.db.exists('key5'))

    @patch('douban.mc.debug.LocalMemcache.delete')
    def test_delete_with_expire(self, mock_delete):
        self.mc.set(key, value)
        self.db.db.set(key, value)
        self.db.delete(key)
        if self.db.delay_cleaner:
            self.m.assert_called_with(key)
            self.assertEqual(mock_delete.call_count, 2)
            mock_delete.assert_called_with(key)
        else:
            self.assertEqual(mock_delete.call_count, 3)
            mock_delete.assert_called_with(key, time=60)


def test_rechoose_proxy_after_given_period():
    class Store(object):

        def __init__(self, addr, **kw):
            self.addr = addr

        def get(self, key):
            return None

    class Proxy(BeansDBProxy):
        store_cls = Store

    with patch('random.shuffle') as mock_shuffle, \
            patch('time.time') as mock_time:
        mock_shuffle.side_effect = lambda x: x
        mock_time.return_value = 1000

        db = Proxy(['server1', 'server2', 'server3', 'server4'])
        db.get('key')
        assert db.servers[0].addr == 'server1'

        with patch('time.time') as mock_time2:
            mock_time2.return_value = 2000
            db.get('key')
            assert db.servers[0].addr == 'server2'

class DelayCleanTest(CachedBeansdbTest):

    def setUp(self):
        self.m = Mock()
        self.mc = LocalMemcache()
        self.db = CacheWrapper(LocalBeansDBProxy(), self.mc, delay_cleaner=self.m)


class LogTest(BeansdbTest):

    def test_log_without_scribe(self):
        temp = douban.utils.slog.scribeclient
        douban.utils.slog.scribeclient = None
        from douban.beansdb import log
        with patch('sys.stderr') as mock_stderr:
            log('testmessage')
            assert mock_stderr.write.called
        douban.utils.slog.scribeclient = temp

    def test_log_with_scribe(self):
        mock = Mock()
        temp = douban.utils.slog.scribeclient
        douban.utils.slog.scribeclient = mock
        from douban.beansdb import log
        log('test-message')
        assert mock.send.called
        douban.utils.slog.scribeclient = temp

if __name__ == '__main__':
    unittest.main()
