"""Test the queue functionality"""

from common import TestQless


class TestResources(TestQless):
    """We should be able to access resources"""
    def test_malformed(self):
        self.assertMalformed(self.lua, [
            ('resource.set', 0),
            ('resource.set', 0, 'test'),
            ('resource.set', 0, 'test', 'sfdgl'),
            ('resource.get', 0),
            ('resource.unset', 0),
            ('resource.locks', 0),
        ])

    def test_set(self):
        res = self.lua('resource.set', 0, 'test', 5)

        self.assertEquals(res, 'test')

    def test_get(self):
        self.lua('resource.set', 0, 'test', 5)
        res = self.lua('resource.get', 0, 'test')

        self.assertEquals(res['rid'], 'test')
        self.assertEquals(res['count'], 5)

    def test_unset(self):
        self.lua('resource.set', 0, 'test', 5)
        self.assertIsInstance(self.lua('resource.get', 0, 'test'), dict)
        self.lua('resource.unset', 0, 'test')
        self.assertIsNone(self.lua('resource.get', 0, 'test'))

    def test_locks(self):
        self.lua('resource.set', 0, 'test', 5)
        locks = self.lua('resource.locks', 0, 'test')
        self.assertEquals(locks, 0)

    def test_does_not_add_lock_and_pending(self):
        self.lua('resource.set', 0, 'r-1', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('resource.get', 0, 'r-1')

        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

