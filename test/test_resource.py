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
        self.assertEquals(res['max'], 5)

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

    def test_handles_increasing_resource_limit(self):
        """Resource will eventually raise when jobs complete"""
        self.lua('resource.set', 0, 'r-1', 2)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1', 'jid-2'])
        self.assertEqual(res['pending'], {})

        self.lua('put', 0, None, 'queue', 'jid-3', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1', 'jid-2'])
        self.assertEqual(res['pending'], ['jid-3'])

        self.lua('resource.set', 0, 'r-1', 3)
        self.lua('pop', 2, 'queue', 'worker-1', 1)
        self.lua('complete', 3, 'jid-2', 'worker-1', 'queue', {})
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1', 'jid-3'])
        self.assertEqual(res['pending'], {})

    def test_handles_decreasing_resource_limit(self):
        """Resource decrease will eventually lower as work processes"""
        self.lua('resource.set', 0, 'r-1', 3)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-3', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1', 'jid-2', 'jid-3'])
        self.assertEqual(res['pending'], {})
        self.lua('resource.set', 0, 'r-1', 1)
        jobs = self.lua('pop', 2, 'queue', 'worker-1', 3)

        self.lua('complete', 3, 'jid-1', 'worker-1', 'queue', {})
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2', 'jid-3'])
        self.assertEqual(res['pending'], {})

        self.lua('complete', 3, 'jid-2', 'worker-1', 'queue', {})
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-3'])
        self.assertEqual(res['pending'], {})

        self.lua('complete', 3, 'jid-3', 'worker-1', 'queue', {})
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

        self.lua('put', 0, None, 'queue', 'jid-4', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-5', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-4'])
        self.assertEqual(res['pending'], ['jid-5'])

    def test_handles_setting_resource_limit_to_zero(self):
        """Resource decrease will eventually lower as work processes"""
        self.lua('resource.set', 0, 'r-1', 1)

        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        self.lua('resource.set', 0, 'r-1', 0)
        self.lua('pop', 2, 'queue', 'worker-1', 1)
        self.lua('complete', 3, 'jid-1', 'worker-1', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], ['jid-2'])

        jobs = self.lua('pop', 2, 'queue', 'worker-1', 1)
        self.assertEqual(jobs, {})

        self.lua('put', 0, None, 'queue', 'jid-3', 'klass', {}, 0, 'resources', ['r-1'])
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], ['jid-3', 'jid-2'])
