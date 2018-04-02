'''Test job-centric operations'''

import redis
from common import TestQless


class TestJob(TestQless):
    '''Some general jobby things'''
    def test_malformed(self):
        '''Enumerate all malformed input to priority'''
        self.assertMalformed(self.lua, [
            ('priority', '0'),
            ('priority', '0', 'jid'),
            ('priority', '0', 'jid', 'foo')
        ])

    def test_log(self):
        '''Can add a log to a job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('log', 0, 'jid', 'foo', {'foo': 'bar'})
        self.assertEqual(self.lua('get', 0, 'jid')['history'], [
            {'q': 'queue', 'what': 'put', 'when': 0},
            {'foo': 'bar', 'what': 'foo', 'when': 0}
        ])

    def test_log_nonexistent(self):
        '''If a job doesn't exist, logging throws an error'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'log', 0, 'jid', 'foo', {'foo': 'bar'})

    def test_history(self):
        '''We only keep the most recent max-job-history items in history'''
        self.lua('config.set', 0, 'max-job-history', 5)
        for index in range(100):
            self.lua('put', index, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('get', 0, 'jid')['history'], [
            {'q': 'queue', 'what': 'put', 'when': 0},
            {'q': 'queue', 'what': 'put', 'when': 96},
            {'q': 'queue', 'what': 'put', 'when': 97},
            {'q': 'queue', 'what': 'put', 'when': 98},
            {'q': 'queue', 'what': 'put', 'when': 99}])

class TestRequeue(TestQless):
    def test_requeue_existing_job(self):
        '''Requeueing an existing job is identical to `put`'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('requeue', 1, 'worker', 'queue-2', 'jid', 'klass', {}, 0)
        self.assertEqual(self.lua('get', 0, 'jid')['queue'], 'queue-2')

    def test_requeue_cancelled_job(self):
        '''Requeueing a cancelled (or non-existent) job fails'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('cancel', 1, 'jid')
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'requeue', 2, 'worker', 'queue-2', 'jid', 'klass', {}, 0)

class TestComplete(TestQless):
    '''Test how we complete jobs'''
    def test_malformed(self):
        '''Enumerate all the way they can be malformed'''
        self.assertMalformed(self.lua, [
            ('complete', 0, 'jid', 'worker', 'queue', {}, 'next'),
            ('complete', 0, 'jid', 'worker', 'queue', {}, 'delay'),
            ('complete', 0, 'jid', 'worker', 'queue', {}, 'delay', 'foo'),
            ('complete', 0, 'jid', 'worker', 'queue', {}, 'depends'),
            ('complete', 0, 'jid', 'worker', 'queue', {}, 'depends', '[}'),
            # Can't have 'depends' with a delay
            ('complete', 0, 'jid', 'worker', 'queue', {},
                'depends', ['foo'], 'delay', 5),
            # Can't have 'depends' without 'next'
            ('complete', 0, 'jid', 'worker', 'queue', {}, 'depends', ['foo'])
        ])

    def test_complete_waiting(self):
        '''Only popped jobs can be completed'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.assertRaisesRegexp(redis.ResponseError, r'waiting',
            self.lua, 'complete', 1, 'jid', 'worker', 'queue', {})
        # Pop it and it should work
        self.lua('pop', 2, 'queue', 'worker', 10)
        self.lua('complete', 1, 'jid', 'worker', 'queue', {})

    def test_complete_depends(self):
        '''Cannot complete a dependent job'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'depends',
            self.lua, 'complete', 1, 'b', 'worker', 'queue', {})

    def test_complete_scheduled(self):
        '''Cannot complete a scheduled job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.assertRaisesRegexp(redis.ResponseError, r'scheduled',
            self.lua, 'complete', 1, 'jid', 'worker', 'queue', {})

    def test_complete_nonexistent(self):
        '''Cannot complete a job that doesn't exist'''
        self.assertRaisesRegexp(redis.ResponseError, r'does not exist',
            self.lua, 'complete', 1, 'jid', 'worker', 'queue', {})
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('complete', 1, 'jid', 'worker', 'queue', {})

    def test_complete_failed(self):
        '''Cannot complete a failed job'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.assertRaisesRegexp(redis.ResponseError, r'failed',
            self.lua, 'complete', 0, 'jid', 'worker', 'queue', {})

    def test_complete_previously_failed(self):
        '''Erases failure data after completing'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('fail', 2, 'jid', 'worker', 'group', 'message', {})
        self.lua('put', 3, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 4, 'queue', 'worker', 10)
        self.assertEqual(self.lua('get', 5, 'jid')['failure'], {
            'group': 'group',
            'message': 'message',
            'when': 2,
            'worker': 'worker'})
        self.lua('complete', 6, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('get', 7, 'jid')['failure'], {})

    def test_basic(self):
        '''Basic completion'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('complete', 2, 'jid', 'worker', 'queue', {})
        self.assertEqual(self.lua('get', 3, 'jid'), {
            'data': '{}',
            'dependencies': {},
            'dependents': {},
            'expires': 0,
            'failure': {},
            'history': [{'q': 'queue', 'what': 'put', 'when': 0},
                        {'what': 'popped', 'when': 1, 'worker': 'worker'},
                        {'what': 'done', 'when': 2}],
            'jid': 'jid',
            'klass': 'klass',
            'priority': 0,
            'queue': u'',
            'remaining': 5,
            'retries': 5,
            'state': 'complete',
            'tags': {},
            'tracked': False,
            'worker': u'',
            'spawned_from_jid': False,
            'resources': {}})

    def test_advance(self):
        '''Can complete and advance a job in one fell swooop'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('complete', 2, 'jid', 'worker', 'queue', {}, 'next', 'foo')
        self.assertEqual(
            self.lua('pop', 3, 'foo', 'worker', 10)[0]['jid'], 'jid')

    def test_advance_empty_array_mangle(self):
        '''Does not mangle empty arrays in job data when advancing'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', '[]', 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('complete', 2, 'jid', 'worker', 'queue', '[]', 'next', 'foo')
        self.assertEqual(
            self.lua('pop', 3, 'foo', 'worker', 10)[0]['data'], '[]')

    def test_wrong_worker(self):
        '''Only the right worker can complete it'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another worker',
            self.lua, 'complete', 2, 'jid', 'another', 'queue', {})

    def test_wrong_queue(self):
        '''A job can only be completed in the queue it's in'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.assertRaisesRegexp(redis.ResponseError, r'another queue',
            self.lua, 'complete', 2, 'jid', 'worker', 'another-queue', {})

    def test_expire_complete_count(self):
        '''Jobs expire after a k complete jobs'''
        self.lua('config.set', 0, 'jobs-history-count', 5)
        jids = range(10)
        for jid in range(10):
            self.lua('put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        for jid in jids:
            self.lua('complete', 2, jid, 'worker', 'queue', {})
        existing = [self.lua('get', 3, jid) for jid in range(10)]
        self.assertEqual(len([i for i in existing if i]), 5)

    def test_expire_complete_time(self):
        '''Jobs expire after a certain amount of time'''
        self.lua('config.set', 0, 'jobs-history', -1)
        jids = range(10)
        for jid in range(10):
            self.lua('put', 0, 'worker', 'queue', jid, 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        for jid in jids:
            self.lua('complete', 2, jid, 'worker', 'queue', {})
        existing = [self.lua('get', 3, jid) for jid in range(10)]
        self.assertEqual([i for i in existing if i], [])

    def test_complete_releases_resources(self):
        """Can cancel running jobs, prevents heartbeats"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('complete', 3, 'jid', 'worker', 'queue', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})


    def test_complete_releases_resources_for_next_job(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])
        jobs = self.lua('pop', 1, 'queue', 'worker-1', 10)
        self.assertEqual(len(jobs), 1)

        # can't pop next job, as resources are not available
        jobs = self.lua('pop', 1, 'queue', 'worker-2', 10)
        self.assertEqual(jobs, {})

        self.lua('complete', 3, 'jid-1', 'worker-1', 'queue', {})

        # job can now be popped as resource is available
        jobs = self.lua('pop', 1, 'queue', 'worker-2', 10)
        self.assertEqual(len(jobs), 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})



class TestCancel(TestQless):
    '''Canceling jobs'''
    def test_cancel_waiting(self):
        '''You can cancel waiting jobs'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('cancel', 0, 'jid')
        self.assertEqual(self.lua('get', 0, 'jid'), None)

    def test_cancel_depends(self):
        '''You can cancel dependent job'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.lua('cancel', 0, 'b')
        self.assertEqual(self.lua('get', 0, 'b'), None)
        self.assertEqual(self.lua('get', 0, 'a')['dependencies'], {})

    def test_cancel_dependents(self):
        '''Cannot cancel jobs if they still have dependencies'''
        self.lua('put', 0, 'worker', 'queue', 'a', 'klass', {}, 0)
        self.lua('put', 0, 'worker', 'queue', 'b', 'klass', {}, 0, 'depends', ['a'])
        self.assertRaisesRegexp(redis.ResponseError, r'dependency',
            self.lua, 'cancel', 0, 'a')

    def test_cancel_scheduled(self):
        '''You can cancel scheduled jobs'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 1)
        self.lua('cancel', 0, 'jid')
        self.assertEqual(self.lua('get', 0, 'jid'), None)

    def test_cancel_nonexistent(self):
        '''Can cancel jobs that do not exist without failing'''
        self.lua('cancel', 0, 'jid')

    def test_cancel_failed(self):
        '''Can cancel failed jobs'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 1, 'jid', 'worker', 'group', 'message', {})
        self.lua('cancel', 2, 'jid')
        self.assertEqual(self.lua('get', 3, 'jid'), None)

    def test_cancel_running(self):
        '''Can cancel running jobs, prevents heartbeats'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('heartbeat', 2, 'jid', 'worker', {})
        self.lua('cancel', 3, 'jid')
        self.assertRaisesRegexp(redis.ResponseError, r'Job jid does not exist',
            self.lua, 'heartbeat', 4, 'jid', 'worker', {})

    def test_cancel_retries(self):
        '''Can cancel job that has been failed from retries through retry'''
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('get', 2, 'jid')['state'], 'running')
        self.lua('retry', 3, 'jid', 'queue', 'worker')
        self.lua('cancel', 4, 'jid')
        self.assertEqual(self.lua('get', 5, 'jid'), None)

    def test_cancel_pop_retries(self):
        '''Can cancel job that has been failed from retries through pop'''
        self.lua('config.set', 0, 'heartbeat', -10)
        self.lua('config.set', 0, 'grace-period', 0)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('pop', 2, 'queue', 'worker', 10)
        self.lua('cancel', 3, 'jid')
        self.assertEqual(self.lua('get', 4, 'jid'), None)

    def test_returns_cancelled_jids(self):
        self.lua('put', 0, 'worker', 'queue', 'jid-1', 'klass', {}, 0, 'retries', 0)
        self.lua('put', 0, 'worker', 'queue', 'jid-2', 'klass', {}, 0, 'retries', 0)
        self.lua('pop', 1, 'queue', 'worker', 10)
        res = self.lua('cancel', 3, 'jid-1', 'jid-2', 'jid-3')

        self.assertEqual(res, ['jid-1', 'jid-2'])

    def test_cancel_waiting_releases_acquired_resources(self):
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid'])

        self.lua('cancel', 3, 'jid')

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})

    def test_cancel_waiting_releases_pending_resources(self):
        """Can cancel a job waiting on resources and it will release pending resources"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, 'worker', 'queue', 'jid-1', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('put', 0, 'worker', 'queue', 'jid-2', 'klass', {}, 0, 'resources', ['r-1'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], ['jid-2'])

        self.lua('cancel', 3, 'jid-2')

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

    def test_cancel_running_releases_resources(self):
        """Can cancel running jobs, releases acquired resources"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('pop', 1, 'queue', 'worker', 10)
        self.lua('heartbeat', 2, 'jid', 'worker', {})
        self.lua('cancel', 3, 'jid')

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

    def test_failed_releases_resources(self):
        """Can fail job, releases acquired resources"""
        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, 'worker', 'queue', 'jid', 'klass', {}, 0, 'resources', ['r-1'])
        self.lua('pop', 0, 'queue', 'worker', 10)
        self.lua('fail', 1, 'jid', 'worker', 'group', 'message', {})

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})


class TestJobsWithResources(TestQless):
    """Tests jobs which require resources"""

    def test_exhausted_retry_releases_resources(self):
        """Job exhausts retries, releases acquired resources"""

        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, 'worker', 'queue', 'jid-1', 'klass', {}, 0, 'retries', 0, 'resources', ['r-1'])

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-1'])
        self.assertEqual(res['pending'], {})

        self.lua('pop', 1, 'queue', 'worker', 10)
        self.assertEqual(self.lua('get', 2, 'jid-1')['state'], 'running')

        self.lua('retry', 3, 'jid-1', 'queue', 'worker')
        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

    def test_expired_releases_resources(self):
        """Job times out, releases acquired resources"""

        self.lua('config.set', 0, 'heartbeat', 10)
        self.lua('config.set', 0, 'grace-period', 0)

        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'retries', 0, 'resources', ['r-1'])

        jobs = self.lua('pop', 1, 'queue', 'worker-1', 1)
        jobs = self.lua('pop', 22, 'queue', 'worker-2', 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], {})
        self.assertEqual(res['pending'], {})

    def test_expired_releases_resources_to_next_job(self):
        """Job times out, releases acquired resources and assigns to next job"""

        self.lua('config.set', 0, 'heartbeat', 10)
        self.lua('config.set', 0, 'grace-period', 0)

        self.lua('resource.set', 0, 'r-1', 1)
        self.lua('put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'retries', 0, 'resources', ['r-1'])
        self.lua('put', 0, None, 'queue', 'jid-2', 'klass', {}, 0, 'retries', 0, 'resources', ['r-1'])

        jobs = self.lua('pop', 1, 'queue', 'worker-1', 1)
        jobs = self.lua('pop', 22, 'queue', 'worker-2', 1)

        res = self.lua('resource.get', 0, 'r-1')
        self.assertEqual(res['locks'], ['jid-2'])
        self.assertEqual(res['pending'], {})

    def test_error_if_attempts_to_acquire_nonexistent_resource(self):
        """Job requires invalid resource, generates error"""

        self.assertRaisesRegexp(redis.ResponseError, r'resource r-1 does not exist',
            self.lua, 'put', 0, None, 'queue', 'jid-1', 'klass', {}, 0, 'retries', 0, 'resources', ['r-1'])

