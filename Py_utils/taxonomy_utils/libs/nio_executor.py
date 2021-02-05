import boto3
import asyncio
import concurrent.futures
import logging
import sys
import time
from libs.log_decorator import log_decorate

class NIO:

    @classmethod
    def sample_task(cls, *args):
        log = logging.getLogger('(sample_task)')
        log.info("under task")

        return args


    @classmethod
    def sample_task2(cls, *args):
        log = logging.getLogger('(sample_task2)')
        log.info("under task2")

        return args


    @classmethod
    def sample_task3(cls, arg):
        log = logging.getLogger('(sample_task3)')
        log.info("under task3")
    
        return arg


    @classmethod
    def sample_generator(cls):
        for i in range(5):
            yield [i]


    @classmethod
    def custom_task(arg1, arg2, arg3, arg4=-1):
        log = logging.getLogger('(custom_task)')
        log.info("under custom task")

        return str(arg1)+str(arg2)+str(arg3)+str(arg4)


    @classmethod
    def task_wrapper(cls, task, task_num:int, args):
        log = logging.getLogger('({})'.format(task_num))
        log.info('passed args :'+ str(args))
        res = None
        ex = None
        try:
            log.info('running')
#             log.debug('==>'+str(task))
#             log.debug('==>'+str(*args))
            if hasattr(args, '__iter__'):
                res = task(*args)
            else:
                res = task(args)
            log.info('done')
        except Exception as e:
            ex = e
            logging.error(e, exc_info=True)
            log.info('failed')

        return {'id': task_num,'task' : task, 'args': args, 'result': res, 'error': ex} #task_num, res, ex


    @classmethod
    async def run_blocking_tasks(cls, executor, func, **kwargs):
        log = logging.getLogger('run_blocking_tasks')
        log.info('starting')

        log.info('creating executor tasks')
        loop = asyncio.get_event_loop()
    #     blocking_tasks = [
    #         loop.run_in_executor(executor, blocks, i)
    #         for i in range(6)
    #     ]
        blocking_tasks = [
            loop.run_in_executor(executor, cls.task_wrapper, func, key, val)
            for key, val in kwargs.items()
        ]
        log.info('waiting for executor tasks')
        completed, pending = await asyncio.wait(blocking_tasks)
        results = [t.result() for t in completed]
        log.debug('results: {!r}'.format(results))

        log.info('exiting')
        return results

    '''
    NIO.execute_io(func=sample_task, task_1=['abc', 'l'], task_2=['cde', 'n'])

    or 

    tasks_dict = {'task_1':['k', 'l'], 'task_2':['m', 'n']}
    NIO.execute_io(func=sample_task, **tasks_dict)
    '''
    @classmethod
    def execute_io(cls, func=None,  max_workers=10, is_kernal_thread: bool=False, **kwargs):
        
        log = logging.getLogger('execute_io')

        if is_kernal_thread:
            # Create a limited process pool.
            executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers,
            )
        else:
            # Create a limited thread pool.
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers,
            )

        #event_loop = asyncio.get_event_loop()
        event_loop = asyncio.new_event_loop()
        result = None
        try:
            asyncio.set_event_loop(event_loop)
            result = event_loop.run_until_complete(
                cls.run_blocking_tasks(executor,func, **kwargs)
            )
        finally:
            try:
                event_loop.run_until_complete(event_loop.shutdown_asyncgens())
                # asyncio.set_event_loop(None)
                # event_loop.close()
            finally:
                asyncio.set_event_loop(None)
                event_loop.close()
                return result


    '''
    
    NIO.run_io(task=sample_task, ['abc', 'l'], ['cde', 'n'])
    Note: Not supported now
    or

    NIO.run_io(task=sample_task, [['abc', 'l'], ['cde', 'n']])

    or 

    tasks_args_list =  [['abc', 'l'], ['cde', 'n']]
    NIO.run_io(task=sample_task, tasks_args_list)
    '''
    @classmethod
    def run_io(cls, task=None,  max_workers=10, is_kernal_thread: bool=False, task_n_args_list=None):
        
        task_n_args_dict:dict = {}
        [task_n_args_dict.update({'task-{}'.format(str(i)):value}) for i, value in enumerate(task_n_args_list)]
        return cls.execute_io(func=task, max_workers=max_workers, is_kernal_thread=is_kernal_thread, **task_n_args_dict)


    '''
    tasks_args_list =  [['abc', 'l'], ['cde', 'n']]
    NIO.decorated_run_io(task=NIO.sample_task, task_args_list=tasks_args_list)
    '''
    @classmethod
    def decorated_run_io(cls, task=None,  max_workers=10, is_kernal_thread: bool=False, task_n_args_list=None):
        if is_kernal_thread:
            return log_decorate(cls.run_io, task=task, max_workers=max_workers, 
                         is_kernal_thread=is_kernal_thread,task_n_args_list=task_n_args_list, 
                         formattor='%(asctime)3s   process-id:%(process)s %(name)10s: %(message)s\n')
        else:
            return log_decorate(cls.run_io, task=task, max_workers=max_workers, 
                         is_kernal_thread=is_kernal_thread,task_n_args_list=task_n_args_list)

    

    @classmethod
    async def execute_blocking_tasklist(cls, executor, task_n_args_list=None):
        log = logging.getLogger('execute_blocking_tasklist')
        log.info('starting')

        log.info('creating executor tasks')
        loop = asyncio.get_event_loop()

        blocking_tasks = [
            loop.run_in_executor(executor, cls.task_wrapper, value[0], 'task-{}'.format(str(i)), value[1:])
            for i, value in enumerate(task_n_args_list)
        ]
        log.info('waiting for executor tasks')
        completed, pending = await asyncio.wait(blocking_tasks)
        results = [t.result() for t in completed]
        log.debug('results: {!r}'.format(results))

        log.info('exiting')
        return results

    
    '''
    task_n_args_list =  [[NIO.sample_task, 'abc', 'l'], [NIO.sample_task2, 'cde', 'n']]
    NIO.execute_tasklist(task_n_args_list=task_n_args_list)
    '''
    @classmethod
    def execute_tasklist(cls, max_workers=10, is_kernal_thread: bool=False, task_n_args_list=None):
        
        if is_kernal_thread:
            # Create a limited process pool.
            executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers,
            )
        else:
            # Create a limited thread pool.
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers,
            )
        
#         try:
#             event_loop = asyncio.get_event_loop()
#         except:
#             event_loop =None
            
#         if event_loop is None:
        event_loop = asyncio.new_event_loop()

        result = None
        try:
            asyncio.set_event_loop(event_loop)
            result = event_loop.run_until_complete(
                cls.execute_blocking_tasklist(executor, task_n_args_list)
            )
        finally:
            try:
                event_loop.run_until_complete(event_loop.shutdown_asyncgens())
                # asyncio.set_event_loop(None)
                # event_loop.close()
            finally:
                asyncio.set_event_loop(None)
                event_loop.close()
                return result
                
    
    '''
    task_n_args_list =  [[NIO.sample_task, 'abc', 'l'], [NIO.sample_task2, 'cde', 'n']]
    NIO.decorated_execute_tasklist(task_n_args_list=task_n_args_list)
    '''
    @classmethod
    def decorated_execute_tasklist(cls,   max_workers=10, is_kernal_thread: bool=False, task_n_args_list=None):
        if is_kernal_thread:
            return log_decorate(cls.execute_tasklist, 
                         formattor='%(asctime)3s   process-id:%(process)s %(name)10s: %(message)s\n', 
                         max_workers=max_workers, is_kernal_thread=is_kernal_thread,task_n_args_list=task_n_args_list, 
                         )
        else:
            return log_decorate(cls.execute_tasklist, max_workers=max_workers, 
                     is_kernal_thread=is_kernal_thread,task_n_args_list=task_n_args_list)


    '''
    type of inputs #1:
    
    tasks_n_args_list1=[
                        [NIO.sample_task, 'abc', 'l'], 
                        [NIO.sample_task2, 'cde', 'n'], 
                       ]
    tasks_n_args_list2=[
                        [NIO.custom_task, '0', '0', '1'],
                        [NIO.custom_task, '0', '1', '0'],
                        [NIO.custom_task, '0', '1', '1'],
                        [NIO.custom_task, '1', '0', '0'],
                        [NIO.custom_task, '1', '0', '1'],
                        [NIO.custom_task, '1', '1', '0'],
                        [NIO.custom_task, '1', '1', '1'],
                       ]
    tasks_n_args_list3=[
                        [NIO.custom_task, '1', '0', '0', '0'],
                        [NIO.custom_task, '1', '0', '0', '1'],
                        [NIO.custom_task, '1', '0', '1', '0'],
                        [NIO.custom_task, '1', '0', '1', '1'],
                        [NIO.custom_task, '1', '1', '0', '0'],
                        [NIO.custom_task, '1', '1', '0', '1'],
                        [NIO.custom_task, '1', '1', '1', '0'],
                        [NIO.custom_task, '1', '1', '1', '1'],
                       ]
    
    tasks_n_args_list= [ 
                        [tasks_n_args_list1],
                        [tasks_n_args_list2],
                        [tasks_n_args_list3]
                       ]
    
    Note: here, all tasks which is provided by tasks_n_args_list1, tasks_n_args_list2 and tasks_n_args_list3 
          will be executed by thead pools respectively. each thread pool will be ececuted by own process from process pool.
          In this example, three processes are executing three thread pools.
          
          process1: thread pool1: 2 tasks
          process2: thread pool1: 7 tasks
          process3: thread pool1: 8 tasks
    or
    
    tasks_n_args_list= [ 
                               [
                                   [
                                    [NIO.sample_task, 'abc', 'l'], 
                                    [NIO.sample_task2, 'cde', 'n'], 
                                   ],
                               ],
                               [ 
                                   [
                                    [NIO.custom_task, '1', '0', '0', '0'],
                                    [NIO.custom_task, '1', '0', '0', '1'],
                                    [NIO.custom_task, '1', '0', '1', '0'],
                                    [NIO.custom_task, '1', '0', '1', '1'],
                                    [NIO.custom_task, '1', '1', '0', '0'],
                                    [NIO.custom_task, '1', '1', '0', '1'],
                                    [NIO.custom_task, '1', '1', '1', '0'],
                                    [NIO.custom_task, '1', '1', '1', '1'],
                                   ],
                               ],
                               [
                                   [
                                    [NIO.custom_task, '1', '0', '0', '0'],
                                    [NIO.custom_task, '1', '0', '0', '1'],
                                    [NIO.custom_task, '1', '0', '1', '0'],
                                    [NIO.custom_task, '1', '0', '1', '1'],
                                    [NIO.custom_task, '1', '1', '0', '0'],
                                    [NIO.custom_task, '1', '1', '0', '1'],
                                    [NIO.custom_task, '1', '1', '1', '0'],
                                    [NIO.custom_task, '1', '1', '1', '1'],
                                   ],
                               ],
                       ]
    
    
    
    NIO.run_using_process_of_threads(tasks_n_args_list=tasks_n_args_list)
    
    
    
    Max thread workers:
                        tasks_n_args_list=    [ 
                                                [tasks_n_args_list1, 5],
                                                [tasks_n_args_list2, 12],
                                                [tasks_n_args_list3, 20]
                                               ]
                        Here, 5, 12 and 20 are optional values of max_thread_workers for respective process.
                        By default, It is 10
    
    
    Max Process workers:
                    
        NIO.run_using_process_of_threads(tasks_n_args_list=tasks_n_args_list, max_proc_workers=5)
    
    
    Note: max_proc_workers=10, max_thread_workers=10, will be introduced later.
    '''
    @classmethod
    def run_using_process_of_threads(cls, tasks_n_args_list, max_proc_workers=10):
        return cls.decorated_run_io(task=cls.__execute_thread_pool_inernal__, 
                             task_n_args_list=tasks_n_args_list, max_workers=max_proc_workers, is_kernal_thread=True,)


    @classmethod
    def __execute_thread_pool_inernal__(cls, tasks_n_args_list, max_thread_workers=10):
        log = logging.getLogger('__execute_thread_pool_inernal__')
        ret = cls.decorated_execute_tasklist(task_n_args_list=tasks_n_args_list, max_workers=max_thread_workers)
        [log.info('{} tasks are completed. \n'.format(str(i))) for i in tasks_n_args_list]
        return ret 


    @classmethod
    async def run_blocking_tasks_with_args_generator(cls, executor, func, generator, task_prefix = 'task-'):
        log = logging.getLogger('run_blocking_tasks')
        log.info('starting')

        log.info('creating executor tasks')
        loop = asyncio.get_event_loop()
        blocking_tasks = [
            loop.run_in_executor(executor, cls.task_wrapper, func, '{}{}'.format(task_prefix, str(key)), val)
            for key, val in enumerate(generator)
        ]
        log.info('waiting for executor tasks')
        completed, pending = await asyncio.wait(blocking_tasks)
        results = [t.result() for t in completed]
        log.debug('results: {!r}'.format(results))

        log.info('exiting')
        return results


    '''
    def sample_generator():
        for i in range(15):
            yield i


    NIO.run_with_args_generator(task=NIO.sample_task3, args_generator=NIO.sample_generator())
    '''
    @classmethod
    def run_with_args_generator(cls, task=None, args_generator=None, max_workers=10, 
                                is_kernal_thread: bool=False, task_prefix = 'task-'):

        if is_kernal_thread:
            # Create a limited process pool.
            executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers,
            )
        else:
            # Create a limited thread pool.
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers,
            )

        #event_loop = asyncio.get_event_loop()
        event_loop = asyncio.new_event_loop()

        result = None
        try:
            asyncio.set_event_loop(event_loop)
            result = event_loop.run_until_complete(
                cls.run_blocking_tasks_with_args_generator(executor,task, args_generator, task_prefix=task_prefix)
            )
        finally:
            try:
                event_loop.run_until_complete(event_loop.shutdown_asyncgens())
                # asyncio.set_event_loop(None)
                # event_loop.close()
            finally:
                asyncio.set_event_loop(None)
                event_loop.close()
                return result


    '''
    def sample_generator():
        for i in range(15):
            yield i


    NIO.decorated_run_with_args_generator(task=NIO.sample_task3, args_generator=NIO.sample_generator())
    '''
    @classmethod
    def decorated_run_with_args_generator(cls, task=None,  args_generator=None, 
                                          max_workers=10, is_kernal_thread: bool=False):
        if is_kernal_thread:
            return log_decorate(cls.run_with_args_generator,  task=task, args_generator=args_generator,
                         max_workers=max_workers, is_kernal_thread=is_kernal_thread, 
                         formattor='%(asctime)3s   process-id:%(process)s %(name)10s: %(message)s\n')
        else:
            return log_decorate(cls.run_with_args_generator, task=task, args_generator=args_generator, 
                         max_workers=max_workers, is_kernal_thread=is_kernal_thread)
