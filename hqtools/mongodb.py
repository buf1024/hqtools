from functools import wraps
from collections import namedtuple
from typing import List, Optional, Dict
import motor.motor_asyncio
from pymongo.errors import ServerSelectionTimeoutError, AutoReconnect
import time
import traceback
import pandas as pd
import hqtools.log as log
import asyncio
from functools import partial

import nest_asyncio
nest_asyncio.apply()


class MongoDB():
    _MongoStat = namedtuple('_MongoStat', ['client', 'count', 'last'])

    def __init__(self, *, meta: Dict = dict(),  uri: str = 'mongodb://localhost:27017/', db: str = 'winq',  pool: int = 5):
        """MongoDB访问类，可以继承方便使用，不想继承时，创数据库的meta即可。

        meta的格式为，比如有一张表 abc: {a: 1, b: '1'}, 则meta={'abc': {'a': '字段a的含义', 'b': '字段b的含义'}}

        则可以通过self.abc访问coll，self.load_abc self.save_abc等形式方便数据库访问，表比较多时比较方便。

        Args:
            meta (dict, optional): 见上 默认 {}.
            uri (str, optional): mongodb 链接地址 默认 'mongodb://localhost:27017/'.
            db (str, optional):  mongodb 数据库 默认 'winq'.
            pool (int, optional): mongodb 链接池数量 默认  1.
        """

        self.log = log.get_logger(self.__class__.__name__)
        self.clients = []

        self.uri = uri
        self.pool = pool

        self.db = db
        self.meta = meta

        self.is_init = False

    def _best_client(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.clients = sorted(
                self.clients, key=lambda stat: (stat.count, -stat.last))
            stat_client = self.clients[0]
            self.clients[0] = stat_client._replace(
                count=stat_client.count + 1, last=time.time())

            kwargs['__client'] = stat_client.client
            return func(self, *args, **kwargs)

        return wrapper

    def __getattr__(self, attr: str):
        attr = attr.lower()
        tab, func = None, None
        if attr.startswith('load_'):
            tab, func = attr[len('load_'):], self._base_load
        elif attr.startswith('save_'):
            tab, func = attr[len('save_'):], self._base_save
        else:
            if attr not in self.meta.keys():
                raise Exception('Invalid property')

            return self.get_coll(self.db, attr)

        if tab is None or tab not in self.meta.keys():
            raise Exception('Invalid mongodb function!')

        coll = self.get_coll(self.db, tab)

        return partial(func, attr=attr, coll=coll)

    async def _base_load(self, *, attr, coll, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame
        """
        self.log.debug('加载: {}, kwargs={} ...'.format(attr, kwargs))
        df = await self.do_load(coll, **kwargs)
        self.log.debug('加载: {} 成功 size={}'.format(
            attr, df.shape[0] if df is not None else 0))
        return df

    async def _base_save(self, *, attr, coll, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame 同tab
        :return: list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存: {}, count = {} ...'.format(attr, count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=coll, data=data)
        self.log.debug('保存: {} 成功, size = {}'.format(
            attr, len(inserted_ids) if inserted_ids is not None else 0))

        return inserted_ids

    def init(self) -> bool:
        if self.is_init:
            return True

        if len(self.meta) == 0 or len(self.db) == 0:
            return False
        try:
            for _ in range(self.pool):
                client = motor.motor_asyncio.AsyncIOMotorClient(self.uri)
                self.clients.append(self._MongoStat(
                    client=client, count=0, last=time.time()))

            test_coll = self.get_coll(self.db, list(self.meta.keys())[0])
            if test_coll is not None:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(test_coll.count_documents({}))
        except Exception as e:
            self.log.error(type(e))
            raise e

        self.is_init = True
        return self.is_init

    @_best_client
    def get_client(self, **kwargs):
        return kwargs['__client'] if '__client' in kwargs else None

    def get_coll(self, db: str, col: str):
        client = self.get_client()
        if client is None:
            return None
        return client[db][col]

    async def do_load(self, coll, filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True):
        for i in range(5):
            try:
                cursor = coll.find(
                    filter=filter, projection=projection, skip=skip, limit=limit, sort=sort)
                if cursor is not None:
                    # data = [await item async for item in cursor]
                    data = await cursor.to_list(None)
                    await cursor.close()
                    if to_frame:
                        df = pd.DataFrame(data=data, columns=projection)
                        if not df.empty:
                            if '_id' in df.columns:
                                df.drop(columns=['_id'], inplace=True)
                            return df
                    else:
                        if len(data) > 0:
                            for item in data:
                                del item['_id']
                        return data
                break
            except (ServerSelectionTimeoutError, AutoReconnect) as e:
                self.log.error('mongodb 调用 {}, 连接异常: ex={}, call {}, {}s后重试'.format(
                    self.do_load.__name__, e, traceback.format_exc(), (i + 1) * 5))
                await asyncio.sleep((i + 1) * 5)
                self.init()
        return None

    async def do_update(self, coll, filter=None, update=None, upsert=True):
        for i in range(5):
            try:
                if update is None:
                    return None
                res = await coll.update_one(filter, {'$set': update}, upsert=upsert)
                # return res.upserted_id
                return res.matched_count if res.matched_count > 0 else (
                    res.upserted_id if res.upserted_id is not None else 0)
            except (ServerSelectionTimeoutError, AutoReconnect) as e:
                self.log.error('mongodb 调用 {}, 连接异常: ex={}, call {}, {}s后重试'.format(
                    self.do_update.__name__, e, traceback.format_exc(), (i + 1) * 5))
                await asyncio.sleep((i + 1) * 5)
                self.init()
        return 0

    async def do_update_many(self, coll, filter=None, update=None, upsert=True):
        for i in range(5):
            try:
                if update is None:
                    return None
                res = await coll.update_many(filter, {'$set': update}, upsert=upsert)
                # return res.upserted_id
                return res.matched_count if res.matched_count > 0 else (
                    res.upserted_id if res.upserted_id is not None else 0)
            except (ServerSelectionTimeoutError, AutoReconnect) as e:
                self.log.error('mongodb 调用 {}, 连接异常: ex={}, call {}, {}s后重试'.format(
                    self.do_update.__name__, e, traceback.format_exc(), (i + 1) * 5))
                await asyncio.sleep((i + 1) * 5)
                self.init()
        return 0

    async def do_batch_update(self, data, func):
        upsert_list = []
        for item in data.to_dict('records'):
            coll, filter, update = func(item)
            upsert = await self.do_update(coll, filter=filter, update=update)
            if upsert is None:
                continue
            if isinstance(upsert, list):
                upsert_list = upsert_list + upsert
            else:
                upsert_list.append(upsert)
        return upsert_list if len(upsert_list) > 0 else None

    async def do_delete(self, coll, filter=None, just_one=True):
        for i in range(5):
            try:
                res = None
                if just_one:
                    res = await coll.delete_one(filter)
                else:
                    if filter is not None:
                        res = await coll.delete_many(filter)
                    else:
                        res = await coll.drop()
                return 0 if res is None else res.deleted_count
            except (ServerSelectionTimeoutError, AutoReconnect) as e:
                self.log.error('mongodb 调用 {}, 连接异常: ex={}, call {}, {}s后重试').format(
                    self.do_delete.__name__, e, traceback.format_exc(), (i + 1) * 5)
                await asyncio.sleep((i + 1) * 5)
                self.init()
        return 0

    async def do_insert(self, coll, data):
        for i in range(5):
            try:
                inserted_ids = []
                if data is not None and not data.empty:
                    docs = data.to_dict('records')
                    result = await coll.insert_many(docs)
                    inserted_ids = result.inserted_ids
                return inserted_ids
            except (ServerSelectionTimeoutError, AutoReconnect) as e:
                self.log.error('mongodb 调用 {}, 连接异常: ex={}, call {}, {}s后重试').format(
                    self.do_insert.__name__, e, traceback.format_exc(), (i + 1) * 5)
                await asyncio.sleep((i + 1) * 5)
                self.init()
        return 0
