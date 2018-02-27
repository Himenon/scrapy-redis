from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider

# redisの接続と、デフォルトの設定値
from . import connection, defaults
# 便利メソッド
from .utils import bytes_to_str


class RedisMixin(object):
    """Mixin class to implement reading urls from a redis queue."""
    redis_key = None
    redis_batch_size = None
    redis_encoding = None

    # Redis client placeholder.
    server = None

    def start_requests(self):
        # 1 リクエストスタート
        """Returns a batch of start requests from redis."""
        return self.next_requests()

    def setup_redis(self, crawler=None):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.server is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        # settingsの読み込み
        settings = crawler.settings

        if self.redis_key is None:
            # PREFIXをつけるようにできたりする
            # キーがデフォルトだと start_urls をそれにspider名を追記できる
            self.redis_key = settings.get(
                'REDIS_START_URLS_KEY', defaults.START_URLS_KEY,
            )

        # 注入
        self.redis_key = self.redis_key % {'name': self.name}

        # emptyじゃだめ！
        if not self.redis_key.strip():
            raise ValueError("redis_key must not be empty")

        # batch size = concurrent size ?
        if self.redis_batch_size is None:
            # TODO: Deprecate this setting (REDIS_START_URLS_BATCH_SIZE).
            self.redis_batch_size = settings.getint(
                'REDIS_START_URLS_BATCH_SIZE',
                settings.getint('CONCURRENT_REQUESTS'),
            )

        try:
            self.redis_batch_size = int(self.redis_batch_size)
        except (TypeError, ValueError):
            raise ValueError("redis_batch_size must be an integer")

        # encodingはデフォ
        if self.redis_encoding is None:
            self.redis_encoding = settings.get('REDIS_ENCODING', defaults.REDIS_ENCODING)

        # ログ出力
        self.logger.info("Reading start URLs from redis key '%(redis_key)s' "
                         "(batch size: %(redis_batch_size)s, encoding: %(redis_encoding)s",
                         self.__dict__)

        # ここでredisとconnectionをしている
        self.server = connection.from_settings(crawler.settings)
        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue

        # アイドルシグナルは、Spiderリクエストが残っていない時に呼び出される
        # redis queueから新しいリクエストをスケジュールする
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        """Returns a request to be scheduled or none."""
        # settingsはMixinされるCrawlSpiderから来る
        use_set = self.settings.getbool('REDIS_START_URLS_AS_SET', defaults.START_URLS_AS_SET)
        # SET型のPOPか、LISTのPOPかを選べる
        fetch_one = self.server.spop if use_set else self.server.lpop
        # XXX: Do we need to use a timeout here?
        found = 0
        # pipeline実行にすることで、排他的になる
        # TODO: Use redis pipeline execution.
        # ここで、redisに問合せをこおなっている
        # キューがあった場合は、yieldで返すが、
        # 無かった場合はそのままループする

        # バッチサイズ？?????
        while found < self.redis_batch_size:
            # 一つの値を取得
            data = fetch_one(self.redis_key)
            if not data:
                # Queue empty.
                break

            # リクエストの取得
            req = self.make_request_from_data(data)
            if req:
                # リクエストが存在していればyieldで次の処理
                yield req
                # カウンタをincrement
                found += 1
            else:
                # もしリクエストがなければログを吐き出す
                self.logger.debug("Request not made from data: %r", data)

        # カウンタ > 1であれば、keyを読み込んだことを通知する
        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.redis_key)

    def make_request_from_data(self, data):
        """Returns a Request instance from data coming from Redis.

        By default, ``data`` is an encoded URL. You can override this method to
        provide your own message decoding.

        Parameters
        ----------
        data : bytes
            Message from redis.

        """
        # バイト型 b'' でくるので、変換
        url = bytes_to_str(data, self.redis_encoding)
        # Mixinされるメソッド
        return self.make_requests_from_url(url)

    def schedule_next_requests(self):
        """Schedules a request if available"""
        # TODO: While there is capacity, schedule a batch of redis requests.
        # Next Requestが存在すれば、Crawlを実行する
        for req in self.next_requests():
            # あった場合に実行
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        # アイドル状態にする
        """Schedules a request if available, otherwise waits."""
        # XXX: Handle a sentinel to close the spider.
        self.schedule_next_requests()
        # アイドル状態にずっとする
        raise DontCloseSpider


class RedisSpider(RedisMixin, Spider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: False)
        Use SET operations to retrieve messages from the redis queue. If False,
        the messages are retrieve using the LPOP command.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RedisSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj


class RedisCrawlSpider(RedisMixin, CrawlSpider):
    """Spider that reads urls from redis queue when idle.

    Attributes
    ----------
    redis_key : str (default: REDIS_START_URLS_KEY)
        Redis key where to fetch start URLs from..
    redis_batch_size : int (default: CONCURRENT_REQUESTS)
        Number of messages to fetch from redis on each attempt.
    redis_encoding : str (default: REDIS_ENCODING)
        Encoding to use when decoding messages from redis queue.

    Settings
    --------
    REDIS_START_URLS_KEY : str (default: "<spider.name>:start_urls")
        Default Redis key where to fetch start URLs from..
    REDIS_START_URLS_BATCH_SIZE : int (deprecated by CONCURRENT_REQUESTS)
        Default number of messages to fetch from redis on each attempt.
    REDIS_START_URLS_AS_SET : bool (default: True)
        Use SET operations to retrieve messages from the redis queue.
    REDIS_ENCODING : str (default: "utf-8")
        Default encoding to use when decoding messages from redis queue.

    """

    # Crawlerを作成するためのメソッド
    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        # RedisCrawlから派生
        obj = super(RedisCrawlSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj
