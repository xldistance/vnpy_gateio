import csv
import hashlib
import hmac
import json
import sys
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from time import sleep, time
from typing import Dict, List,Union
from urllib.parse import urlencode

from peewee import chunked
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway, LocalOrderManager
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import gateio_account
from vnpy.trader.utility import (
    TZ_INFO,
    GetFilePath,
    delete_dr_data,
    extract_vt_symbol,
    get_folder_path,
    get_local_datetime,
    get_symbol_mark,
    is_target_contract,
    load_json,
    remain_digit,
    save_connection_status,
    save_json,
)

TESTNET_REST_HOST = "https://fx-api-testnet.gateio.ws"
REST_HOST = "https://fx-api.gateio.ws"

TESTNET_WEBSOCKET_HOST = "wss://fx-ws-testnet.gateio.ws/v4/ws"
WEBSOCKET_HOST = "wss://fx-ws.gateio.ws/v4/ws/btc"  # 币本位永续ws_host

INTERVAL_VT2GATEIO = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}


# ----------------------------------------------------------------------------------------------------
class GateiosGateway(BaseGateway):
    """
    * Gateio币本位永续合约
    * 仅支持单向持仓模式,下单合约数量张数(int)
    """

    default_setting = {
        "key": "",
        "secret": "",
        "server": ["REAL", "TESTNET"],
        "host": "",
        "port": "",
    }
    # 所有合约列表
    recording_list = GetFilePath.recording_list
    exchanges = [Exchange.GATEIO]
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine):
        """ """
        super().__init__(event_engine, "GATEIOS")
        self.ws_api: GateiosWebsocketApi = GateiosWebsocketApi(self)
        self.rest_api: GateiosRestApi = GateiosRestApi(self)
        self.query_count = 0
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        self.query_func = [self.query_account, self.query_position, self.query_order]
        # 下载历史数据状态
        self.history_status: bool = True
    # ----------------------------------------------------------------------------------------------------
    def connect(self, log_account: Dict):
        """ """
        if not log_account:
            log_account = gateio_account
        key = log_account["key"]
        secret = log_account["secret"]
        server = log_account["server"]
        proxy_host = log_account["host"]
        proxy_port = log_account["port"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)

        self.init_query()
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest):
        """ """
        self.ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest):
        """ """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest):
        """ """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self):
        """ """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_position(self):
        """ """
        self.rest_api.query_position()
    # ----------------------------------------------------------------------------------------------------
    def query_order(self):
        """ """
        self.rest_api.query_order()
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, event):
        """
        查询合约历史数据
        """
        # 等待restapi合约数据推送完成后再查询历史数据
        if not self.rest_api.contract_inited:
            return
        if len(self.history_contracts) > 0:
            symbol, exchange, gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol=symbol,
                exchange=Exchange(exchange),
                interval=Interval.MINUTE,
                start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
                end=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.rest_api.query_history(req)
            self.rest_api.set_leverage(symbol)
    # ----------------------------------------------------------------------------------------------------
    def close(self):
        """ """
        self.rest_api.stop()
        self.ws_api.stop()
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event: Event):
        """
        轮询账户，持仓，未完成委托单函数
        """
        """
        self.query_count += 1
        if self.query_count < 3:
            return
        self.query_count = 0
        """
        func = self.query_func.pop(0)
        func()
        self.query_func.append(func)
    # ----------------------------------------------------------------------------------------------------
    def init_query(self):
        """ """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
# ----------------------------------------------------------------------------------------------------
class GateiosRestApi(RestClient):
    """
    Gateios REST API
    """

    def __init__(self, gateway: GateiosGateway):
        """ """
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.ws_api = gateway.ws_api

        self.key = ""
        self.secret = ""
        self.account_id = ""
        self.server = ""
        self.proxy_host = ""
        self.proxy_port = 0

        # 生成委托单号加线程锁
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
        # 用户自定义委托单id和交易所委托单id映射
        self.orderid_map: Dict[str, str] = defaultdict(str)

        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}
        self.contract_inited: bool = False
        self.position_pnl: Dict[str, float] = {}
    # ----------------------------------------------------------------------------------------------------
    def sign(self, request):
        """
        Generate signature.
        """
        request.headers = generate_sign(self.key, self.secret, request.method, request.path, get_params=request.params, get_data=request.data)

        if not request.data:
            request.data = ""

        return request
    # ----------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, server: str, proxy_host: str, proxy_port: int):
        """
        初始化连接REST
        """
        self.key = key
        self.secret = secret
        self.server = server
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))
        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)

        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}REST API启动成功")
    # ----------------------------------------------------------------------------------------------------
    def set_leverage(self, symbol: str):
        """
        设置单向全仓杠杆
        """
        params = {"leverage": "0", "cross_leverage_limit": "20"}
        self.add_request(method="POST", path=f"/api/v4/futures/btc/positions/{symbol}/leverage", callback=self.on_leverage, params=params)
    # ----------------------------------------------------------------------------------------------------
    def on_leverage(self, data: dict, request: Request):
        """
        收到修改杠杆回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_account(self):
        """ """
        self.add_request(method="GET", path="/api/v4/futures/btc/accounts", callback=self.on_query_account)
    # ----------------------------------------------------------------------------------------------------
    def query_position(self):
        """ """
        self.add_request(method="GET", path="/api/v4/futures/btc/positions", callback=self.on_query_position)
    # ----------------------------------------------------------------------------------------------------
    def query_order(self):
        """ """
        for vt_symbol in self.gateway.recording_list:
            symbol, exchange, gateway_name = extract_vt_symbol(vt_symbol)
            params = {
                "contract": symbol,
                "status": "open",
            }

            self.add_request(method="GET", path="/api/v4/futures/btc/orders", callback=self.on_query_order, on_failed=self.query_order_failed, params=params)
    # ----------------------------------------------------------------------------------------------------
    def query_contract(self):
        """ """
        self.add_request(method="GET", path=f"/api/v4/futures/btc/contracts", callback=self.on_query_contract)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest):
        """ """
        history = []
        interval = INTERVAL_VT2GATEIO[req.interval]
        time_consuming_start = time()
        start_time = req.start
        limit = 100
        while True:
            end_time = start_time + timedelta(minutes=limit)
            params = {
                "contract": req.symbol,
                "from": int(start_time.timestamp()),
                "to":int(end_time.timestamp()),
                "interval": interval,
            }
            resp = self.request(method="GET", path="/api/v4/futures/btc/candlesticks", params=params)
            if resp.status_code // 100 != 2:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    msg = f"标的：{req.vt_symbol}获取历史数据为空"
                    self.gateway.write_log(msg)
                    break
                buf = []
                for raw in data:
                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=get_local_datetime(raw["t"]),
                        interval=req.interval,
                        volume=raw["v"],
                        open_price=float(raw["o"]),
                        high_price=float(raw["h"]),
                        low_price=float(raw["l"]),
                        close_price=float(raw["c"]),
                        gateway_name=self.gateway_name,
                    )
                    # 过滤交易所返回大于等于当前时间的错误bar
                    if bar.datetime >= datetime.now(TZ_INFO):
                        continue
                    buf.append(bar)
                start_time = bar.datetime + timedelta(minutes=1)
                history.extend(buf)
                # 已经获取了所有可用的历史数据或者start_time已经到了请求的终止时间则终止循环
                if len(buf) < limit or start_time >= req.end:
                    break
        if not history:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
            return
        for bar_data in chunked(history, 10000):  # 分批保存数据
            try:
                database_manager.save_bar_data(bar_data, False)  # 保存数据到数据库
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return
        time_consuming_end = time()
        query_time = round(time_consuming_end - time_consuming_start, 3)
        msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime} ，结束时间： {history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def query_order_failed(self, status_code: int, request: Request) -> None:
        """
        查询未成交委托单错误回调
        """
        # 过滤系统错误
        error = request.response.json().get("label", None)
        if error == "SERVER_ERROR":
            return
        self.gateway.write_log(f"错误代码：{status_code}，错误请求：{request.path}，完整请求：{request}")
    # ----------------------------------------------------------------------------------------------------
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest):
        """ """
        # 生成本地委托号
        orderid: str = req.symbol + "-" + str(self.connect_time + self._new_order_id())

        order = req.create_order_data(orderid, self.gateway_name)
        order.datetime = datetime.now(TZ_INFO)

        if req.direction == Direction.SHORT:
            volume = -int(req.volume)
        else:
            volume = int(req.volume)

        request_body = {"contract": req.symbol, "size": volume, "price": str(req.price), "tif": "gtc", "text": f"t-{orderid}"}
        if req.offset == Offset.CLOSE:
            request_body["reduce_only"] = True

        data = json.dumps(request_body)

        self.add_request(
            method="POST",
            path="/api/v4/futures/btc/orders",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )
        self.gateway.on_order(order)
        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest):
        """ """
        gateway_id = self.orderid_map[req.orderid]
        if not gateway_id:
            if self.orderid_map:
                local_id = list(self.orderid_map)[0]
                gateway_id = self.orderid_map[local_id]
                self.orderid_map.pop(local_id)
                self.gateway.write_log(f"合约：{req.vt_symbol}未获取到委托单id映射：自定义委托单id：{req.orderid}，使用交易所orderid：{gateway_id}撤单")

        self.add_request(
            method="DELETE", path=f"/api/v4/futures/btc/orders/{gateway_id}", callback=self.on_cancel_order, on_failed=self.on_cancel_order_failed, extra=req
        )
    # ----------------------------------------------------------------------------------------------------
    def on_query_account(self, data, request):
        """ """
        self.account_id = str(data["user"])

        account = AccountData(
            accountid=f"BTC_{self.gateway_name}",
            balance=float(data["total"]),
            frozen=float(data["total"]) - float(data["available"]),
            position_profit=float(data["unrealised_pnl"]),
            margin=float(data["order_margin"]),
            datetime=datetime.now(TZ_INFO),
            file_name=self.gateway.account_file_name,
            gateway_name=self.gateway_name,
        )
        # 用accounts_info过滤只查询一次合约信息
        if not self.accounts_info:
            self.query_contract()

        if account.balance:
            self.gateway.on_account(account)
            # 保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__

        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = str(GetFilePath.ctp_account_path).replace("ctp_account_main", self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)
    # ----------------------------------------------------------------------------------------------------
    def on_query_position(self, data, request):
        """ """
        for raw in data:
            volume = float(raw["size"])
            if volume >= 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            pos_1 = PositionData(
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                volume=abs(volume),
                direction=direction,
                price=float(raw["entry_price"]),
                pnl=float(raw["unrealised_pnl"]),
                gateway_name=self.gateway_name,
            )
            pos_2 = PositionData(
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[direction],
                volume=0,
                price=0,
                pnl=0,  # 持仓盈亏
                frozen=0,  # 持仓冻结保证金
            )
            pos_1_direction = pos_1.vt_symbol + pos_1.direction.value
            self.position_pnl[pos_1_direction] = pos_1.pnl
            self.gateway.on_position(pos_1)
            self.gateway.on_position(pos_2)
    # ----------------------------------------------------------------------------------------------------
    def on_query_order(self, data, request):
        """ """
        for raw in data:
            local_orderid = str(raw["text"])[2:]
            gateway_orderid = str(raw["id"])
            self.orderid_map[local_orderid] = gateway_orderid

            volume = abs(raw["size"])
            traded = abs(raw["size"] - raw["left"])
            status = get_order_status(raw["status"], volume, traded)
            if raw["size"] > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            order = OrderData(
                orderid=local_orderid,
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                price=float(raw["price"]),
                volume=volume,
                direction=direction,
                status=status,
                datetime=get_local_datetime(raw["create_time"]),
                gateway_name=self.gateway_name,
            )
            reduce_only = raw["is_reduce_only"]
            if reduce_only:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_contract(self, data, request):
        """ """
        for raw in data:
            symbol = raw["name"]
            contract = ContractData(
                symbol=symbol,
                exchange=Exchange.GATEIO,
                name=symbol,
                price_tick=float(raw["order_price_round"]),
                size=float(raw["quanto_multiplier"]),  # 合约面值，即1张合约对应多少标的币种
                min_volume=raw["order_size_min"],
                max_volume=raw["order_size_max"],
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            # BTC_USD交易所返回合约面值数据为0，需重新赋值
            if symbol == "BTC_USD":
                contract.size = 0.01
            self.gateway.on_contract(contract)

        self.gateway.write_log(f"交易接口：{self.gateway_name} 合约信息查询成功")
        self.contract_inited = True
        # 等待rest api获取到account id再连接websocket api
        self.ws_api.connect(
            self.key,
            self.secret,
            self.server,
            self.proxy_host,
            self.proxy_port,
            self.account_id,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data, request):
        """ """
        order = request.extra
        gateway_orderid = str(data["id"])
        self.orderid_map[order.orderid] = gateway_orderid
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: int, request: Request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, data, request):
        """ """
        if data["status"] == "error":
            error_code = data["err_code"]
            error_msg = data["err_msg"]
            self.gateway.write_log(f"撤单失败，错误代码：{error_code}，信息：{error_msg}")
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order_failed(self, status_code: str, request: Request):
        """
        Callback when canceling order failed on server.
        """
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)


# ----------------------------------------------------------------------------------------------------
class GateiosWebsocketApi(WebsocketClient):
    """ """

    def __init__(self, gateway: GateiosGateway):
        """ """
        super(GateiosWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.account_id = ""

        self.trade_count = 0
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.order_book_bids = defaultdict(dict)  # 订单簿买单字典
        self.order_book_asks = defaultdict(dict)  # 订单簿卖单字典
        self.topic_map = {
            "futures.order_book_update":self.on_depth,
            "futures.book_ticker":self.on_book_ticker,
            "futures.trades":self.on_public_trade,
            "futures.orders": self.on_order,
            "futures.usertrades": self.on_trade,
            "futures.positions": self.on_position,
        }
    # ----------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
        account_id: str,
    ):
        """ """
        self.key = key
        self.secret = secret
        self.account_id = account_id

        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)

        self.start()
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self):
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API连接成功")
        # 重订阅标的tick数据
        for req in list(self.subscribed.values()):
            self.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest):
        """
        订阅tick数据
        """
        while not self.account_id:
            rest_api = self.gateway.rest_api
            rest_api.query_account()
            self.account_id = rest_api.account_id
            sleep(1)
        # 订阅symbol主题
        topic = [
            "futures.usertrades",
            "futures.orders",
            "futures.positions",
        ]
        for channel in topic:
            topic_req = self.generate_req(channel=channel, event="subscribe", pay_load=[self.account_id, req.symbol])
            self.send_packet(topic_req)
        tick = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            name=req.symbol,
            datetime=datetime.now(TZ_INFO),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick
        self.subscribed[req.symbol] = req
        # 订阅tick行情
        tick_req = self.generate_req(channel="futures.tickers", event="subscribe", pay_load=[req.symbol])
        self.send_packet(tick_req)
        # 反向合约最快深度推送100ms
        depth_req = self.generate_req(channel="futures.order_book_update", event="subscribe", pay_load=[req.symbol, "100ms", "5"])
        self.send_packet(depth_req)
        # 订阅逐笔成交
        public_trade_req = self.generate_req(channel="futures.trades", event="subscribe", pay_load=[req.symbol])
        self.send_packet(public_trade_req)
        # 订阅逐笔一档深度
        book_ticker_req = self.generate_req(channel="futures.book_ticker", event="subscribe", pay_load=[req.symbol])
        self.send_packet(book_ticker_req)
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API连接断开")
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Dict):
        """ """
        timestamp = packet["time_ms"]
        channel = packet["channel"]
        event = packet["event"]
        result = packet["result"]
        error = packet.get("error", None)
        if error:
            self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API报错：{error}")
            return
        if event == "subscribe":
            return
        if channel == "futures.tickers":
            self.on_tick(result, timestamp)
        else:
            self.topic_map[channel](result)
    # ----------------------------------------------------------------------------------------------------
    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """ """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb))
    # ----------------------------------------------------------------------------------------------------
    def generate_req(self, channel: str, event: str, pay_load: List):
        """ """
        expires = int(time())
        signature = generate_websocket_sign(self.secret, channel, event, expires)

        req = {"time": expires, "channel": channel, "event": event, "payload": pay_load, "auth": {"method": "api_key", "KEY": self.key, "SIGN": signature}}

        return req
    # ----------------------------------------------------------------------------------------------------
    def on_tick(self, raw: List, timestamp: int):
        """
        收到tick回报
        """
        for data in raw:
            symbol = data["contract"]
            tick = self.ticks[symbol]
            tick.high_price = float(data["high_24h"])
            tick.low_price = float(data["low_24h"])
            tick.last_price = float(data["last"])
            tick.volume = int(data["volume_24h_base"])     # 最近24小时币的成交量
            tick.datetime = get_local_datetime(timestamp)
    # ----------------------------------------------------------------------------------------------------
    def on_depth(self, raw: Dict):
        """
        收到深度回报
        """
        timestamp = raw["t"]
        symbol = raw["s"]
        tick = self.ticks[symbol]
        
        # 更新order book的通用函数
        def update_order_book(order_book:Dict[float,float], data:List[Dict[str,Union[str,float]]]):
            for item in data:
                price, volume = float(item["p"]), float(item["s"])
                if volume > 0:
                    order_book[price] = volume
                else:
                    order_book.pop(price, None)
        
        # 更新买单和卖单
        update_order_book(self.order_book_bids[symbol], raw["b"])
        update_order_book(self.order_book_asks[symbol], raw["a"])

        # 设置最高的5个买单和卖单价格及数量
        def set_top_prices_volumes(order_book:Dict[float,float], prefix:str):
            sorted_items = sorted(order_book.items(), key=lambda x: x[0], reverse=(prefix == "bid"))[:5]
            for index, (price, volume) in enumerate(sorted_items, start=1):
                setattr(tick, f"{prefix}_price_{index}", price)
                setattr(tick, f"{prefix}_volume_{index}", volume)
        
        set_top_prices_volumes(self.order_book_bids[symbol], "bid")
        set_top_prices_volumes(self.order_book_asks[symbol], "ask")
        
        # 更新时间
        tick.datetime = get_local_datetime(timestamp)
    # ----------------------------------------------------------------------------------------------------
    def on_book_ticker(self, raw: Dict):
        """
        收到逐笔一档深度回报
        """
        symbol = raw["s"]
        timestamp = raw["t"]
        tick = self.ticks[symbol]
        tick.datetime = get_local_datetime(timestamp)
        tick.bid_price_1,tick.bid_volume_1 = float(raw["b"]),float(raw["B"])
        tick.ask_price_1,tick.ask_volume_1 = float(raw["a"]),float(raw["A"])
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_public_trade(self, raw: Dict):
        """
        逐笔成交回报
        """
        data = raw[-1]
        symbol = data["contract"]
        timestamp = data["create_time_ms"]
        tick = self.ticks[symbol]
        tick.datetime = get_local_datetime(timestamp)
        tick.last_price = float(data["price"])
        self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, raw: List):
        """
        收到委托单回报
        """
        for data in raw:
            local_orderid = str(data["text"])[2:]

            if data["size"] > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT

            volume = abs(data["size"])
            traded = abs(data["size"] - data["left"])
            status = get_order_status(data["status"], volume, traded)
            reduce_only = data["is_reduce_only"]
            order = OrderData(
                orderid=local_orderid,
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                price=float(data["price"]),
                volume=volume,
                traded=traded,
                type=OrderType.LIMIT,
                direction=direction,
                status=status,
                datetime=get_local_datetime(data["create_time_ms"]),
                gateway_name=self.gateway_name,
            )
            if reduce_only:
                order.offset = Offset.CLOSE
            orderid_map = self.gateway.rest_api.orderid_map
            if not order.is_active():
                if local_orderid in orderid_map:
                    orderid_map.pop(local_orderid)
            else:
                orderid_map[local_orderid] = str(data["id"])

            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def on_trade(self, raw: List):
        """
        收到成交回报
        """
        for data in raw:
            volume = float(data["size"])
            if volume > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            trade = TradeData(
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                orderid=data["text"][2:],
                tradeid=data["id"],
                direction=direction,
                price=float(data["price"]),
                volume=abs(data["size"]),
                datetime=get_local_datetime(data["create_time_ms"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    # ----------------------------------------------------------------------------------------------------
    def on_position(self, raw: List):
        """
        * 收到持仓回报
        * websocket没有未结持仓盈亏参数
        """
        position_pnl = self.gateway.rest_api.position_pnl
        for data in raw:
            volume = float(data["size"])
            if volume >= 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            pos_1 = PositionData(
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                volume=abs(volume),
                direction=direction,
                price=float(data["entry_price"]),
                gateway_name=self.gateway_name,
            )
            pos_1_direction = pos_1.vt_symbol + pos_1.direction.value
            pos_1.pnl = position_pnl.get(pos_1_direction, 0)

            pos_2 = PositionData(
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[direction],
                volume=0,
                price=0,
                pnl=0,  # 持仓盈亏
                frozen=0,  # 持仓冻结保证金
            )
            self.gateway.on_position(pos_1)
            self.gateway.on_position(pos_2)


# ----------------------------------------------------------------------------------------------------
def generate_sign(key, secret, method, path, get_params=None, get_data=None):
    """ """
    if get_params:
        params = urlencode(get_params)
    else:
        params = ""

    hashed_data = get_hashed_data(get_data)

    timestamp = str(time())

    pay_load = [method, path, params, hashed_data, timestamp]
    pay_load = "\n".join(pay_load)

    signature = hmac.new(secret.encode("utf-8"), pay_load.encode("utf-8"), hashlib.sha512).hexdigest()

    headers = {"Accept": "application/json", "Content-Type": "application/json", "KEY": key, "Timestamp": timestamp, "SIGN": signature}

    return headers


# ----------------------------------------------------------------------------------------------------
def get_hashed_data(get_data):
    """ """
    hashed_data = hashlib.sha512()
    if get_data:
        data = get_data
        hashed_data.update(data.encode("utf-8"))

    return hashed_data.hexdigest()


# ----------------------------------------------------------------------------------------------------
def generate_websocket_sign(secret: str, channel: str, event: str, time: int):
    """ """
    message = "channel={}&event={}&time={}".format(channel, event, time)

    signature = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512).hexdigest()

    return signature


# ----------------------------------------------------------------------------------------------------
def get_order_status(status: str, volume: int, traded: int):
    """
    获取委托单成交状态
    """
    if status == "open":
        if traded:
            return Status.PARTTRADED
        else:
            return Status.NOTTRADED
    else:
        if traded == volume:
            return Status.ALLTRADED
        else:
            return Status.CANCELLED
