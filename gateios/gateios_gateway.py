import hashlib
import hmac
import json
import sys
import csv
from time import time,sleep
from copy import copy
from datetime import datetime, timedelta
from urllib.parse import urlencode
from typing import List, Dict
from peewee import chunked
from pathlib import Path

from vnpy.trader.database import database_manager
from vnpy.trader.utility import (save_connection_status,delete_dr_data,get_folder_path,load_json, save_json,remain_digit,get_symbol_mark,get_local_datetime,extract_vt_symbol,TZ_INFO,publish_redis_data,GetFilePath)
from vnpy.trader.setting import gateio_account
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event
from vnpy.trader.constant import (Direction,Offset, Exchange, Interval, OrderType,
                                  Product, Status)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway, LocalOrderManager
from vnpy.trader.object import (AccountData, BarData, CancelRequest,
                                ContractData, HistoryRequest, OrderData,
                                OrderRequest, PositionData, SubscribeRequest,
                                TickData, TradeData)


TESTNET_REST_HOST = "https://fx-api-testnet.gateio.ws"
REST_HOST = "https://fx-api.gateio.ws"

TESTNET_WEBSOCKET_HOST = "wss://fx-ws-testnet.gateio.ws/v4/ws"
WEBSOCKET_HOST = "wss://fx-ws.gateio.ws/v4/ws/btc"      #币本位永续ws_host

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

#-------------------------------------------------------------------------------------------------
class GateiosGateway(BaseGateway):
    """
    Gateio币本位永续合约
    """

    default_setting = {
        "API Key": "",
        "Secret Key": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": "",
    }
    #所有合约列表
    recording_list = GetFilePath.recording_list
    exchanges = [Exchange.GATEIO]
    #-------------------------------------------------------------------------------------------------
    def __init__(self, event_engine):
        """
        """
        super().__init__(event_engine, "GATEIOS")
        self.order_manager = LocalOrderManager(self)

        self.ws_api:GateiosWebsocketApi = GateiosWebsocketApi(self)
        self.rest_api:GateiosRestApi = GateiosRestApi(self)
        self.query_count = 0
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if extract_vt_symbol(vt_symbol)[2] == self.gateway_name  and not extract_vt_symbol(vt_symbol)[0].endswith("99")]
        #历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        self.query_func = [self.query_account,self.query_position,self.query_order]
    #-------------------------------------------------------------------------------------------------
    def connect(self, log_account: Dict):
        """
        """
        if not log_account:
            log_account = gateio_account    
        key = log_account["API Key"]
        secret = log_account["Secret Key"]
        server = log_account["服务器"]
        proxy_host = log_account["代理地址"]
        proxy_port = log_account["代理端口"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)

        self.init_query()
    #-------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest):
        """
        """
        self.ws_api.subscribe(req)
    #-------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest):
        """
        """
        return self.rest_api.send_order(req)
    #-------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest):
        """
        """
        self.rest_api.cancel_order(req)
    #-------------------------------------------------------------------------------------------------
    def query_account(self):
        """
        """
        self.rest_api.query_account()
    #-------------------------------------------------------------------------------------------------
    def query_position(self):
        """
        """
        self.rest_api.query_position()
    #-------------------------------------------------------------------------------------------------
    def query_order(self):
        """
        """
        self.rest_api.query_order()
    #-------------------------------------------------------------------------------------------------   
    def query_history(self,event):
        """
        查询合约历史数据
        """
        # 等待restapi合约数据推送完成后再查询历史数据
        if not self.rest_api.contract_inited:
            return
        if len(self.history_contracts) > 0:
            symbol,exchange,gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol = symbol,
                exchange = Exchange(exchange),
                interval = Interval.MINUTE,
                start = datetime.now(TZ_INFO) - timedelta(days = 1),
                gateway_name = self.gateway_name
            )
            self.rest_api.query_history(req)
            self.rest_api.set_leverage(symbol)
    #-------------------------------------------------------------------------------------------------
    def close(self):
        """
        """
        self.rest_api.stop()
        self.ws_api.stop()
    #-------------------------------------------------------------------------------------------------
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
    #-------------------------------------------------------------------------------------------------
    def init_query(self):
        """
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TIMER, self.query_history)
#-------------------------------------------------------------------------------------------------
class GateiosRestApi(RestClient):
    """
    Gateios REST API
    """

    def __init__(self, gateway: GateiosGateway):
        """
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager
        self.ws_api = gateway.ws_api

        self.key = ""
        self.secret = ""
        self.account_id = ""
        self.server = ""
        self.proxy_host = ""
        self.proxy_port = 0

        self.account_date = None    #账户日期
        self.accounts_info:Dict[str,dict] = {}
        self.contract_inited:bool = False
    #-------------------------------------------------------------------------------------------------
    def sign(self, request):
        """
        Generate signature.
        """
        request.headers = generate_sign(
            self.key,
            self.secret,
            request.method,
            request.path,
            get_params=request.params,
            get_data=request.data
        )

        if not request.data:
            request.data = ""

        return request
    #-------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ):
        """
        初始化连接REST
        """
        self.key = key
        self.secret = secret
        self.server = server
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port,gateway_name = self.gateway_name)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port,gateway_name = self.gateway_name)

        self.start(3)
        self.gateway.write_log(f"交易接口：{self.gateway_name}REST API启动成功")
    #-------------------------------------------------------------------------------------------------
    def set_leverage(self,symbol:str):
        """
        设置单向全仓杠杆
        """
        params = {
            "leverage":"0",
            "cross_leverage_limit":"20"
        }
        self.add_request(
            method="POST",
            path=f"/api/v4/futures/btc/positions/{symbol}/leverage",
            callback=self.on_leverage,
            params = params
        )
    #-------------------------------------------------------------------------------------------------
    def on_leverage(self,data:dict, request: Request):
        """
        收到修改杠杆回报
        """
        pass
    #-------------------------------------------------------------------------------------------------
    def query_account(self):
        """
        """
        self.add_request(
            method="GET",
            path="/api/v4/futures/btc/accounts",
            callback=self.on_query_account
        )
    #-------------------------------------------------------------------------------------------------
    def query_position(self):
        """
        """
        self.add_request(
            method="GET",
            path="/api/v4/futures/btc/positions",
            callback=self.on_query_position
        )
    #-------------------------------------------------------------------------------------------------
    def query_order(self):
        """
        """
        for vt_symbol in self.gateway.recording_list:
            symbol,exchange,gateway_name = extract_vt_symbol(vt_symbol)
            params = {
                "contract": symbol,
                "status": "open",
            }

            self.add_request(
                method="GET",
                path="/api/v4/futures/btc/orders",
                callback=self.on_query_order,
                on_failed= self.query_order_failed,
                params=params
            )
    #-------------------------------------------------------------------------------------------------
    def query_contract(self):
        """
        """
        self.add_request(
            method="GET",
            path=f"/api/v4/futures/btc/contracts",
            callback=self.on_query_contract
        )
    #-------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest):
        """
        """
        history = []
        interval = INTERVAL_VT2GATEIO[req.interval]
        time_consuming_start = time()
        params = {
            "contract": req.symbol,
            "limit": 2000,
            "interval": interval,
        }

        resp = self.request(
            method="GET",
            path="/api/v4/futures/btc/candlesticks",
            params=params
        )

        if resp.status_code // 100 != 2:
            msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.gateway.write_log(msg)
        else:
            data = resp.json()
            if not data:
                msg = f"标的：{req.vt_symbol}获取历史数据为空"
                self.gateway.write_log(msg)
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
                    gateway_name=self.gateway_name
                )
                history.append(bar)

        if not history:
            self.gateway.write_log(f"标的：{req.vt_symbol}获取历史数据为空")
            return
        for bar_data in chunked(history, 10000):               #分批保存数据
            try:
                database_manager.save_bar_data(bar_data,False)      #保存数据到数据库  
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return    
        time_consuming_end =time()        
        query_time = round(time_consuming_end - time_consuming_start,3)
        msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime} ，结束时间： {history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
        self.gateway.write_log(msg)   
    #------------------------------------------------------------------------------------------------- 
    def query_order_failed(self, status_code: int, request: Request) -> None:
        """
        查询未成交委托单错误回调
        """
        self.gateway.write_log(f"错误代码：{status_code}，错误请求：{request.path}，错误信息：{request.response.json()}")
    #-------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest):
        """
        """
        local_orderid = self.order_manager.new_local_orderid()
        order = req.create_order_data(
            local_orderid,
            self.gateway_name
        )
        order.datetime = datetime.now(TZ_INFO)

        if req.direction == Direction.SHORT:
            volume = -int(req.volume)
        else:
            volume = int(req.volume)

        request_body = {
            "contract": req.symbol,
            "size": volume,
            "price": str(req.price),
            "tif": "gtc",
            "text": f"t-{local_orderid}"
        }
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
            on_failed=self.on_send_order_failed
        )
        self.order_manager.on_order(order)
        return order.vt_orderid
    #-------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest):
        """
        """
        sys_orderid = self.order_manager.get_sys_orderid(req.orderid)
        if not sys_orderid:
            self.write_log("撤单失败，找不到对应系统委托号{}".format(req.orderid))
            return

        self.add_request(
            method="DELETE",
            path=f"/api/v4/futures/btc/orders/{sys_orderid}",
            callback=self.on_cancel_order,
            on_failed=self.on_cancel_order_failed,
            extra=req
        )
    #-------------------------------------------------------------------------------------------------
    def on_query_account(self, data, request):
        """
        """
        self.account_id = str(data["user"])

        account = AccountData(
            accountid= f"BTC_{self.gateway_name}",
            balance=float(data["total"]),
            frozen=float(data["total"]) - float(data["available"]),
            position_profit = float(data["unrealised_pnl"]),
            margin = float(data["order_margin"]),
            datetime = datetime.now(TZ_INFO),
            gateway_name=self.gateway_name,
        )
        # 用accounts_info过滤只查询一次合约信息
        if not self.accounts_info:
            self.query_contract()

        if account.balance:
            self.gateway.on_account(account)
            #保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__

        if  not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = GetFilePath().ctp_account_path.replace("ctp_account_1",self.gateway.account_file_name)
        for account_data in accounts_info:
            if not Path(account_path).exists(): # 如果文件不存在，需要写header
                with open(account_path, 'w',newline="") as f1:          #newline=""不自动换行
                    w1 = csv.DictWriter(f1, account_data.keys())
                    w1.writeheader()
                    w1.writerow(account_data)
            else: # 文件存在，不需要写header
                if self.account_date and self.account_date != account_date:        #一天写入一次账户信息         
                    with open(account_path,'a',newline="") as f1:                               #a二进制追加形式写入
                        w1 = csv.DictWriter(f1, account_data.keys())
                        w1.writerow(account_data)
        self.account_date = account_date
    #-------------------------------------------------------------------------------------------------
    def on_query_position(self, data, request):
        """
        """
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
                pnl=float(raw["realised_pnl"]),
                gateway_name=self.gateway_name,
            )
            pos_2 = PositionData(
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                gateway_name=self.gateway_name,
                direction = OPPOSITE_DIRECTION[direction],
                volume=0,
                price=0,
                pnl = 0,          #持仓盈亏
                frozen= 0,        # 持仓冻结保证金
            )
            self.gateway.on_position(pos_1)
            self.gateway.on_position(pos_2)
    #-------------------------------------------------------------------------------------------------
    def on_query_order(self, data, request):
        """
        """
        for raw in data:
            local_orderid = str(raw["text"])[2:]
            sys_orderid = str(raw["id"])
            self.order_manager.update_orderid_map(
                local_orderid=local_orderid,
                sys_orderid=sys_orderid
            )
            volume = abs(raw["size"])
            traded = abs(raw["size"] - raw["left"])
            status = get_order_status(raw["status"], volume, traded)
            if volume > 0:
                direction = Direction.LONG
            else:
                direction = Direction.SHORT
            order = OrderData(
                orderid=local_orderid,
                symbol=raw["contract"],
                exchange=Exchange.GATEIO,
                price=float(raw["price"]),
                volume=abs(volume),
                direction=direction,
                status=status,
                datetime=get_local_datetime(raw["create_time"]),
                gateway_name=self.gateway_name,
            )
            reduce_only = raw["is_reduce_only"]
            if reduce_only:
                order.offset = Offset.CLOSE
            self.order_manager.on_order(order)
    #-------------------------------------------------------------------------------------------------
    def on_query_contract(self, data, request):
        """
        """
        for raw in data:
            symbol = raw["name"]

            contract = ContractData(
                symbol=symbol,
                exchange=Exchange.GATEIO,
                name=symbol,
                price_tick=float(raw["order_price_round"]),
                size=int(raw["leverage_max"]),
                min_volume=raw["order_size_min"],
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
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
    #-------------------------------------------------------------------------------------------------
    def on_send_order(self, data, request):
        """
        """
        order = request.extra
        sys_orderid = str(data["id"])
        self.order_manager.update_orderid_map(order.orderid, sys_orderid)
    #-------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: str, request: Request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #-------------------------------------------------------------------------------------------------
    def on_send_order_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    #-------------------------------------------------------------------------------------------------
    def on_cancel_order(self, data, request):
        """
        """
        if data["status"] == "error":
            error_code = data["err_code"]
            error_msg = data["err_msg"]
            self.gateway.write_log(f"撤单失败，错误代码：{error_code}，信息：{error_msg}")
    #-------------------------------------------------------------------------------------------------
    def on_cancel_order_failed(self, status_code: str, request: Request):
        """
        Callback when canceling order failed on server.
        """
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

#-------------------------------------------------------------------------------------------------
class GateiosWebsocketApi(WebsocketClient):
    """
    """

    def __init__(self, gateway):
        """
        """
        super(GateiosWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.order_manager = gateway.order_manager

        self.key = ""
        self.secret = ""
        self.account_id = ""

        self.trade_count = 0
        self.ticks: Dict[str, TickData]= {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
    #-------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
        account_id: str,
    ):
        """
        """
        self.key = key
        self.secret = secret
        self.account_id = account_id

        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port,gateway_name = self.gateway_name)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port,gateway_name = self.gateway_name)

        self.start()
    #-------------------------------------------------------------------------------------------------
    def on_connected(self):
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API连接成功")
        # 重订阅标的tick数据
        for req in list(self.subscribed.values()):
            self.subscribe(req)
    #-------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest):
        """
        订阅tick数据
        """
        while True:
            if not self.account_id:
                sleep(1)
            else:
                break
        # 订阅symbol主题
        topic = [
                "futures.usertrades",
                "futures.orders",
                "futures.positions",
            ]
        for channel in topic:
            topic_req = self.generate_req(
                channel=channel,
                event="subscribe",
                pay_load=[self.account_id, req.symbol]
            )
            self.send_packet(topic_req)
        # 订阅tick和深度数据
        tick = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            name=req.symbol,
            datetime=datetime.now(TZ_INFO),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick
        self.subscribed[req.symbol] = req

        tick_req = self.generate_req(
            channel="futures.tickers",
            event="subscribe",
            pay_load=[req.symbol]
        )
        self.send_packet(tick_req)

        depth_req = self.generate_req(
            channel="futures.order_book",
            event="subscribe",
            pay_load=[req.symbol, "5", "0"]
        )
        self.send_packet(depth_req)
    #-------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API连接断开")
    #-------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Dict):
        """
        """
        timestamp = packet["time"]
        channel = packet["channel"]
        event = packet["event"]
        result = packet["result"]
        error = packet.get("error",None)
        if error:
            self.gateway.write_log(f"交易接口：{self.gateway_name} Websocket API报错：{error}")
            return
        if channel == "futures.tickers" and event == "update":
            self.on_tick(result, timestamp)
        elif channel == "futures.order_book" and event == "all":
            self.on_depth(result, timestamp)
        elif channel == "futures.orders" and event == "update":
            self.on_order(result, timestamp)
        elif channel == "futures.usertrades" and event == "update":
            self.on_trade(result, timestamp)
        elif channel == "futures.positions" and event == "update":
            self.on_position(result, timestamp)
    #-------------------------------------------------------------------------------------------------
    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """
        """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(
            exception_type, exception_value, tb))
    #-------------------------------------------------------------------------------------------------
    def generate_req(self, channel: str, event: str, pay_load: List):
        """
        """
        expires = int(time())
        signature = generate_websocket_sign(
            self.secret, channel, event, expires)

        req = {
            "time": expires,
            "channel": channel,
            "event": event,
            "payload": pay_load,
            "auth": {
                "method": "api_key",
                "KEY": self.key,
                "SIGN": signature
            }
        }

        return req
    #-------------------------------------------------------------------------------------------------
    def on_tick(self, raw: List, timestamp:int):
        """
        收到tick回报
        """
        data = raw[0]
        symbol = data["contract"]
        tick = self.ticks.get(symbol, None)
        if not tick:
            return
        tick.last_price = float(data["last"])
        tick.volume = int(data["volume_24h"])
        tick.datetime = get_local_datetime(timestamp)
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    #-------------------------------------------------------------------------------------------------
    def on_depth(self, raw: Dict, timestamp: int):
        """
        收到tick深度回报
        """
        symbol = raw["contract"]
        tick = self.ticks.get(symbol, None)
        if not tick:
            return
        for index, buf in enumerate(raw["bids"][:5]):
            price = float(buf["p"])
            volume = buf["s"]
            tick.__setattr__("bid_price_%s" % (index + 1), price)
            tick.__setattr__("bid_volume_%s" % (index + 1), volume)

        for index, buf in enumerate(raw["asks"][:5]):
            price = float(buf["p"])
            volume = buf["s"]
            tick.__setattr__("ask_price_%s" % (index + 1), price)
            tick.__setattr__("ask_volume_%s" % (index + 1), volume)

        tick.datetime = get_local_datetime(timestamp)
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    #-------------------------------------------------------------------------------------------------
    def on_order(self, raw: List, timestamp: int):
        """
        收到委托单回报
        """
        data = raw[0]
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
            traded = traded,
            type=OrderType.LIMIT,
            direction=direction,
            status=status,
            datetime=get_local_datetime(timestamp),
            gateway_name=self.gateway_name,
        )
        if reduce_only:
            order.offset = Offset.CLOSE
        self.order_manager.on_order(order)
    #-------------------------------------------------------------------------------------------------
    def on_trade(self, raw: List, timestamp: int):
        """
        收到成交回报
        """
        data = raw[0]

        sys_orderid = data["order_id"]
        order = self.order_manager.get_order_with_sys_orderid(sys_orderid)
        if not order:
            return
        trade = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=data["id"],
            direction=order.direction,
            offset = order.offset,
            price=float(data["price"]),
            volume=abs(data["size"]),
            datetime=get_local_datetime(data["create_time"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)
    #-------------------------------------------------------------------------------------------------
    def on_position(self,raw:List, timestamp:int):
        """
        收到持仓回报
        """
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
                pnl=float(data["realised_pnl"]),
                gateway_name=self.gateway_name,
            )
            pos_2 = PositionData(
                symbol=data["contract"],
                exchange=Exchange.GATEIO,
                gateway_name=self.gateway_name,
                direction = OPPOSITE_DIRECTION[direction],
                volume=0,
                price=0,
                pnl = 0,          #持仓盈亏
                frozen= 0,        # 持仓冻结保证金
            )
            self.gateway.on_position(pos_1)
            self.gateway.on_position(pos_2)
#-------------------------------------------------------------------------------------------------
def generate_sign(key, secret, method, path, get_params=None, get_data=None):
    """
    """
    if get_params:
        params = urlencode(get_params)
    else:
        params = ""

    hashed_data = get_hashed_data(get_data)

    timestamp = str(time())

    pay_load = [method, path, params, hashed_data, timestamp]
    pay_load = "\n".join(pay_load)

    signature = hmac.new(
        secret.encode("utf-8"),
        pay_load.encode("utf-8"),
        hashlib.sha512
    ).hexdigest()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": key,
        "Timestamp": str(timestamp),
        "SIGN": signature
    }

    return headers

#-------------------------------------------------------------------------------------------------
def get_hashed_data(get_data):
    """
    """
    hashed_data = hashlib.sha512()
    if get_data:
        data = get_data
        hashed_data.update(data.encode("utf-8"))

    return hashed_data.hexdigest()
#-------------------------------------------------------------------------------------------------
def generate_websocket_sign(secret, channel, event, time):
    """
    """
    message = 'channel=%s&event=%s&time=%s' % (channel, event, time)

    signature = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha512
    ).hexdigest()

    return signature
#-------------------------------------------------------------------------------------------------
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
