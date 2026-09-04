import asyncio, aiohttp, websockets, json, os, time, base64, uuid, logging, argparse
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from urllib.parse import urlencode
from cryptography.hazmat.primitives import serialization

# ============================================================
# KONFIGURASI GLOBAL / URL
# ============================================================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
RECV_WINDOW = 5000
REST_URL = "https://fapi.binance.com"
USER_STREAM_URL = "wss://fstream.binance.com/ws/"
LISTEN_KEY_URL = "/fapi/v1/listenKey"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

class BinanceError(Exception):
    def __init__(self, code, msg):
        self.code, self.msg = code, msg
        super().__init__(f"{code}: {msg}")

class DiscordNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        self.colors = {"BUY": 3447003, "TP": 5763719, "WARNING": 15548997, "INFO": 16776960}

    async def send(self, title, description, category="INFO", fields=None):
        if not self.webhook_url: return
        fields = fields or {}
        embed = {"title": title, "description": description, "color": self.colors.get(category, 16777215), "fields": [{"name": str(k), "value": str(v), "inline": True} for k, v in fields.items()], "footer": {"text": "One-Shot Futures Executor"}}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json={"embeds": [embed]}) as r:
                    if r.status not in (200, 204): logging.error(f"[DISCORD] HTTP {r.status}: {await r.text()}")
        except Exception as e:
            logging.error(f"[DISCORD] {e}")

class FuturesOneShot:
    def __init__(self, args):
        self.symbol = args.symbol.upper()
        self.margin_usdt = args.margin
        self.leverage = args.leverage
        self.target_net_pnl = args.target
        self.interval = args.interval
        self.real_trading = args.real
        self.max_qty_precision_arg = args.max_qty_precision

        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        if not self.api_key or not self.api_secret: raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET belum diset.")

        formatted_secret = self.api_secret.replace("\\n", "\n")
        if "-----BEGIN" not in formatted_secret: raise RuntimeError("BINANCE_API_SECRET bukan PEM Ed25519 yang valid.")

        self.private_key = serialization.load_pem_private_key(formatted_secret.encode("ascii"), password=None)
        self.session = None
        self.server_offset = 0
        self.price_tick = Decimal("0")
        self.qty_step = Decimal("0")
        self.price_precision = 8
        self.qty_precision = 8
        self.min_qty = Decimal("0")
        self.max_qty = Decimal("0")
        self.min_notional = Decimal("0")
        self.entry_order_id = None
        self.entry_client_id = None
        self.tp_order_id = None
        self.tp_client_id = None
        self.entry_price = Decimal("0")
        self.entry_qty = Decimal("0")
        self.entry_commission = Decimal("0")
        self.exit_commission_rate = Decimal("0")
        self.tp_price = Decimal("0")
        self.listen_key = None
        self.entry_filled_event = asyncio.Event()
        self.position_closed_event = asyncio.Event()
        self.entry_filled_data = None
        self.close_event_data = None
        self.notifier = DiscordNotifier(DISCORD_WEBHOOK_URL)

    # ========================================================
    # TIME
    # ========================================================
    def now(self): return int(time.time() * 1000) + self.server_offset

    async def sync_time(self):
        async with self.session.get(REST_URL + "/fapi/v1/time") as r:
            data = await r.json()
            self.server_offset = data["serverTime"] - int(time.time() * 1000)
        logging.info(f"[TIME] Server offset: {self.server_offset} ms")

    # ========================================================
    # REQUEST / SIGNING
    # ========================================================
    async def request(self, method, endpoint, params=None, signed=False):
        params = dict(params or {})
        headers = {"X-MBX-APIKEY": self.api_key}

        if signed:
            params["timestamp"] = self.now()
            params["recvWindow"] = RECV_WINDOW
            query = urlencode(params)
            signature = self.private_key.sign(query.encode("ascii"))
            params["signature"] = base64.b64encode(signature).decode("ascii")
            url = REST_URL + endpoint + "?" + urlencode(params)

            async with self.session.request(method, url, headers=headers) as r:
                try: data = await r.json()
                except Exception:
                    text = await r.text()
                    raise RuntimeError(f"Response bukan JSON: HTTP {r.status} {text[:300]}")
                if r.status >= 400: raise BinanceError(data.get("code"), data.get("msg"))
                return data

        async with self.session.request(method, REST_URL + endpoint, params=params, headers=headers) as r:
            try: data = await r.json()
            except Exception:
                text = await r.text()
                raise RuntimeError(f"Response bukan JSON: HTTP {r.status} {text[:300]}")
            if r.status >= 400: raise BinanceError(data.get("code"), data.get("msg"))
            return data

    # ========================================================
    # EXCHANGE RULES
    # ========================================================
    async def load_exchange_rules(self):
        data = await self.request("GET", "/fapi/v1/exchangeInfo", {"symbol": self.symbol})
        info = data["symbols"][0]
        filters = {x["filterType"]: x for x in info["filters"]}
        
        # Presisi Harga
        api_price_precision = int(info.get("pricePrecision", 8))
        tick_size_str = filters["PRICE_FILTER"]["tickSize"]
        price_tick = Decimal(tick_size_str)
        tick_precision = len(tick_size_str.rstrip('0').split('.')[1]) if '.' in tick_size_str.rstrip('0') else 0
        self.price_precision = min(api_price_precision, tick_precision)
        self.price_tick = price_tick

        # Presisi Kuantitas
        api_qty_precision = int(info.get("quantityPrecision", 8))
        step_size_str = filters["LOT_SIZE"]["stepSize"]
        qty_step = Decimal(step_size_str)
        step_precision = len(step_size_str.rstrip('0').split('.')[1]) if '.' in step_size_str.rstrip('0') else 0
        
        calculated_qty_precision = min(api_qty_precision, step_precision)
        if self.max_qty_precision_arg is not None:
            self.qty_precision = min(calculated_qty_precision, self.max_qty_precision_arg)
        else:
            self.qty_precision = calculated_qty_precision

        self.qty_step = qty_step
        self.min_qty = Decimal(filters["LOT_SIZE"]["minQty"]).normalize()
        self.max_qty = Decimal(filters["LOT_SIZE"]["maxQty"]).normalize()
        if "MIN_NOTIONAL" in filters: self.min_notional = Decimal(filters["MIN_NOTIONAL"]["notional"]).normalize()

        logging.info(f"[RULES] {self.symbol}")
        logging.info(f"[RULES] Price Tick  : {self.price_tick} (Max desimal: {self.price_precision})")
        logging.info(f"[RULES] Qty Step    : {self.qty_step} (Max desimal: {self.qty_precision})")
        logging.info(f"[RULES] Min qty     : {self.min_qty}")

    def round_price(self, price):
        if self.price_tick > 0:
            stepped = (price // self.price_tick) * self.price_tick
        else:
            stepped = price
        quant_target = Decimal("10") ** -self.price_precision
        return stepped.quantize(quant_target, rounding=ROUND_HALF_UP)

    def round_qty(self, qty):
        if self.qty_step > 0:
            stepped = (qty // self.qty_step) * self.qty_step
        else:
            stepped = qty
        quant_target = Decimal("10") ** -self.qty_precision
        return stepped.quantize(quant_target, rounding=ROUND_DOWN)
    
    # ========================================================
    # SINGLE ASSET MODE
    # ========================================================
    async def get_multi_assets_mode(self):
        data = await self.request("GET", "/fapi/v1/multiAssetsMargin", signed=True)
        return bool(data.get("multiAssetsMargin", False))

    async def ensure_single_asset_mode(self):
        current = await self.get_multi_assets_mode()

        if not current:
            logging.info("[ACCOUNT] Single-Asset Mode sudah aktif.")
            return

        logging.info("[ACCOUNT] Mengubah ke Single-Asset Mode...")
        await self.request("POST", "/fapi/v1/multiAssetsMargin", {"multiAssetsMargin": "false"}, signed=True)
        logging.info("[ACCOUNT] Single-Asset Mode aktif.")

    # ========================================================
    # CEK MARGIN MODE
    # ========================================================
    async def ensure_isolated(self):
        margin_type = ""
        try:
            data = await self.request("GET", "/fapi/v3/positionRisk", {"symbol": self.symbol}, signed=True)
            if isinstance(data, dict): data = [data]
            position = next((p for p in data if p.get("symbol") == self.symbol), None)
            if position:
                margin_type = position.get("marginType", "").upper()
        except Exception as e:
            logging.warning(f"[MARGIN] Gagal membaca status margin dari API: {e}")

        logging.info(f"[MARGIN] {self.symbol} = {margin_type if margin_type else 'TIDAK_TERBACA/CROSSED'}")

        if margin_type == "ISOLATED":
            logging.info(f"[MARGIN] {self.symbol} sudah ISOLATED.")
            return

        logging.info(f"[MARGIN] Mengubah {self.symbol} -> ISOLATED...")

        try:
            await self.request("POST", "/fapi/v1/marginType", {"symbol": self.symbol, "marginType": "ISOLATED"}, signed=True)
            logging.info(f"[MARGIN] {self.symbol} berhasil diubah ke ISOLATED.")
        except BinanceError as e:
            if e.code == -4046:
                logging.info(f"[MARGIN] {self.symbol} sudah ISOLATED (Response Binance Code -4046).")
            else:
                raise e

    # ========================================================
    # LEVERAGE
    # ========================================================
    async def set_leverage(self):
        data = await self.request("POST", "/fapi/v1/leverage", {"symbol": self.symbol, "leverage": self.leverage}, signed=True)
        logging.info(f"[LEVERAGE] {self.symbol} = {data.get('leverage')}x")

    # ========================================================
    # POSITION
    # ========================================================
    async def get_position(self):
        data = await self.request("GET", "/fapi/v3/positionRisk", {"symbol": self.symbol}, signed=True)
        if isinstance(data, dict): data = [data]

        for p in data:
            if p["symbol"] == self.symbol and Decimal(p["positionAmt"]) != 0: return p

        return None

    # ========================================================
    # COMMISSION
    # ========================================================
    async def load_commission_rate(self):
        data = await self.request("GET", "/fapi/v1/commissionRate", {"symbol": self.symbol}, signed=True)
        self.exit_commission_rate = Decimal(data["makerCommissionRate"])
        logging.info(f"[FEE] Maker commission = {self.exit_commission_rate}")

    # ========================================================
    # CANDLE CLOSED (Presisi REST + Countdown + Validation)
    # ========================================================
    def parse_interval_ms(self, interval_str):
        unit = interval_str[-1].lower()
        val = int(interval_str[:-1])
        if unit == 'm': return val * 60 * 1000
        elif unit == 'h': return val * 3600 * 1000
        elif unit == 'd': return val * 86400 * 1000
        return 900000

    async def wait_for_candle_close(self):
        interval_ms = self.parse_interval_ms(self.interval)
        now_ms = self.now()
        candle_end_ms = ((now_ms // interval_ms) + 1) * interval_ms
        expected_close_time = candle_end_ms - 1

        logging.info(f"[CANDLE] Menunggu candle {self.interval} benar-benar CLOSED...")

        last_log_time = 0
        while True:
            current_ms = self.now()
            remaining_sec = int((candle_end_ms - current_ms) / 1000)

            if remaining_sec <= 0:
                break

            if time.time() - last_log_time >= 30:
                logging.info(f"[CANDLE] Sisa waktu penutupan candle {self.interval}: {remaining_sec} detik...")
                last_log_time = time.time()

            await asyncio.sleep(1)

        logging.info(f"[CANDLE] Waktu candle {self.interval} selesai. Mengambil harga close...")
        await asyncio.sleep(1.5)

        closed_candle = None
        for _ in range(10):
            klines = await self.request("GET", "/fapi/v1/klines", {"symbol": self.symbol, "interval": self.interval, "limit": 5})
            for k in reversed(klines):
                if int(k[6]) == expected_close_time or int(k[0]) == (candle_end_ms - interval_ms):
                    closed_candle = k
                    break
            if closed_candle:
                break
            await asyncio.sleep(1)

        if not closed_candle:
            closed_candle = klines[-2] if len(klines) >= 2 else klines[-1]

        close_price = Decimal(closed_candle[4])

        logging.info(f"[CANDLE] CLOSED | Close={close_price} | Time={closed_candle[6]}")
        return close_price

    # ========================================================
    # CLIENT ID
    # ========================================================
    def client_id(self, name):
        return f"ONE_{name}_{int(time.time())}_{uuid.uuid4().hex[:8]}"

    # ========================================================
    # RECOVER / QUERY ENTRY
    # ========================================================
    async def query_order(self, order_id):
        return await self.request("GET", "/fapi/v1/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)

    async def recover_entry(self):
        if not self.entry_order_id or self.entry_order_id == "SIM_ENTRY_ID":
            return False
        try:
            order = await self.query_order(self.entry_order_id)
            status = order.get("status")
            executed_qty = Decimal(order.get("executedQty", "0"))

            if status == "FILLED" or executed_qty > 0:
                logging.info(f"[RECOVERY] Order {self.entry_order_id} terkonfirmasi FILLED/PARTIAL via REST API!")
                await self.process_entry_filled(order)
                return True
        except Exception as e:
            logging.warning(f"[RECOVERY] Gagal kueri order {self.entry_order_id}: {e}")
        return False

    # ========================================================
    # CANCEL ENTRY ORDER
    # ========================================================
    async def cancel_entry_order(self):
        if not self.entry_order_id or self.entry_order_id == "SIM_ENTRY_ID":
            return False
        try:
            res = await self.request("DELETE", "/fapi/v1/order", {
                "symbol": self.symbol,
                "orderId": self.entry_order_id
            }, signed=True)
            logging.info(f"[ENTRY] Order {self.entry_order_id} berhasil dibatalkan.")
            if res.get("status") == "FILLED" or Decimal(res.get("executedQty", "0")) > 0:
                await self.process_entry_filled(res)
                return True
            return False
        except BinanceError as e:
            if e.code == -2011:
                logging.info(f"[ENTRY] Order {self.entry_order_id} tidak ada di open orders (-2011). Memeriksa status eksekusi...")
                return await self.recover_entry()
            logging.warning(f"[ENTRY] Gagal membatalkan order entry: {e}")
            return await self.recover_entry()
        except Exception as e:
            logging.warning(f"[ENTRY] Error saat cancel order: {e}")
            return await self.recover_entry()

    # ========================================================
    # MONITOR & WAIT ENTRY FILL (30s / +0.2% Spike)
    # ========================================================
    async def wait_and_monitor_entry_fill(self, entry_target_price):
        logging.info("[ENTRY] Memantau eksekusi order Post-Only (Max 30s / Limit Kenaikan +0.2%)...")
        start_time = time.time()
        threshold_price = entry_target_price * Decimal("1.002")

        while time.time() - start_time < 30:
            if self.entry_filled_event.is_set():
                logging.info("[ENTRY] Order Post-Only TERISI (FILLED)!")
                return True

            if not self.real_trading:
                try:
                    ticker = await self.request("GET", "/fapi/v1/ticker/price", {"symbol": self.symbol})
                    curr_price = Decimal(ticker["price"])
                    if curr_price <= entry_target_price:
                        logging.info(f"[SIMULASI] Harga menyentuh order ({curr_price} <= {entry_target_price})!")
                        self.entry_filled_event.set()
                        return True
                    if curr_price > threshold_price:
                        logging.info(f"[SIMULASI] Harga melesat naik ({curr_price} > {threshold_price} / +0.2%). Batal.")
                        return False
                except Exception as e:
                    logging.warning(f"[SIMULASI] Gagal membaca harga ticker: {e}")
            else:
                try:
                    ticker = await self.request("GET", "/fapi/v1/ticker/price", {"symbol": self.symbol})
                    curr_price = Decimal(ticker["price"])
                    if curr_price > threshold_price:
                        logging.info(f"[ENTRY] Harga melesat naik ({curr_price} > {threshold_price} / +0.2%). Membatalkan order...")
                        filled_on_cancel = await self.cancel_entry_order()
                        return filled_on_cancel
                except Exception as e:
                    logging.warning(f"[ENTRY] Gagal membaca harga ticker: {e}")

            await asyncio.sleep(1)

        if not self.entry_filled_event.is_set():
            logging.info("[ENTRY] Timeout 30 detik tercapai. Memeriksa status akhir order...")
            if await self.recover_entry():
                return True

            logging.info("[ENTRY] Order belum terisi. Membatalkan order...")
            filled_on_cancel = await self.cancel_entry_order()
            return filled_on_cancel

        return True

    # ========================================================
    # LIMIT BUY (POST-ONLY / GTX)
    # ========================================================
    async def open_position(self, close_price):
        # Logika Opsi A: Floor Grid ke tick terdekat di bawah/sama dengan close_price
        floor_price = (close_price // self.price_tick) * self.price_tick
        if floor_price == close_price:
            floor_price -= self.price_tick
        entry_target_price = self.round_price(floor_price)

        notional = self.margin_usdt * Decimal(str(self.leverage))

        quantity = self.round_qty(notional / entry_target_price)

        if quantity < self.min_qty: raise RuntimeError(f"Quantity {quantity} di bawah minimum {self.min_qty}")

        actual_notional = quantity * entry_target_price

        if self.min_notional > 0 and actual_notional < self.min_notional:
            raise RuntimeError(f"Notional {actual_notional} di bawah minimum {self.min_notional}")

        self.entry_client_id = self.client_id("ENTRY")

        formatted_price = f"{entry_target_price:.{self.price_precision}f}"
        formatted_qty = f"{quantity:.{self.qty_precision}f}"

        logging.info("[ENTRY] BUY LIMIT (POST-ONLY / GTX)")
        logging.info(f"[ENTRY] Close Price   : {close_price}")
        logging.info(f"[ENTRY] Target Price  : {formatted_price} (Grid floor di bawah close)")
        logging.info(f"[ENTRY] Margin target : {self.margin_usdt} USDT/USDC")
        logging.info(f"[ENTRY] Notional      : {actual_notional:.8f}")
        logging.info(f"[ENTRY] Quantity      : {formatted_qty}")

        if not self.real_trading:
            logging.info("[SIMULASI] Memasang order virtual Post-Only...")
            self.entry_qty = Decimal(formatted_qty)
            self.entry_price = Decimal(formatted_price)
            self.entry_commission = Decimal("0")
            self.entry_order_id = "SIM_ENTRY_ID"

            await self.notifier.send(
                "🔵 [SIMULASI] LONG OPEN (POST-ONLY)",
                f"{self.symbol} memasang BUY Post-Only di {formatted_price}.",
                "BUY",
                {"Price": formatted_price, "Qty": str(self.entry_qty), "Margin": f"{self.margin_usdt}", "Leverage": f"{self.leverage}x"}
            )
            return await self.wait_and_monitor_entry_fill(entry_target_price)

        try:
            response = await self.request("POST", "/fapi/v1/order", {
                "symbol": self.symbol,
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTX",
                "quantity": formatted_qty,
                "price": formatted_price,
                "newClientOrderId": self.entry_client_id,
                "newOrderRespType": "RESULT"
            }, signed=True)

            self.entry_order_id = response.get("orderId")
            logging.info(f"[ENTRY] Order ID = {self.entry_order_id}")

            if response.get("status") == "FILLED":
                await self.process_entry_filled(response)
                return True

            return await self.wait_and_monitor_entry_fill(entry_target_price)

        except BinanceError as e:
            logging.error(f"[ENTRY] Order Post-Only ditolak/gagal oleh Binance (Code {e.code}): {e.msg}")
            await self.notifier.send("⚠️ ORDER ENTRY DITOLAK", f"Order Post-Only {self.symbol} ditolak: {e.msg}", "WARNING")
            return False

    # ========================================================
    # ENTRY FILLED
    # ========================================================
    async def process_entry_filled(self, order):
        executed_qty = Decimal(order.get("executedQty", order.get("cumQty", "0")))
        quote_qty = Decimal(order.get("cumQuote", order.get("cumQuoteQty", "0")))

        if executed_qty <= 0: raise RuntimeError("BUY FILLED tetapi executedQty = 0")

        self.entry_qty = executed_qty
        self.entry_price = quote_qty / executed_qty if quote_qty > 0 else Decimal(order.get("price", "0"))

        logging.info("[ENTRY FILLED]")
        logging.info(f"Qty   : {self.entry_qty}")
        logging.info(f"Price : {self.entry_price}")
        self.entry_filled_event.set()

    # ========================================================
    # LISTEN KEY
    # ========================================================
    async def create_listen_key(self):
        data = await self.request("POST", LISTEN_KEY_URL)
        self.listen_key = data["listenKey"]
        logging.info("[WS USER] ListenKey berhasil dibuat.")

    # ========================================================
    # USER STREAM
    # ========================================================
    async def user_stream(self):
        if not self.listen_key: await self.create_listen_key()

        url = USER_STREAM_URL + self.listen_key

        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            logging.info("[WS USER] Connected.")

            async for raw in ws:
                data = json.loads(raw)

                if data.get("e") != "ORDER_TRADE_UPDATE": continue

                order = data.get("o", {})
                if order.get("s") != self.symbol: continue

                client_id = order.get("c", "")
                status = order.get("X")

                if client_id == self.entry_client_id:
                    if status == "FILLED":
                        executed_qty = Decimal(order.get("z", "0"))
                        avg_price = Decimal(order.get("ap", "0"))

                        if executed_qty > 0:
                            self.entry_qty = executed_qty
                            self.entry_price = avg_price
                            self.entry_commission = Decimal("0")

                            logging.info("[WS] ENTRY FILLED")

                            await self.notifier.send("🔵 LONG OPEN", f"{self.symbol} berhasil BUY (Post-Only).", "BUY", {"Entry": self.entry_price, "Qty": self.entry_qty, "Margin": f"{self.margin_usdt}", "Leverage": f"{self.leverage}x"})

                            self.entry_filled_event.set()

                    continue

                if client_id == self.tp_client_id and status == "FILLED":
                    logging.info("[WS] TAKE PROFIT FILLED")
                    self.close_event_data = order
                    self.position_closed_event.set()

    # ========================================================
    # HITUNG TP NET (0% Fee Maker Promo USDC)
    # ========================================================
    def calculate_net_tp_price(self):
        qty = self.entry_qty
        entry = self.entry_price
        target = self.target_net_pnl
        return self.round_price(entry + (target / qty))

    # ========================================================
    # TAKE PROFIT (Post-Only LIMIT GTX Sell)
    # ========================================================
    async def place_take_profit(self, qty=None):
        target_qty = qty if qty is not None else self.entry_qty
        target_qty = self.round_qty(target_qty)

        if target_qty < self.min_qty:
            logging.warning(f"[TP] Kuantitas TP ({target_qty}) di bawah minimum {self.min_qty}. Batal memasang.")
            return False

        self.tp_price = self.calculate_net_tp_price()
        self.tp_client_id = self.client_id("TP")

        formatted_tp_price = f"{self.tp_price:.{self.price_precision}f}"
        formatted_qty = f"{target_qty:.{self.qty_precision}f}"

        logging.info(f"[TP] Target NET PnL = +{self.target_net_pnl}")
        logging.info(f"[TP] TP price (Post-Only) = {formatted_tp_price} | Qty = {formatted_qty}")

        if not self.real_trading:
            self.tp_order_id = "SIM_TP_ID"
            logging.info(f"[TP] Order ID (Simulasi) = {self.tp_order_id}")
            await self.notifier.send("🎯 [SIMULASI] TAKE PROFIT DIPASANG (POST-ONLY)", f"Target net PnL +{self.target_net_pnl}.", "INFO", {"Symbol": self.symbol, "Entry": str(self.entry_price), "TP": formatted_tp_price, "Qty": formatted_qty, "Target Net PnL": f"+{self.target_net_pnl}"})
            return True

        try:
            response = await self.request("POST", "/fapi/v1/order", {
                "symbol": self.symbol, 
                "side": "SELL", 
                "type": "LIMIT", 
                "timeInForce": "GTX",
                "quantity": formatted_qty, 
                "price": formatted_tp_price, 
                "newClientOrderId": self.tp_client_id,
                "newOrderRespType": "RESULT"
            }, signed=True)

            self.tp_order_id = response.get("orderId")
            logging.info(f"[TP] Order ID Post-Only = {self.tp_order_id}")

            await self.notifier.send("🎯 TAKE PROFIT DIPASANG (POST-ONLY)", f"Target net PnL +{self.target_net_pnl}.", "INFO", {"Symbol": self.symbol, "Entry": self.entry_price, "TP": formatted_tp_price, "Qty": formatted_qty, "Target Net PnL": f"+{self.target_net_pnl}"})
            return True

        except BinanceError as e:
            logging.error(f"[TP] Order Post-Only ditolak/gagal oleh Binance (Code {e.code}): {e.msg}")
            return False
        except Exception as e:
            logging.error(f"[TP] Error saat memasang order TP: {e}")
            return False

    # ========================================================
    # WAIT POSITION CLOSED
    # ========================================================
    async def wait_position_closed(self):
        logging.info("[POSITION] Menunggu TP Post-Only terisi...")

        if not self.real_trading:
            while True:
                try:
                    ticker = await self.request("GET", "/fapi/v1/ticker/price", {"symbol": self.symbol})
                    current_price = Decimal(ticker["price"])
                    
                    logging.info(f"[SIMULASI] Harga saat ini: {current_price} | Target TP: {self.tp_price}")
                    
                    if current_price >= self.tp_price:
                        logging.info(f"[SIMULASI] Harga menyentuh TP ({current_price} >= {self.tp_price})!")
                        self.position_closed_event.set()
                        return
                except Exception as e:
                    logging.warning(f"[SIMULASI] Gagal mengambil harga: {e}")
                
                await asyncio.sleep(2)

        while True:
            try:
                position = await self.get_position()

                if not position:
                    logging.info("[POSITION] Posisi sudah CLOSED.")
                    return

                pos_qty = abs(Decimal(position.get("positionAmt", "0")))
                if pos_qty == 0:
                    logging.info("[POSITION] Posisi sudah CLOSED.")
                    return

                # Memeriksa open orders untuk mendeteksi order SELL yang aktif
                open_orders = await self.request("GET", "/fapi/v1/openOrders", {"symbol": self.symbol}, signed=True)
                sell_qty = Decimal("0")
                for o in open_orders:
                    if o.get("side") == "SELL":
                        orig = Decimal(o.get("origQty", "0"))
                        executed = Decimal(o.get("executedQty", "0"))
                        sell_qty += (orig - executed)

                uncovered_qty = self.round_qty(pos_qty - sell_qty)

                if uncovered_qty >= self.min_qty:
                    logging.warning(f"[POSITION] Ditemukan posisi menggantung tanpa TP ({uncovered_qty} {self.symbol}). Memasang TP Post-Only ulang...")
                    await self.place_take_profit(qty=uncovered_qty)

            except Exception as e:
                logging.warning(f"[POSITION] Error saat memeriksa status posisi/order: {e}")

            await asyncio.sleep(2)

    # ========================================================
    # FINAL INCOME
    # ========================================================
    async def get_final_income(self):
        end_time = self.now()
        start_time = end_time - 15 * 60 * 1000

        data = await self.request("GET", "/fapi/v1/income", {"symbol": self.symbol, "startTime": start_time, "endTime": end_time, "limit": 100}, signed=True)

        realized = Decimal("0")
        commission = Decimal("0")
        funding = Decimal("0")

        for item in data:
            income_type = item.get("incomeType")
            value = Decimal(item.get("income", "0"))

            if income_type == "REALIZED_PNL": realized += value
            elif income_type == "COMMISSION": commission += value
            elif income_type == "FUNDING_FEE": funding += value

        return {"realized_pnl": realized, "commission": commission, "funding": funding, "net_pnl": realized + commission + funding}

    # ========================================================
    # FINAL REPORT
    # ========================================================
    async def final_report(self):
        if not self.real_trading:
            realized_pnl = (self.tp_price - self.entry_price) * self.entry_qty
            total_commission = Decimal("0")
            net_pnl = realized_pnl

            logging.info("================================================")
            logging.info("[SIMULASI FINAL RESULT]")
            logging.info(f"REALIZED PNL : {realized_pnl:.8f}")
            logging.info(f"COMMISSION   : {total_commission:.8f} (0% Maker)")
            logging.info(f"NET PNL      : {net_pnl:.8f}")
            logging.info("================================================")

            await self.notifier.send("🟢 [SIMULASI] TRANSAKSI SELESAI", f"{self.symbol} one-shot trade simulasi selesai.", "TP", {"Entry": str(self.entry_price), "TP": str(self.tp_price), "Realized PnL": f"{realized_pnl:.8f}", "Commission": "0.00000000", "NET PnL": f"{net_pnl:.8f}"})
            return

        result = await self.get_final_income()

        logging.info("================================================")
        logging.info("[FINAL RESULT]")
        logging.info(f"REALIZED PNL : {result['realized_pnl']:.8f}")
        logging.info(f"COMMISSION   : {result['commission']:.8f}")
        logging.info(f"FUNDING      : {result['funding']:.8f}")
        logging.info(f"NET PNL      : {result['net_pnl']:.8f}")
        logging.info("================================================")

        await self.notifier.send("🟢 TRANSAKSI SELESAI", f"{self.symbol} one-shot trade selesai.", "TP", {"Entry": self.entry_price, "TP": self.tp_price, "Realized PnL": f"{result['realized_pnl']:.8f}", "Commission": f"{result['commission']:.8f}", "Funding": f"{result['funding']:.8f}", "NET PnL": f"{result['net_pnl']:.8f}"})

    # ========================================================
    # RUN
    # ========================================================
    async def run(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

        try:
            logging.info("================================================")
            logging.info(" ONE-SHOT USDⓈ-M FUTURES EXECUTOR (POST-ONLY)")
            logging.info("================================================")
            logging.info(f"Symbol       : {self.symbol}")
            logging.info(f"Margin       : {self.margin_usdt}")
            logging.info(f"Leverage     : {self.leverage}x")
            logging.info(f"Timeframe    : {self.interval}")
            logging.info(f"Target NET   : +{self.target_net_pnl}")
            logging.info(f"Real Trading : {self.real_trading}")

            await self.sync_time()
            await self.load_exchange_rules()
            await self.ensure_single_asset_mode()
            await self.ensure_isolated()
            await self.set_leverage()
            await self.load_commission_rate()

            existing_position = await self.get_position()
            if existing_position: raise RuntimeError(f"Sudah ada posisi aktif pada {self.symbol}. Program dibatalkan.")

            if self.real_trading:
                await self.create_listen_key()
                user_task = asyncio.create_task(self.user_stream())

            await asyncio.sleep(1)
            close_price = await self.wait_for_candle_close()

            existing_position = await self.get_position()
            if existing_position: raise RuntimeError("Posisi muncul sebelum entry. Program dihentikan.")

            entry_success = await self.open_position(close_price)
            if not entry_success:
                logging.info("[RUN] Order Entry Post-Only tidak terisi atau dibatalkan. Sesi dihentikan.")
                return

            await self.place_take_profit()
            await self.wait_position_closed()
            await asyncio.sleep(1)
            await self.final_report()

            logging.info("[DONE] ONE-SHOT TRADE SELESAI.")

        finally:
            if self.listen_key and self.real_trading:
                try: await self.request("DELETE", LISTEN_KEY_URL, {"listenKey": self.listen_key})
                except Exception: pass

            if self.session: await self.session.close()

def parse_args():
    parser = argparse.ArgumentParser(description="One-Shot USDⓈ-M Futures Executor (Post-Only)")
    parser.add_argument("--symbol", type=str, default="XRPUSDC", help="Pasangan koin (contoh: XRPUSDC)")
    parser.add_argument("--margin", type=Decimal, default=Decimal("20"), help="Margin dalam USDT/USDC (contoh: 20)")
    parser.add_argument("--leverage", type=int, default=3, help="Leverage (contoh: 3)")
    parser.add_argument("--target", type=Decimal, default=Decimal("0.30"), help="Target Net PnL (contoh: 0.30)")
    parser.add_argument("--interval", type=str, default="15m", help="Interval candle (contoh: 1m, 5m, 15m, 30m, 1h)")
    parser.add_argument("--max-qty-precision", type=int, default=2, help="Batas maksimal desimal kuantitas (default: 2)")
    parser.add_argument("--real", action="store_true", help="Aktifkan trading riil (tanpa opsi ini = mode simulasi/aman)")
    return parser.parse_args()

async def main():
    args = parse_args()
    bot = FuturesOneShot(args)
    await bot.run()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: logging.info("BOT DIHENTIKAN USER.")
    except Exception as e: logging.exception(f"FATAL ERROR: {e}")