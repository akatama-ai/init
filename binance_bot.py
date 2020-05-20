"""
    Подробная информация о боте на сайте bablofil.ru/bot-dlya-binance
"""
#  -*- coding: utf-8 -*-


import sqlite3
import logging
import time
import os

from datetime import datetime



from binance_api import Binance
bot = Binance(
    API_KEY='...',
    API_SECRET='...'
)

"""
     Write down the pairs that will be traded.
     base - this is the base pair (BTC, ETH, BNB, USDT) - what is written on the banner in the table above
     quote is a quota currency. For example, for trading on the NEO / USDT pair, the base currency is USDT, NEO is the quota
"""

pairs = [
   {
        'base': 'BTC',
        'quote': 'EOS',
        'offers_amount': 5, # How many offers from a glass we take to calculate the average price
                            # Maximum 1000. The following values are allowed: [5, 10, 20, 50, 100, 500, 1000]
        'spend_sum': 0.001,  # How much to spend base every time you buy a quote
        'profit_markup': 0.005, # What kind of fat is needed with each transaction? (0.001 = 0.1%)
        'use_stop_loss': False, # Do I need to sell at a loss when the price drops
        'stop_loss': 1, # 1% - How much should the price fall to sell at a loss
    },
    {
        'base': 'BTC',
        'quote': 'ADA',
        'offers_amount': 5, # How many offers from a glass we take to calculate the average price
                            # Maximum 1000. The following values are allowed: [5, 10, 20, 50, 100, 500, 1000]
        'spend_sum': 0.001,  # How much to spend base every time you buy a quote
        'profit_markup': 0.005, # What kind of fat is needed with each transaction? (0.001 = 0.1%)
        'use_stop_loss': False, # Do I need to sell at a loss when the price drops
        'stop_loss': 1, # 1% - How much should the price fall to sell at a loss
    },
    {
        'base': 'BTC',
        'quote': 'XRP',
        'offers_amount': 5, # How many offers from a glass we take to calculate the average price
                            # Maximum 1000. The following values are allowed: [5, 10, 20, 50, 100, 500, 1000]
        'spend_sum': 0.001,  # How much to spend base every time you buy a quote
        'profit_markup': 0.005, # What kind of fat is needed with each transaction? (0.001 = 0.1%)
        'use_stop_loss': False, # Do I need to sell at a loss when the price drops
        'stop_loss': 1, # 1% - How much should the price fall to sell at a loss
    }
]



BUY_LIFE_TIME_SEC = 180 # How many (in seconds) to keep a sell order open

STOCK_FEE = 0.00075 # Commission taken by the exchange (0.001 = 0.1%)

# If you decide not to pay a fee in BNB, then set to False. This is usually not necessary.
USE_BNB_FEES = True

# We get trading restrictions for all pairs from the exchange
local_time = int(time.time())
limits = bot.exchangeInfo()
server_time = int(limits['serverTime'])//1000

# F-tion, which leads any number to a multiple of the step indicated by the exchange
#If you pass the parameter increase = True, then rounding will occur to the next step
def adjust_to_step(value, step, increase=False):
   return ((int(value * 100000000) - int(value * 100000000) % int(
        float(step) * 100000000)) / 100000000)+(float(step) if increase else 0)

logging.basicConfig(
    format="%(asctime)s [%(levelname)-5.5s] %(message)s",
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler("{path}/logs/{fname}.log".format(path=os.path.dirname(os.path.abspath(__file__)), fname="binance")),
        logging.StreamHandler()
    ])

log = logging.getLogger('')
# Endless program loop

shift_seconds = server_time-local_time
bot.set_shift_seconds(shift_seconds)

log.debug("""
    Current time: {local_time_d} {local_time_u}
    Server time: {server_time_d} {server_time_u}
    Difference: {diff:0.8f} {warn}
    The bot will work as if now: {fake_time_d} {fake_time_u}
""".format(
    local_time_d = datetime.fromtimestamp(local_time), local_time_u=local_time,
    server_time_d=datetime.fromtimestamp(server_time), server_time_u=server_time,
    diff=abs(local_time-server_time),
    warn="Current time ВЫШЕ" if local_time > server_time else '',
    fake_time_d=datetime.fromtimestamp(local_time+shift_seconds), fake_time_u=local_time+shift_seconds
))

while True:
    try:
        # Establish a connection to the local database
        conn = sqlite3.connect('binance.db')
        cursor = conn.cursor()

        # If no tables exist, they need to be created (first run)
        orders_q = """
          create table if not exists
            orders (
              order_type TEXT,
              order_pair TEXT,

              buy_order_id NUMERIC,
              buy_amount REAL,
              buy_price REAL,
              buy_created DATETIME,
              buy_finished DATETIME NULL,
              buy_cancelled DATETIME NULL,

              sell_order_id NUMERIC NULL,
              sell_amount REAL NULL,
              sell_price REAL NULL,
              sell_created DATETIME NULL,
              sell_finished DATETIME NULL,
              force_sell INT DEFAULT 0
            );
        """
        cursor.execute(orders_q)

        log.debug("We receive all outstanding orders on the database")

        orders_q = """
            SELECT
              CASE WHEN order_type='buy' THEN buy_order_id ELSE sell_order_id END order_id
              , order_type
              , order_pair
              , sell_amount
              , sell_price
              ,  strftime('%s',buy_created)
              , buy_amount
              , buy_price
            FROM
              orders
            WHERE
              buy_cancelled IS NULL AND CASE WHEN order_type='buy' THEN buy_finished IS NULL ELSE sell_finished IS NULL END
        """
        orders_info = {}


        for row in cursor.execute(orders_q):
            orders_info[str(row[0])] = {'order_type': row[1], 'order_pair': row[2], 'sell_amount': row[3], 'sell_price': row[4],
                                         'buy_created': row[5], 'buy_amount': row[6], 'buy_price': row[7] }
        # we form the dictionary from the specified pairs, for easy access
        all_pairs = {pair['quote'].upper() + pair['base'].upper():pair for pair in pairs}
        if orders_info:
            log.debug("Received outstanding orders from the database: {orders}".format(orders=[(order, orders_info[order]['order_pair']) for order in orders_info]))

            # We check each order that is not executed on the base
            for order in orders_info:
                #We receive the latest exchange information on the order
                stock_order_data = bot.orderInfo(symbol=orders_info[order]['order_pair'], orderId=order)

                order_status = stock_order_data['status']
                log.debug("Order Status {order} - {status}".format(order=order, status=order_status))
                if order_status == 'NEW':
                    log.debug('Order {order} still not done'.format(order=order))

                # If a buy order
                if orders_info[order]['order_type'] == 'buy':
                    # If the order is already executed
                    if order_status == 'FILLED':
                        log.info("""
                            Order {order} completed, received {exec_qty:0.8f}.
                            Create a sell order
                        """.format(
                            order=order, exec_qty=float(stock_order_data['executedQty'])
                        ))

                        # look at the restrictions for creating a sell order
                        for elem in limits['symbols']:
                            if elem['symbol'] == orders_info[order]['order_pair']:
                                CURR_LIMITS = elem
                                break
                        else:
                            raise Exception("Не удалось найти настройки выбранной пары " + pair_name)

                        # We calculate data for a sell order

                        # Available qty for sale
                        has_amount = orders_info[order]['buy_amount']*((1-STOCK_FEE) if not USE_BNB_FEES else 1)
                        # Reduce the number of sales to a multiple of the restriction
                        sell_amount = adjust_to_step(has_amount, CURR_LIMITS['filters'][2]['stepSize'])
                        # We calculate the minimum amount you need to receive in order to stay in the black.
                        need_to_earn = orders_info[order]['buy_amount']*orders_info[order]['buy_price']*(1+all_pairs[stock_order_data['symbol']]['profit_markup'])
                        # We calculate the minimum price for sale
                        min_price = (need_to_earn/sell_amount)/((1-STOCK_FEE) if not USE_BNB_FEES else 1)
                        # We bring to the desired form, if the price after cutting off the extra characters is less than the necessary, increase by a step
                        cut_price = max(
                            adjust_to_step(min_price, CURR_LIMITS['filters'][0]['tickSize'], increase=True),
                            adjust_to_step(min_price, CURR_LIMITS['filters'][0]['tickSize'])
                        )
                        # Get current exchange rates
                        curr_rate = float(bot.tickerPrice(symbol=orders_info[order]['order_pair'])['price'])
                        # If the current price is higher than necessary, we sell at the current
                        need_price = max(cut_price, curr_rate)

                        log.info("""
                            Was originally purchased {buy_initial:0.8f}, net of commission {has_amount:0.8f},
                            Only sell {sell_amount:0.8f}
                            You need to get at least {need_to_earn:0.8f} {curr}
                            Min price (with commission) will be {min_price}, after casting {cut_price:0.8f}
                            Current market price {curr_rate:0.8f}
                            Total sale price: {need_price:0.8f}
                        """.format(
                            buy_initial=orders_info[order]['buy_amount'], has_amount=has_amount,sell_amount=sell_amount,
                            need_to_earn=need_to_earn, curr=all_pairs[orders_info[order]['order_pair']]['base'],
                            min_price=min_price, cut_price=cut_price, need_price=need_price,
                            curr_rate=curr_rate
                        ))

                        # Если итоговая сумма продажи меньше минимума, ругаемся и не продаем
                        if (need_price*has_amount) <float(CURR_LIMITS['filters'][3]['minNotional']):
                            raise Exception("""
                                Total transaction size {trade_am:0.8f} less than acceptable for the pair {min_am:0.8f}. """.format(
                                trade_am=(need_price*has_amount), min_am=float(CURR_LIMITS['filters'][3]['minNotional'])
                            ))

                        log.debug(
                            'Sell order calculated: qty {amount:0.8f}, course: {rate:0.8f}'.format(
                                amount=sell_amount, rate=need_price)
                        )

                        # We send a command to create an order with the calculated parameters
                        new_order = bot.createOrder(
                            symbol=orders_info[order]['order_pair'],
                            recvWindow=5000,
                            side='SELL',
                            type='LIMIT',
                            timeInForce='GTC',  # Good Till Cancel
                            quantity="{quantity:0.{precision}f}".format(
                                quantity=sell_amount, precision=CURR_LIMITS['baseAssetPrecision']
                            ),
                            price="{price:0.{precision}f}".format(
                                price=need_price, precision=CURR_LIMITS['baseAssetPrecision']
                            ),
                            newOrderRespType='FULL'
                        )
                        # If the order was created without errors, write the data to the database
                        if 'orderId' in new_order:
                            log.info("Создан ордер на продажу {new_order}".format(new_order=new_order))
                            cursor.execute(
                                """
                                  UPDATE orders
                                  SET
                                    order_type = 'sell',
                                    buy_finished = datetime(),
                                    sell_order_id = :sell_order_id,
                                    sell_created = datetime(),
                                    sell_amount = :sell_amount,
                                    sell_price = :sell_initial_price
                                  WHERE
                                    buy_order_id = :buy_order_id

                                """, {
                                    'buy_order_id': order,
                                    'sell_order_id': new_order['orderId'],
                                    'sell_amount': sell_amount,
                                    'sell_initial_price': need_price
                                }
                            )
                            conn.commit()
                        # If there were errors during creation, we display a message
                        else:
                            log.warning("Failed to create a sell order. {new_order}".format(new_order=new_order))

                    # The order has not yet been executed, there is no partial execution, we check the possibility of cancellation
                    elif order_status == 'NEW':
                        order_created = int(orders_info[order]['buy_created'])
                        time_passed = int(time.time()) - order_created
                        log.debug("Elapsed time after creation {passed:0.2f}".format(passed=time_passed))
                        # More time than allowed to hold an order
                        if time_passed > BUY_LIFE_TIME_SEC:
                            log.info("""Order {order} its time to cancel, its been {passed:0.1f} сек.""".format(
                                order=order, passed=time_passed
                            ))
                            # Отменяем ордер на бирже
                            cancel = bot.cancelOrder(
                                symbol=orders_info[order]['order_pair'],
                                orderId=order
                            )
                            # Если удалось отменить ордер, скидываем информацию в БД
                            if 'orderId' in cancel:
                                
                                log.info("Order {order} was successfully canceled".format(order=order))
                                cursor.execute(
                                    """
                                      UPDATE orders
                                      SET
                                        buy_cancelled = datetime()
                                      WHERE
                                        buy_order_id = :buy_order_id
                                    """, {
                                        'buy_order_id': order
                                    }
                                 )
                                
                                conn.commit()
                            else:
                                log.warning("Failed to cancel order: {cancel}".format(cancel=cancel))
                    elif order_status == 'PARTIALLY_FILLED':
                        log.debug("The order {order} is partially executed, we are waiting for completion".format(order=order))

                # Если это ордер на продажу, и он исполнен
                if order_status == 'FILLED' and orders_info[order]['order_type'] == 'sell':
                    log.debug("Order {order} for sale is executed".format(
                        order=order
                    ))
                    # Обновляем информацию в БД
                    cursor.execute(
                        """
                          UPDATE orders
                          SET
                            sell_finished = datetime()
                          WHERE
                            sell_order_id = :sell_order_id

                        """, {
                            'sell_order_id': order
                        }
                    )
                    conn.commit()
                if all_pairs[orders_info[order]['order_pair']]['use_stop_loss']:
                   
                   if order_status == 'NEW' and orders_info[order]['order_type'] == 'sell':
                     curr_rate = float(bot.tickerPrice(symbol=orders_info[order]['order_pair'])['price'])
                     
                     if (1 - curr_rate/orders_info[order]['buy_price'])*100 >= all_pairs[orders_info[order]['order_pair']]['stop_loss']:
                        log.debug("{pair} The price fell to stoploss (bought at {b:0.8f}, now {s:0.8f}), it's time to sell".format(
                           pair=orders_info[order]['order_pair'],
                           b=orders_info[order]['buy_price'],
                           s=curr_rate
                        ))
                        # Отменяем ордер на бирже
                        cancel = bot.cancelOrder(
                          symbol=orders_info[order]['order_pair'],
                             orderId=order
                         )
                        # Если удалось отменить ордер, скидываем информацию в БД
                        if 'orderId' in cancel:
                           log.info("The order {order} was successfully canceled, we sell by market".format(order=order))
                           new_order = bot.createOrder(
                                  symbol=orders_info[order]['order_pair'],
                                  recvWindow=15000,
                                  side='SELL',
                                  type='MARKET',
                                  quantity=orders_info[order]['sell_amount'],
                            )
                           if not new_order.get('code'):
                              log.info("Market Sell Order Created" + str(new_order))
                              cursor.execute(
                                 """
                                   DELETE FROM orders
                                   WHERE
                                     sell_order_id = :sell_order_id
                                 """, {
                                     'sell_order_id': order
                                 }
                              )
                              conn.commit()
                        else:
                           log.warning("Failed to cancel order: {cancel}".format(cancel=cancel))
                     else:
                         log.debug("{pair} (bought by {b:0.8f}, now {s:0.8f}), discrepancy {sl:0.4f}%, panic_sell = {ps:0.4f}% ({ps_rate:0.8f}), sale at a profit: {tp:0.8f}".format(
                           pair=orders_info[order]['order_pair'],
                           b=orders_info[order]['buy_price'],
                           s=curr_rate,
                           sl=(1 - curr_rate/orders_info[order]['buy_price'])*100,
                           ps=all_pairs[orders_info[order]['order_pair']]['stop_loss'],
                           ps_rate=orders_info[order]['buy_price']/100 * (100-all_pairs[orders_info[order]['order_pair']]['stop_loss']),
                           tp=orders_info[order]['sell_price']
                        ))
                   
                   elif order_status == 'CANCELED' and orders_info[order]['order_type'] == 'sell':
                     # На случай, если после отмены произошел разрыв связи
                     new_order = bot.createOrder(
                                  symbol=orders_info[order]['order_pair'],
                                  recvWindow=15000,
                                  side='SELL',
                                  type='MARKET',
                                  quantity=orders_info[order]['sell_amount'],
                            )
                     if not new_order.get('code'):
                        log.info("Market Sell Order Created " + str(new_order))
                        cursor.execute(
                           """
                             DELETE FROM orders
                             WHERE
                               sell_order_id = :sell_order_id
                           """, {
                               'sell_order_id': order
                           }
                        )
                        conn.commit()
        else:
            log.debug("There are no outstanding orders in the database")

        log.debug('We get from the settings all pairs for which there are no outstanding orders')

        orders_q = """
            SELECT
              distinct(order_pair) pair
            FROM
              orders
            WHERE
              buy_cancelled IS NULL AND CASE WHEN order_type='buy' THEN buy_finished IS NULL ELSE sell_finished IS NULL END
        """
        # Получаем из базы все ордера, по которым есть торги, и исключаем их из списка, по которому будем создавать новые ордера
        for row in cursor.execute(orders_q):
            del all_pairs[row[0]]

        # Если остались пары, по которым нет текущих торгов
        if all_pairs:
            log.debug('Found pairs for which there are no outstanding orders: {pairs}'.format(pairs=list(all_pairs.keys())))
            for pair_name, pair_obj in all_pairs.items():
                log.debug("Working with a couple {pair}".format(pair=pair_name))

                # Получаем лимиты пары с биржи
                for elem in limits['symbols']:
                    if elem['symbol'] == pair_name:
                        CURR_LIMITS = elem
                        break
                else:
                    raise Exception("Не удалось найти настройки выбранной пары " + pair_name)

                # Получаем балансы с биржи по указанным валютам
                balances = {
                    balance['asset']: float(balance['free']) for balance in bot.account()['balances']
                    if balance['asset'] in [pair_obj['base'], pair_obj['quote']]
                }
                log.debug("Balance {balance}".format(balance=["{k}:{bal:0.8f}".format(k=k, bal=balances[k]) for k in balances]))
                # Если баланс позволяет торговать - выше лимитов биржи и выше указанной суммы в настройках
                if balances[pair_obj['base']] >= pair_obj['spend_sum']:
                    # Получаем информацию по предложениям из стакана, в кол-ве указанном в настройках
                    offers = bot.depth(
                        symbol=pair_name,
                        limit=pair_obj['offers_amount']
                    )

                    # Берем цены покупок (для цен продаж замените bids на asks)
                    prices = [float(bid[0]) for bid in offers['bids']]

                    try:
                        # Рассчитываем среднюю цену из полученных цен
                        avg_price = sum(prices) / len(prices)
                        # Среднюю цену приводим к требованиям биржи о кратности
                        my_need_price = adjust_to_step(avg_price, CURR_LIMITS['filters'][0]['tickSize'])
                        # Рассчитываем кол-во, которое можно купить, и тоже приводим его к кратному значению
                        my_amount = adjust_to_step(pair_obj['spend_sum']/ my_need_price, CURR_LIMITS['filters'][2]['stepSize'])
                        # Если в итоге получается объем торгов меньше минимально разрешенного, то ругаемся и не создаем ордер
                        if my_amount < float(CURR_LIMITS['filters'][2]['stepSize']) or my_amount < float(CURR_LIMITS['filters'][2]['minQty']):
                            log.warning("""
                                Minimum Lot Amount: {min_lot:0.8f}
                                Minimum lot step: {min_lot_step:0.8f}
                                With our money we could buy {wanted_amount:0.8f}
                                After reducing to the minimum step, we can buy {my_amount:0.8f}
                                No purchase, exit. Increase bet size
                            """.format(
                                wanted_amount=pair_obj['spend_sum']/ my_need_price,
                                my_amount=my_amount,
                                min_lot=float(CURR_LIMITS['filters'][2]['minQty']),
                                min_lot_step=float(CURR_LIMITS['filters'][2]['stepSize'])
                            ))
                            continue

                        # Итоговый размер лота
                        trade_am = my_need_price*my_amount
                        log.debug("""
                                average price {av_price:0.8f}, 
                                after casting {need_price:0.8f}, 
                                volume after cast {my_amount:0.8f},
                                final transaction size {trade_am:0.8f}
                                """.format(
                            av_price=avg_price, need_price=my_need_price, my_amount=my_amount, trade_am=trade_am
                        ))
                        # Если итоговый размер лота меньше минимального разрешенного, то ругаемся и не создаем ордер
                        if trade_am < float(CURR_LIMITS['filters'][3]['minNotional']):
                            raise Exception("""
                                Total transaction size {trade_am:0.8f} меньше допустимого по паре {min_am:0.8f}. 
                                Increase bid amount (в {incr} раз(а))""".format(
                                trade_am=trade_am, min_am=float(CURR_LIMITS['filters'][3]['minNotional']),
                                incr=float(CURR_LIMITS['filters'][3]['minNotional'])/trade_am
                            ))
                        log.debug(
                            'A buy order has been calculated: qty {amount:0.8f}, course: {rate:0.8f}'.format(amount=my_amount, rate=my_need_price)
                        )
                        # Отправляем команду на бирже о создании ордера на покупку с рассчитанными параметрами
                        new_order = bot.createOrder(
                            symbol=pair_name,
                            recvWindow=5000,
                            side='BUY',
                            type='LIMIT',
                            timeInForce='GTC',  # Good Till Cancel
                            quantity="{quantity:0.{precision}f}".format(
                                quantity=my_amount, precision=CURR_LIMITS['baseAssetPrecision']
                            ),
                            price="{price:0.{precision}f}".format(
                                price=my_need_price, precision=CURR_LIMITS['baseAssetPrecision']
                            ),
                            newOrderRespType='FULL'
                        )
                        # Если удалось создать ордер на покупку, записываем информацию в БД
                        if 'orderId' in new_order:
                            log.info("Purchase order created {new_order}".format(new_order=new_order))
                            cursor.execute(
                                """
                                  INSERT INTO orders(
                                      order_type,
                                      order_pair,
                                      buy_order_id,
                                      buy_amount,
                                      buy_price,
                                      buy_created

                                  ) Values (
                                    'buy',
                                    :order_pair,
                                    :order_id,
                                    :buy_order_amount,
                                    :buy_initial_price,
                                    datetime()
                                  )
                                """, {
                                    'order_pair': pair_name,
                                    'order_id': new_order['orderId'],
                                    'buy_order_amount': my_amount,
                                    'buy_initial_price': my_need_price
                                }
                            )
                            conn.commit()
                        else:
                            log.warning("Failed to create purchase order.! {new_order}".format(new_order=str(new_order)))

                    except ZeroDivisionError:
                        log.debug('Unable to calculate average price: {prices}'.format(prices=str(prices)))
                else:
                    log.warning('To create a buy order you need a minimum {min_qty:0.8f} {curr}, output'.format(
                        min_qty=pair_obj['spend_sum'], curr=pair_obj['base']
                    ))

        else:
            log.debug('All pairs have outstanding orders')
        time.sleep(10)
    except Exception as e:
        log.exception(e)
    finally:
        conn.close()

