import decimal
import pymysql
import json
from datetime import datetime as datatime
from loguru import logger


class Encoder(json.JSONEncoder):
    """
    Handle special data types, such as decimal and time types
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        if isinstance(o, datatime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        super(Encoder, self).default(o)


def get_db_connect(network):
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="xxx",
        passwd="xxx",
        db="xxx")
    return conn


def add_swap(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(swapper, token_in, token_out, amount_in, amount_out, tx_id, " \
          "block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["swapper"], data["token_in"], data["token_out"], data["amount_in"],
                                data["amount_out"], data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert swap log to db error:{}", e)
        logger.error("insert swap log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_swap_desire(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(swapper, token_in, token_out, amount_in, amount_out, tx_id, " \
          "block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["swapper"], data["token_in"], data["token_out"], data["amount_in"],
                                data["amount_out"], data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert swap_desire log to db error:{}", e)
        logger.error("insert swap_desire log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_liquidity_added(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(lpt_id, owner_id, pool_id, left_point, right_point, added_amount, " \
          "cur_amount, paid_token_x, paid_token_y, tx_id, block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["lpt_id"], data["owner_id"], data["pool_id"], data["left_point"],
                                data["right_point"], data["added_amount"], data["cur_amount"], data["paid_token_x"],
                                data["paid_token_y"], data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert liquidity_added log to db error:{}", e)
        logger.error("insert liquidity_added log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_liquidity_removed(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(lpt_id, owner_id, pool_id, left_point, right_point, removed_amount, " \
          "cur_amount, refund_token_x, refund_token_y, tx_id, block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["lpt_id"], data["owner_id"], data["pool_id"], data["left_point"],
                                data["right_point"], data["removed_amount"], data["cur_amount"], data["refund_token_x"],
                                data["refund_token_y"], data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert liquidity_removed log to db error:{}", e)
        logger.error("insert liquidity_removed log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_lostfound(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(user, token, amount, locked, tx_id, " \
          "block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["user"], data["token"], data["amount"], data["locked"],
                                data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert lostfound log to db error:{}", e)
        logger.error("insert lostfound log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_order_added(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(order_id, created_at, owner_id, pool_id, point, sell_token, " \
          "buy_token, original_amount, original_deposit_amount, swap_earn_amount, " \
          "tx_id, block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["order_id"], data["created_at"], data["owner_id"], data["pool_id"],
                                data["point"], data["sell_token"], data["buy_token"], data["original_amount"],
                                data["original_deposit_amount"], data["swap_earn_amount"],
                                data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert order_added log to db error:{}", e)
        logger.error("insert order_added log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_order_cancelled(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(order_id, created_at, cancel_at, owner_id, pool_id, point, sell_token, " \
          "buy_token, request_cancel_amount, actual_cancel_amount, original_amount, cancel_amount, remain_amount, " \
          "bought_amount, tx_id, block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["order_id"], data["created_at"], data["cancel_at"], data["owner_id"], data["pool_id"],
                                data["point"], data["sell_token"], data["buy_token"], data["request_cancel_amount"],
                                data["actual_cancel_amount"], data["original_amount"], data["cancel_amount"],
                                data["remain_amount"], data["bought_amount"],
                                data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert order_cancelled log to db error:{}", e)
        logger.error("insert order_cancelled log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_order_completed(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(order_id, created_at, completed_at, owner_id, pool_id, point, " \
          "sell_token, buy_token, original_amount, original_deposit_amount, swap_earn_amount, cancel_amount, " \
          "bought_amount, tx_id, block_id, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["order_id"], data["created_at"], data["completed_at"], data["owner_id"],
                                data["pool_id"], data["point"], data["sell_token"], data["buy_token"],
                                data["original_amount"], data["original_deposit_amount"], data["swap_earn_amount"],
                                data["cancel_amount"], data["bought_amount"],
                                data["tx_id"], data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert order_completed log to db error:{}", e)
        logger.error("insert order_completed log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def add_latest_actions(data_list, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_latest_actions(transaction_hash, receiver_account_id, receipt_predecessor_account_id, " \
          "method_name, args, deposit, status, timestamp, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["transaction_hash"], data["receiver_account_id"], data["receipt_predecessor_account_id"],
                                data["method_name"], data["args"], data["deposit"], data["status"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert latest_actions log to db error:{}", e)
        logger.error("insert latest_actions log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


if __name__ == '__main__':
    logger.info("#########MAINNET###########")
    account_data_list = [
        {'swapper': 'juaner.testnet',
         'token_in': 'willa.fakes.testnet',
         'token_out': 'wrap.testnet',
         'amount_in': '3685473573416472',
         'amount_out': '330984447402029415454',
         'tx_id': '661WcPqWBtuSb18PnYPnh5QC2YCPyAi8MiHvApxdtar4',
         'block_id': 99418881,
         'timestamp': 1662350936892168204}
    ]
    add_swap(account_data_list, "swap")


