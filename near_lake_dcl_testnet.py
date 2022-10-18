import asyncio
import json
import os
from loguru import logger
from near_lake_framework import near_primitives, LakeConfig, streamer
from db_provider_dcl_testnet import add_swap, add_liquidity_added, add_liquidity_removed, add_lostfound
from db_provider_dcl_testnet import add_order_added, add_order_cancelled, add_order_completed, add_swap_desire
from db_provider_dcl_testnet import add_latest_actions
import base64


async def handle_streamer_message(streamer_message: near_primitives.StreamerMessage):
    block_id = streamer_message.block.header.height
    timestamp = streamer_message.block.header.timestamp
    account_id = "juaner.testnet"
    for shard in streamer_message.shards:
        # logger.info("shard:{}", shard)
        for receipt_execution_outcome in shard.receipt_execution_outcomes:
            logs = receipt_execution_outcome.execution_outcome.outcome.logs
            tx_id = receipt_execution_outcome.execution_outcome.id
            if "mock-dcl.ref-dev.testnet" == receipt_execution_outcome.receipt.receiver_id:
                handle_receiver_id(logs, tx_id, block_id, timestamp, "dev")
            if "dcl.ref-dev.testnet" == receipt_execution_outcome.receipt.receiver_id:
                handle_receiver_id(logs, tx_id, block_id, timestamp, "testnet")

            executor_id = receipt_execution_outcome.execution_outcome.outcome.executor_id
            status_data = receipt_execution_outcome.execution_outcome.outcome.status,
            for status_key in status_data[0].keys():
                status = status_key
            transaction_hash = receipt_execution_outcome.execution_outcome.id
            receiver_account_id = receipt_execution_outcome.receipt.receiver_id
            receipt_predecessor_account_id = receipt_execution_outcome.receipt.predecessor_id
            if account_id == executor_id or account_id == receiver_account_id or account_id == receipt_predecessor_account_id:
                handle_latest_actions(receipt_execution_outcome.receipt.receipt, transaction_hash, receiver_account_id,
                                      receipt_predecessor_account_id, timestamp, status)


def handle_latest_actions(receipt, transaction_hash, receiver_account_id, receipt_predecessor_account_id, timestamp, status):
    latest_actions_date_list = []
    args_list = handle_receipt_content(receipt)
    for args in args_list:
        latest_actions_date = {
            "transaction_hash": transaction_hash,
            "receiver_account_id": receiver_account_id,
            "receipt_predecessor_account_id": receipt_predecessor_account_id,
            "method_name": args["method_name"],
            "args": args["args"],
            "deposit": args["deposit"],
            "status": status,
            "timestamp": timestamp
        }
        latest_actions_date_list.append(latest_actions_date)
    logger.info("latest_actions_date_list:{}", latest_actions_date_list)
    add_latest_actions(latest_actions_date_list, "testnet")


def handle_receipt_content(content):
    res = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                deposit = action["FunctionCall"]["deposit"]
                method_name = action["FunctionCall"]["method_name"]
                args = str(json.loads(base64.b64decode(action["FunctionCall"]["args"])))
                arg_data = {
                    "method_name": method_name,
                    "args": args,
                    "deposit": deposit
                }
                res.append(arg_data)
    except Exception as e:
        logger.error("handle_receipt_content analysis error:{}", e)
        logger.error("handle_receipt_content content:{}", content)
    return res


def handle_receiver_id(logs, tx_id, block_id, timestamp, network):
    for log in logs:
        # logger.info("log:{}", log)
        if not log.startswith("EVENT_JSON:"):
            continue
        try:
            parsed_log = json.loads(log[len("EVENT_JSON:"):])
        except json.JSONDecodeError:
            logger.error("Error during parsing logs from JSON string to dict")
            logger.error("error content:{}", log)
            continue
        # if parsed_log.get("standard") != "dcl.ref" or parsed_log.get("event") != "swap":
        #     continue
        logger.info("EVENT_JSON parsed_log:{}", parsed_log)
        handle_log_content(parsed_log, tx_id, block_id, timestamp, network)


def handle_log_content(parsed_log, tx_id, block_id, timestamp, network):
    event = parsed_log.get("event")
    if "swap" == event:
        swap_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            swap_date = {
                "swapper": data["swapper"],
                "token_in": data["token_in"],
                "token_out": data["token_out"],
                "amount_in": data["amount_in"],
                "amount_out": data["amount_out"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            swap_date_list.append(swap_date)
        logger.info("swap_date_list:{}", swap_date_list)
        add_swap(swap_date_list, event, network)

    elif "swap_desire" == event:
        swap_desire_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            swap_desire_date = {
                "swapper": data["swapper"],
                "token_in": data["token_in"],
                "token_out": data["token_out"],
                "amount_in": data["amount_in"],
                "amount_out": data["amount_out"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            swap_desire_date_list.append(swap_desire_date)
        logger.info("swap_desire_date_list:{}", swap_desire_date_list)
        add_swap_desire(swap_desire_date_list, event, network)

    elif "liquidity_added" == event:
        liquidity_added_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            liquidity_added_date = {
                "lpt_id": data["lpt_id"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "left_point": data["left_point"],
                "right_point": data["right_point"],
                "added_amount": data["added_amount"],
                "cur_amount": data["cur_amount"],
                "paid_token_x": data["paid_token_x"],
                "paid_token_y": data["paid_token_y"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            liquidity_added_date_list.append(liquidity_added_date)
        logger.info("liquidity_added_date_list:{}", liquidity_added_date_list)
        add_liquidity_added(liquidity_added_date_list, event, network)

    elif "liquidity_removed" == event:
        liquidity_removed_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            liquidity_removed_date = {
                "lpt_id": data["lpt_id"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "left_point": data["left_point"],
                "right_point": data["right_point"],
                "removed_amount": data["removed_amount"],
                "cur_amount": data["cur_amount"],
                "refund_token_x": data["refund_token_x"],
                "refund_token_y": data["refund_token_y"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            liquidity_removed_date_list.append(liquidity_removed_date)
        logger.info("liquidity_removed_date_list:{}", liquidity_removed_date_list)
        add_liquidity_removed(liquidity_removed_date_list, event, network)

    elif "lostfound" == event:
        lostfound_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            lostfound_date = {
                "user": data["user"],
                "token": data["token"],
                "amount": data["amount"],
                "locked": data["locked"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            lostfound_date_list.append(lostfound_date)
        logger.info("lostfound_date_list:{}", lostfound_date_list)
        add_lostfound(lostfound_date_list, event, network)

    elif "order_added" == event:
        order_added_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            order_added_date = {
                "order_id": data["order_id"],
                "created_at": data["created_at"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "buy_token": data["buy_token"],
                "original_amount": data["original_amount"],
                "original_deposit_amount": data["original_deposit_amount"],
                "swap_earn_amount": data["swap_earn_amount"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            order_added_date_list.append(order_added_date)
        logger.info("order_added_date_list:{}", order_added_date_list)
        add_order_added(order_added_date_list, event, network)

    elif "order_cancelled" == event:
        order_cancelled_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            order_cancelled_date = {
                "order_id": data["order_id"],
                "created_at": data["created_at"],
                "cancel_at": data["cancel_at"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "buy_token": data["buy_token"],
                "request_cancel_amount": data["request_cancel_amount"],
                "actual_cancel_amount": data["actual_cancel_amount"],
                "original_amount": data["original_amount"],
                "cancel_amount": data["cancel_amount"],
                "remain_amount": data["remain_amount"],
                "bought_amount": data["bought_amount"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            order_cancelled_date_list.append(order_cancelled_date)
        logger.info("order_cancelled_date_list:{}", order_cancelled_date_list)
        add_order_cancelled(order_cancelled_date_list, event, network)

    elif "order_completed" == event:
        order_completed_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            order_completed_date = {
                "order_id": data["order_id"],
                "created_at": data["created_at"],
                "completed_at": data["completed_at"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "buy_token": data["buy_token"],
                "original_amount": data["original_amount"],
                "original_deposit_amount": data["original_deposit_amount"],
                "swap_earn_amount": data["swap_earn_amount"],
                "cancel_amount": data["cancel_amount"],
                "bought_amount": data["bought_amount"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp
            }
            order_completed_date_list.append(order_completed_date)
        logger.info("order_completed_date_list:{}", order_completed_date_list)
        add_order_completed(order_completed_date_list, event, network)


async def main():
    config = LakeConfig.testnet()
    config.start_block_height = 100068635
    config.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "XXXxxx")
    config.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "XXXxxx")

    stream_handle, streamer_messages_queue = streamer(config)
    while True:
        streamer_message = await streamer_messages_queue.get()
        print(f"Block #{streamer_message.block.header.height} Shards: {len(streamer_message.shards)}")
        await handle_streamer_message(streamer_message)


logger.add("near_lake_dcl.log")
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
