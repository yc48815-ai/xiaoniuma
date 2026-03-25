#!/usr/bin/env python3
"""
从已加入的群组读取成员，按固定间隔将成员添加到你的 Telegram 通讯录（使用用户账号，非 Bot）。

首次运行会在终端提示输入登录验证码（及两步验证密码，若已开启）。
状态保存在 state.json，中断后可继续，不会重复添加已处理的用户。

使用前请确保：仅在你有权使用的场景下使用，并遵守 Telegram 服务条款与当地法规。
"""

from __future__ import annotations

import asyncio
import random
import json
import logging
import os
import sys
import time
from pathlib import Path
from collections import deque
from urllib.parse import urlparse

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.functions.contacts import (
    AddContactRequest,
    DeleteContactsRequest,
    GetContactsRequest,
)
from telethon.tl.types import InputUser, User

# 固定从脚本所在目录加载 .env，避免在别的目录执行 python 时读不到配置
_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).resolve().parent / "state.json"
SESSION_NAME = Path(__file__).resolve().parent / "user_session"


def _load_state() -> dict:
    if not STATE_FILE.is_file():
        return {
            "done_ids": [],
            "failed": {},
            "channel_seen_ids": [],
            "channel_left_ids": [],
            "channel_last_stats_ts": 0.0,
            "channel_stats_history": [],
        }
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        data.setdefault("done_ids", [])
        data.setdefault("failed", {})
        data.setdefault("channel_seen_ids", [])
        data.setdefault("channel_left_ids", [])
        data.setdefault("channel_last_stats_ts", 0.0)
        data.setdefault("channel_stats_history", [])
        return data
    except (json.JSONDecodeError, OSError):
        return {
            "done_ids": [],
            "failed": {},
            "channel_seen_ids": [],
            "channel_left_ids": [],
            "channel_last_stats_ts": 0.0,
            "channel_stats_history": [],
        }


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _require_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        logger.error("请在 .env 中设置 %s（可参考 env.example）", name)
        sys.exit(1)
    return v


def _proxy_from_url(url: str):
    """
    Telethon 只接受 tuple / dict，不接受 URL 字符串。
    约定见 telethon.network.connection.Connection._parse_proxy：
    (type, host, port) 或 (type, host, port, rdns, username, password)
    """
    u = urlparse(url)
    scheme = (u.scheme or "socks5").lower()
    if scheme in ("socks", "socks5"):
        ptype = "socks5"
    elif scheme == "socks4":
        ptype = "socks4"
    elif scheme in ("http", "https"):
        ptype = "http"
    else:
        logger.error("不支持的 PROXY_URL 协议: %s（示例: socks5://127.0.0.1:7890）", scheme)
        sys.exit(1)

    host = u.hostname
    if not host:
        logger.error("PROXY_URL 缺少主机名（示例: socks5://127.0.0.1:7890）")
        sys.exit(1)

    if u.port is not None:
        port = u.port
    else:
        port = 7890 if ptype == "socks5" else 80

    user = u.username
    pwd = u.password
    if user is not None and pwd is not None:
        return (ptype, host, port, True, user, pwd)
    return (ptype, host, port)


def _build_proxy():
    """
    支持两种代理配置方式：
    1) PROXY_URL=socks5://127.0.0.1:7890
    2) PROXY_HOST=127.0.0.1 + PROXY_PORT=7890 + PROXY_TYPE=socks5
    """
    proxy_url = os.getenv("PROXY_URL", "").strip()
    if proxy_url:
        return _proxy_from_url(proxy_url)

    host = os.getenv("PROXY_HOST", "").strip()
    if not host:
        return None

    port_raw = os.getenv("PROXY_PORT", "").strip() or "7890"
    try:
        port = int(port_raw)
    except ValueError:
        logger.error("PROXY_PORT 不是有效整数: %s", port_raw)
        sys.exit(1)

    ptype = os.getenv("PROXY_TYPE", "socks5").strip().lower()
    username = os.getenv("PROXY_USERNAME", "").strip() or None
    password = os.getenv("PROXY_PASSWORD", "").strip() or None
    if username and password:
        return (ptype, host, port, True, username, password)
    return (ptype, host, port)


async def _build_queue(
    client: TelegramClient,
    entity,
    me_id: int,
    skip_bots: bool,
    require_username: bool,
    done: set[int],
    left: set[int],
) -> list[User]:
    users: list[User] = []
    async for u in client.iter_participants(entity):
        if not isinstance(u, User):
            continue
        if u.id == me_id:
            continue
        if u.id in done:
            continue
        if u.id in left:
            continue
        if skip_bots and u.bot:
            continue
        if require_username and not (u.username and u.username.strip()):
            continue
        if u.access_hash is None:
            logger.warning("跳过 user_id=%s（无 access_hash）", u.id)
            continue
        users.append(u)
    return users


async def _add_one(client: TelegramClient, user: User) -> None:
    fn = (user.first_name or "").strip() or "User"
    ln = (user.last_name or "").strip()
    await client(
        AddContactRequest(
            id=InputUser(user_id=user.id, access_hash=user.access_hash),
            first_name=fn,
            last_name=ln,
            phone="",
            add_phone_privacy_exception=False,
        )
    )


async def _fetch_current_participant_ids(
    client: TelegramClient,
    entity,
    me_id: int,
    skip_bots: bool,
) -> set[int]:
    """
    获取当前目标群/频道的成员 id 集合（用于判断哪些“离开订阅的人”需要清理）。
    """
    ids: set[int] = set()
    async for u in client.iter_participants(entity):
        if not isinstance(u, User):
            continue
        if u.id == me_id:
            continue
        if skip_bots and u.bot:
            continue
        ids.add(u.id)
    return ids


async def _fetch_contacts_users_by_id(
    client: TelegramClient,
    candidate_ids: set[int],
) -> dict[int, User]:
    """
    拉取我的通讯录，并返回：{user_id: User}（仅包含 candidate_ids 中的用户）。
    """
    contacts = await client(GetContactsRequest(hash=0))
    users = getattr(contacts, "users", None) or []
    res: dict[int, User] = {}
    for u in users:
        if not isinstance(u, User):
            continue
        if u.id not in candidate_ids:
            continue
        if u.access_hash is None:
            continue
        res[u.id] = u
    return res


async def _delete_contacts_users(client: TelegramClient, users: list[User], chunk_size: int = 50) -> int:
    if not users:
        return 0

    inputs: list[InputUser] = [
        InputUser(user_id=u.id, access_hash=u.access_hash) for u in users if u.access_hash is not None
    ]
    deleted = 0
    for i in range(0, len(inputs), chunk_size):
        chunk = inputs[i : i + chunk_size]
        if not chunk:
            continue
        await client(DeleteContactsRequest(id=chunk))
        deleted += len(chunk)
    return deleted


async def run() -> None:
    api_id = int(_require_env("API_ID"))
    api_hash = _require_env("API_HASH")
    phone = _require_env("PHONE")
    target = _require_env("TARGET_GROUP")
    skip_bots = _env_bool("SKIP_BOTS", True)
    require_username = _env_bool("REQUIRE_USERNAME", False)
    password = os.getenv("TELEGRAM_PASSWORD") or None
    proxy = _build_proxy()

    # 添加频率控制：
    # 1) 每次成功添加后，等待一个不均匀随机区间（默认 420-720s）
    # 2) 每小时（滚动 3600 秒窗口）最多成功添加 5 个
    min_interval = int(os.getenv("MIN_INTERVAL_SECONDS", "420"))
    max_interval = int(os.getenv("MAX_INTERVAL_SECONDS", "720"))
    hourly_max_adds = int(os.getenv("HOURLY_MAX_ADDS", "5"))
    hourly_window_seconds = int(os.getenv("HOURLY_WINDOW_SECONDS", "3600"))

    if min_interval < 1 or max_interval < 1:
        logger.error("MIN/MAX_INTERVAL_SECONDS 必须 >= 1")
        sys.exit(1)
    if min_interval > max_interval:
        logger.error("MIN_INTERVAL_SECONDS 不能大于 MAX_INTERVAL_SECONDS")
        sys.exit(1)
    if hourly_max_adds < 1 or hourly_window_seconds < 1:
        logger.error("HOURLY_MAX_ADDS / HOURLY_WINDOW_SECONDS 必须 >= 1")
        sys.exit(1)

    state = _load_state()
    done_ids: set[int] = set(state["done_ids"])
    left_ids: set[int] = set(state.get("channel_left_ids", []))
    channel_seen_ids: set[int] = set(state.get("channel_seen_ids", []))
    channel_last_stats_ts: float = float(state.get("channel_last_stats_ts", 0.0) or 0.0)

    # 频道统计（可选）：每隔一段时间同步一次 channel 成员快照，统计：
    # - 当前 channel 人数
    # - 截至当前时间进入过 channel 的人数（历史出现过的 channel_seen_ids）
    # - 截至当前时间取消订阅的人数（历史离开的 channel_left_ids）
    enable_channel_stats = _env_bool("ENABLE_CHANNEL_STATS", False)
    channel_stats_interval_seconds = int(os.getenv("CHANNEL_STATS_INTERVAL_SECONDS", "14400"))
    channel_stats_delete_left = _env_bool("CHANNEL_STATS_DELETE_LEFT", True)
    if channel_stats_interval_seconds < 1:
        logger.error("CHANNEL_STATS_INTERVAL_SECONDS 必须 >= 1")
        sys.exit(1)
    allowed_in_channel_ids: set[int] | None = None
    # 只记录“成功添加”的时间戳，用于滚动 1 小时窗口限流
    add_timestamps: deque[float] = deque()

    def _cleanup_old_adds(now: float) -> None:
        while add_timestamps and now - add_timestamps[0] > hourly_window_seconds:
            add_timestamps.popleft()

    async def _enforce_hourly_limit() -> None:
        now = time.time()
        _cleanup_old_adds(now)
        if len(add_timestamps) < hourly_max_adds:
            return
        # 达到上限：等待最早那次成功添加对应的窗口到期
        wait_seconds = hourly_window_seconds - (now - add_timestamps[0]) + 1
        logger.warning(
            "达到每 %s 秒最多添加 %s 个上限，等待 %.0f 秒后继续…",
            hourly_window_seconds,
            hourly_max_adds,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)

    client = TelegramClient(str(SESSION_NAME), api_id, api_hash, proxy=proxy)
    if proxy:
        logger.info("已启用代理连接 Telegram")

    await client.start(phone=phone, password=password)
    me = await client.get_me()
    if not me:
        logger.error("无法获取当前账号信息")
        await client.disconnect()
        sys.exit(1)

    entity = await client.get_entity(target)
    target_channel = os.getenv("TARGET_CHANNEL", "").strip()
    channel_entity_for_stats = None
    current_channel_ids_snapshot: set[int] = set()
    if target_channel:
        # 根据 channel 参与状态清理联系人，并把离开过的人记录到 state，保证后续不再自动添加。
        # 说明：第一次运行 state 中没有历史时，仅初始化 channel_seen_ids，不会执行删除。
        try:
            channel_entity = await client.get_entity(target_channel)
            channel_entity_for_stats = channel_entity
            current_channel_ids = await _fetch_current_participant_ids(
                client,
                channel_entity,
                me.id,
                skip_bots,
            )
            current_channel_ids_snapshot = current_channel_ids
            # 注意：添加队列的过滤只依赖 left_ids（即“历史进入过 channel，当前已退出的人”）
            # 不要求被添加的人必须“当前仍在 channel”。

            has_history = bool(channel_seen_ids)
            if not has_history:
                # 如果之前没有记录过 channel_seen_ids，则无法严格判断“进入过但现在退出”的集合。
                # 这里提供一个折中：可选地用 done_ids 初始化 channel_seen_ids，从而立即清理已加过但已离开的联系人。
                init_from_done = _env_bool("INIT_CHANNEL_SEEN_FROM_DONE", False)
                if init_from_done and done_ids:
                    channel_seen_ids = set(done_ids)
                    state["channel_seen_ids"] = sorted(channel_seen_ids)
                    logger.info("初始化 channel_seen_ids：从 done_ids 反推（done=%s）（仅当你确认 done_ids 与 channel 历史高度一致时使用）", len(done_ids))
                    has_history = True
                else:
                    channel_seen_ids = set(current_channel_ids)
                    state["channel_seen_ids"] = sorted(current_channel_ids)
                    _save_state(state)
                    logger.info("初始化 channel_seen_ids：当前 channel 成员数=%s（本次不删除）", len(current_channel_ids))
                    has_history = False

            if has_history:
                # “进入过但现在不在”的人
                new_left_ids = (channel_seen_ids - current_channel_ids) - left_ids
                if new_left_ids:
                    contacts_map = await _fetch_contacts_users_by_id(client, new_left_ids)
                    users_to_delete = [contacts_map[uid] for uid in new_left_ids if uid in contacts_map]
                    deleted_count = await _delete_contacts_users(client, users_to_delete)
                    logger.info("已删除：从通讯录移除 %s 人（离开 channel 新用户数=%s）", deleted_count, len(new_left_ids))
                else:
                    logger.info("离开 channel 无新增联系人待删除")

                left_ids |= new_left_ids
                state["channel_left_ids"] = sorted(left_ids)
                state["channel_seen_ids"] = sorted(channel_seen_ids | current_channel_ids)
                _save_state(state)
        except Exception as e:  # noqa: BLE001
            # 清理失败不应影响后续“添加联系人”的既有功能
            logger.error("清理离开 channel 成员失败：%s（将继续执行添加逻辑）", e)

        if enable_channel_stats and channel_entity_for_stats is not None:
            # 在启动/清理结束后先输出一次“截至当前时间”的统计
            now = time.time()
            channel_last_stats_ts = now
            entered_total = len(channel_seen_ids)
            left_total = len(left_ids)
            current_count = len(current_channel_ids_snapshot)
            state["channel_last_stats_ts"] = now
            history = state.get("channel_stats_history", [])
            history.append(
                {
                    "ts": now,
                    "current_count": current_count,
                    "entered_total": entered_total,
                    "left_total": left_total,
                    "reason": "startup",
                }
            )
            # 避免 state.json 无限增长（最多保留 200 条）
            state["channel_stats_history"] = history[-200:]
            _save_state(state)
            logger.info(
                "频道统计（startup）：当前=%s，进入=%s，取消订阅=%s",
                current_count,
                entered_total,
                left_total,
            )

    async def _sync_channel_and_log(reason: str) -> None:
        nonlocal channel_last_stats_ts, channel_seen_ids, left_ids
        if channel_entity_for_stats is None:
            return

        current_channel_ids = await _fetch_current_participant_ids(
            client,
            channel_entity_for_stats,
            me.id,
            skip_bots,
        )

        has_history = bool(channel_seen_ids)
        if not has_history:
            init_from_done = _env_bool("INIT_CHANNEL_SEEN_FROM_DONE", False)
            if init_from_done and done_ids:
                channel_seen_ids = set(done_ids)
                state["channel_seen_ids"] = sorted(channel_seen_ids)
                _save_state(state)
                logger.info("频道统计初始化：从 done_ids 反推（reason=%s）", reason)
            else:
                channel_seen_ids = set(current_channel_ids)
                state["channel_seen_ids"] = sorted(channel_seen_ids)
                _save_state(state)
                logger.info("频道统计初始化：当前 channel 成员数=%s（reason=%s）", len(current_channel_ids), reason)
            has_history = True

        # “进入过但现在不在”的新增离开者
        new_left_ids = (channel_seen_ids - current_channel_ids) - left_ids

        if new_left_ids and channel_stats_delete_left:
            contacts_map = await _fetch_contacts_users_by_id(client, new_left_ids)
            users_to_delete = [contacts_map[uid] for uid in new_left_ids if uid in contacts_map]
            deleted_count = await _delete_contacts_users(client, users_to_delete)
            if deleted_count:
                logger.info(
                    "频道同步：删除通讯录离开者 %s 人（reason=%s）",
                    deleted_count,
                    reason,
                )
        elif new_left_ids:
            logger.info("频道同步：检测到离开者 %s 人（未删除，reason=%s）", len(new_left_ids), reason)

        left_ids |= new_left_ids
        state["channel_left_ids"] = sorted(left_ids)
        state["channel_seen_ids"] = sorted(channel_seen_ids | current_channel_ids)

        now = time.time()
        channel_last_stats_ts = now
        state["channel_last_stats_ts"] = now
        history = state.get("channel_stats_history", [])
        history.append(
            {
                "ts": now,
                "current_count": len(current_channel_ids),
                "entered_total": len(channel_seen_ids),
                "left_total": len(left_ids),
                "reason": reason,
            }
        )
        state["channel_stats_history"] = history[-200:]
        _save_state(state)

        logger.info(
            "频道统计（%s）：当前=%s，进入=%s，取消订阅=%s",
            reason,
            len(current_channel_ids),
            len(channel_seen_ids),
            len(left_ids),
        )

    queue = await _build_queue(
        client,
        entity,
        me.id,
        skip_bots,
        require_username,
        done_ids,
        left_ids,
    )

    logger.info(
        "目标群: %s | 待处理: %s 人 | 每次等待: %s-%s 秒 | 每小时上限: %s（%s秒窗口）| 已跳过已处理: %s",
        target,
        len(queue),
        min_interval,
        max_interval,
        hourly_max_adds,
        hourly_window_seconds,
        len(done_ids),
    )

    if not queue:
        logger.info("没有需要添加的成员，退出。")
        await client.disconnect()
        return

    for idx, user in enumerate(queue):
        if enable_channel_stats and channel_entity_for_stats is not None:
            now = time.time()
            if now - channel_last_stats_ts >= channel_stats_interval_seconds:
                await _sync_channel_and_log("4h")
        await _enforce_hourly_limit()
        uid = user.id
        label = f"@{user.username}" if user.username else uid
        try:
            await _add_one(client, user)
            done_ids.add(uid)
            state["done_ids"] = sorted(done_ids)
            if str(uid) in state["failed"]:
                del state["failed"][str(uid)]
            _save_state(state)
            add_timestamps.append(time.time())
            logger.info("已添加联系人: %s (%s)", label, uid)
        except FloodWaitError as e:
            logger.warning("触发 FloodWait，等待 %s 秒后重试当前用户…", e.seconds)
            await asyncio.sleep(e.seconds + 1)
            try:
                await _add_one(client, user)
                done_ids.add(uid)
                state["done_ids"] = sorted(done_ids)
                _save_state(state)
                add_timestamps.append(time.time())
                logger.info("重试成功: %s (%s)", label, uid)
            except Exception as ex:  # noqa: BLE001
                state["failed"][str(uid)] = repr(ex)
                _save_state(state)
                logger.error("重试仍失败 %s: %s", label, ex)
        except RPCError as e:
            state["failed"][str(uid)] = repr(e)
            _save_state(state)
            logger.error("添加失败 %s: %s", label, e)
        except Exception as e:  # noqa: BLE001
            state["failed"][str(uid)] = repr(e)
            _save_state(state)
            logger.error("添加失败 %s: %s", label, e)

        if idx < len(queue) - 1:
            delay = random.randint(min_interval, max_interval)
            logger.info("等待 %s 秒后处理下一位…", delay)
            await asyncio.sleep(delay)

    logger.info("队列已全部处理完毕。")
    await client.disconnect()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("已中断。")


if __name__ == "__main__":
    main()
