from typing import Dict, Any, Optional
import time
import json
import logging
import requests

logger = logging.getLogger(__name__)


def _endpoint_url() -> str:
    return "https://contract.feishu.cn/clm/api/cooperation/exportCooperationRecords"


def compose_headers(cfg: dict) -> Dict[str, str]:
    eh = cfg.get("export_headers", {})
    headers: Dict[str, str] = {
        "Timezone-Offset": str(eh.get("timezone_offset", -480)),
        "Cookie": str(eh.get("cookie", "")),
        "Content-Type": str(eh.get("content_type", "application/json")),
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
    }
    try:
        logger.debug(
            "compose_headers: keys=%s cookie_present=%s",
            list(headers.keys()),
            bool(headers.get("Cookie")),
        )
    except Exception:
        pass
    return headers


def build_body(from_date: str, to_date: str, keyword: str = "", search_tab_enum_code: int = 0,
               extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "keyword": keyword,
        "searchTabEnumCode": search_tab_enum_code,
        "searchCooperationByCreateTime": [from_date, to_date],
    }
    if extra:
        body.update(extra)
    try:
        logger.debug(
            "build_body: from=%s to=%s keyword=%s tab=%s extra_keys=%s",
            from_date,
            to_date,
            keyword,
            search_tab_enum_code,
            list(extra.keys()) if extra else [],
        )
    except Exception:
        pass
    return body


def submit_export(cfg: dict, from_date: str, to_date: str, keyword: str = "",
                  search_tab_enum_code: int = 0, extra_body: Optional[Dict[str, Any]] = None,
                  timeout_seconds: int = 30) -> Dict[str, Any]:
    url = _endpoint_url()
    headers = compose_headers(cfg)
    body = build_body(from_date, to_date, keyword, search_tab_enum_code, extra_body)
    try:
        logger.debug(
            "submit_export: url=%s timeout=%s attempts/backoff=%s/%s",
            url,
            timeout_seconds,
            int(cfg.get("retry", {}).get("max_attempts", 3)),
            int(cfg.get("retry", {}).get("backoff_seconds", 5)),
        )
        logger.debug("submit_export: body=%s", body)
    except Exception:
        pass

    retry_cfg = cfg.get("retry", {})
    max_attempts = int(retry_cfg.get("max_attempts", 3))
    backoff_seconds = int(retry_cfg.get("backoff_seconds", 5))

    last_err: Optional[str] = None
    for attempt in range(1, max_attempts + 1):
        try:
            logger.debug("submit_export: attempt=%s/%s", attempt, max_attempts)
            resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=timeout_seconds)
            status = resp.status_code
            text = resp.text
            data: Optional[Any] = None
            try:
                data = resp.json()
            except Exception:
                data = None
            ok = 200 <= status < 300
            logger.debug("submit_export: status=%s text_len=%s", status, len(text) if text else 0)
            if ok:
                logger.debug("submit_export: success")
                return {"ok": True, "status_code": status, "data": data, "text": text}
            else:
                last_err = f"HTTP {status}"
                logger.warning("submit_export: non-2xx status=%s", status)
        except Exception as e:  # 请求异常
            last_err = str(e)
            logger.exception("submit_export: request error")

        if attempt < max_attempts:
            logger.debug("submit_export: backoff %ss before retry", backoff_seconds)
            time.sleep(backoff_seconds)

    logger.error("submit_export: failed after %s attempts, last_err=%s", max_attempts, last_err)
    return {"ok": False, "status_code": None, "data": None, "text": None, "error": last_err}
