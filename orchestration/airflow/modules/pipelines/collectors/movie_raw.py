"""Resumable KOFIC/KMDb raw collection. Never writes to the serving DB."""
from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote, quote_plus

import requests

VERSION = 1
KOFIC = "https://www.kobis.or.kr/kobisopenapi/webservice/rest"
KMDB = "https://api.koreafilm.or.kr/openapi-data2/wisenut/search_api/search_json2.jsp"


def packed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(value):
    return hashlib.sha256(value).hexdigest()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


class CollectionError(RuntimeError):
    """Only sanitized messages are allowed across the task/log boundary."""


class S3Store:
    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def get(self, key):
        from botocore.exceptions import ClientError
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return None
            raise CollectionError("S3 read failed; check connection and bucket permissions") from None
        body = response["Body"].read()
        if response.get("Metadata", {}).get("sha256") != digest(body):
            raise CollectionError("S3 object checksum mismatch")
        return json.loads(body)

    def put(self, key, value):
        from botocore.exceptions import ClientError
        body = packed(value)
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=key, Body=body, ContentType="application/json",
                Metadata={"sha256": digest(body)}, IfNoneMatch="*",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise CollectionError("S3 write failed; check connection and bucket permissions") from None
            if self.get(key) != value:
                raise CollectionError("Immutable object conflict; start a new DAG run") from None


class ApiClient:
    def __init__(self, kofic_key, kmdb_key, *, session=None, sleep=time.sleep, delay=0.4):
        # A single configured key per provider; never rotate to bypass a quota.
        self.keys = {"kofic": kofic_key, "kmdb": kmdb_key}
        if not all(self.keys.values()) or any("," in k for k in self.keys.values()):
            raise CollectionError("Configure one nonempty API key per Connection password")
        self.session = session or requests.Session()
        self.sleep, self.delay = sleep, delay

    def fetch(self, source, endpoint, params):
        secret = self.keys[source]
        request_params = {**params, ("key" if source == "kofic" else "ServiceKey"): secret}
        for attempt in range(3):
            self.sleep(self.delay if attempt == 0 else 2 ** attempt)
            response = None
            try:
                response = self.session.get(endpoint, params=request_params, timeout=(10, 40), allow_redirects=False)
            except requests.RequestException:
                if attempt == 2:
                    raise CollectionError(f"{source}: transport failure after 3 attempts") from None
                continue
            if response.status_code == 429:
                raise CollectionError(f"{source}: quota/rate limit; retry later")
            if response.status_code >= 500 and attempt < 2:
                continue
            if response.status_code != 200:
                raise CollectionError(f"{source}: HTTP {response.status_code}")
            # Do not persist a provider accidentally echoing a credential.
            try:
                raw = response.content.decode("utf-8-sig")
            except UnicodeError:
                raise CollectionError(f"{source}: response is not UTF-8") from None
            for key in self.keys.values():
                for encoded in {key, quote(key, safe=""), quote_plus(key)}:
                    raw = raw.replace(encoded, "[REDACTED]")
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                raise CollectionError(f"{source}: invalid JSON response") from None
            if not isinstance(data, dict):
                raise CollectionError(f"{source}: unexpected response shape")
            if data.get("faultInfo") or data.get("error") or data.get("Error"):
                raise CollectionError(f"{source}: provider rejected request; inspect credentials/quota")
            return data
        raise CollectionError(f"{source}: request failed")


def make_plan(params, airflow_run_id, anchor_date):
    start, end = int(params["start_year"]), int(params["end_year"])
    if not 2020 <= start <= end <= date.fromisoformat(anchor_date).year:
        raise CollectionError("Require 2020 <= start_year <= end_year <= current run year")
    limit = int(params["max_movies_per_year"])
    page_limit = int(params["max_pages_per_year"])
    candidate_pages = int(params["kmdb_max_pages"])
    if not 0 <= limit <= 10000 or not 1 <= page_limit <= 200 or not 1 <= candidate_pages <= 10:
        raise CollectionError("Invalid collection bounds")
    bstart, bend = params.get("boxoffice_start", ""), params.get("boxoffice_end", "")
    if bool(bstart) != bool(bend):
        raise CollectionError("Set both boxoffice dates or neither")
    dates = []
    if bstart:
        first, last = date.fromisoformat(bstart), date.fromisoformat(bend)
        if first > last or last >= date.fromisoformat(anchor_date) or (last-first).days > 30:
            raise CollectionError("Boxoffice range must be past dates, at most 31 days")
        dates = [(first + timedelta(days=i)).strftime("%Y%m%d") for i in range((last-first).days+1)]
    scope = {"version": VERSION, "start_year": start, "end_year": end,
             "max_movies_per_year": limit, "max_pages_per_year": page_limit,
             "kmdb_max_pages": candidate_pages, "boxoffice_dates": dates,
             "movie_type_cd": "", "selection": "provider_page_order",
             "policy": "raw_unfiltered; service eligibility and matching belong to Silver"}
    token = digest(packed({"dag_run_id": airflow_run_id, "scope": scope}))[:24]
    return {"run_id": token, "airflow_run_id": airflow_run_id, "scope": scope,
            "raw_prefix": f"raw/movie_api/v1/run_id={token}",
            "manifest_prefix": f"manifests/movie_api/v1/run_id={token}"}


class Collector:
    def __init__(self, store, api, plan):
        self.store, self.api, self.plan = store, api, plan

    def manifest_key(self, stage):
        return f"{self.plan['manifest_prefix']}/{stage}.json"

    def existing(self, stage):
        return self.store.get(self.manifest_key(stage))

    def capture(self, source, resource, params):
        request_id = digest(packed({"source": source, "resource": resource, "params": params}))
        key = f"{self.plan['raw_prefix']}/{source}/{resource}/{request_id}.json"
        envelope = self.store.get(key)
        if envelope is None:
            endpoint = f"{KOFIC}/{resource}.json" if source == "kofic" else KMDB
            payload = self.api.fetch(source, endpoint, params)
            envelope = {"schema_version": VERSION, "run_id": self.plan["run_id"],
                        "source": source, "resource": resource, "request": params,
                        "collected_at": utcnow(), "payload_sha256": digest(packed(payload)),
                        "payload": payload}
            # Validate before checkpointing; an API error must never become a cached empty result.
            self.validate_response(source, resource, payload)
            self.store.put(key, envelope)
        payload = envelope["payload"]
        if digest(packed(payload)) != envelope["payload_sha256"]:
            raise CollectionError("Raw payload checksum mismatch")
        self.validate_response(source, resource, payload)
        return payload, {"key": key, "sha256": envelope["payload_sha256"]}

    @staticmethod
    def validate_response(source, resource, data):
        try:
            if source == "kmdb":
                if str(data.get("ErrorCode", "0")) not in ("0", "", "None"):
                    raise ValueError()
                total = int(data["TotalCount"])
                blocks = data["Data"]
                if total < 0 or not isinstance(blocks, list):
                    raise ValueError()
                rows = blocks[0].get("Result", []) if blocks else []
                if not isinstance(rows, list) or (total > 0 and not rows):
                    raise ValueError()
            elif resource == "movie/searchMovieList":
                block = data["movieListResult"]
                if int(block["totCnt"]) < 0 or not isinstance(block["movieList"], list):
                    raise ValueError()
            elif resource == "movie/searchMovieInfo":
                if not data["movieInfoResult"]["movieInfo"]["movieCd"]:
                    raise ValueError()
            else:
                if not isinstance(data["boxOfficeResult"]["dailyBoxOfficeList"], list):
                    raise ValueError()
        except (KeyError, TypeError, ValueError, IndexError):
            raise CollectionError(f"{source}: malformed {resource} response") from None

    def finish(self, stage, objects, **summary):
        manifest = {"schema_version": VERSION, "run_id": self.plan["run_id"],
                    "airflow_run_id": self.plan["airflow_run_id"], "scope": self.plan["scope"],
                    "stage": stage, "status": "SUCCESS", "completed_at": utcnow(),
                    "objects": objects, **summary}
        self.store.put(self.manifest_key(stage), manifest)
        return self.manifest_key(stage)

    def movie_list(self):
        if self.existing("movie_list"):
            return self.manifest_key("movie_list")
        scope = self.plan["scope"]
        objects, selected, years = [], {}, []
        for year in range(scope["start_year"], scope["end_year"] + 1):
            seen, total, page = {}, None, 1
            while True:
                data, ref = self.capture("kofic", "movie/searchMovieList",
                    {"openStartDt": str(year), "openEndDt": str(year), "curPage": page, "itemPerPage": 100})
                objects.append(ref)
                block = data["movieListResult"]
                reported = int(block["totCnt"])
                if total is not None and reported != total:
                    raise CollectionError("Movie list changed during pagination; start a new run")
                total = reported
                rows = block["movieList"]
                if total and not rows:
                    raise CollectionError("Empty page before declared movie total")
                for row in rows:
                    code = str(row.get("movieCd", ""))
                    # KOFIC now returns codes such as 2026A754; external IDs are strings.
                    if not re.fullmatch(r"[A-Za-z0-9]{1,32}", code) or code in seen:
                        raise CollectionError("Invalid or duplicate movie ID within year pagination")
                    seen[code] = row
                if len(seen) > total:
                    raise CollectionError("Movie count exceeds declared total")
                cap = scope["max_movies_per_year"]
                if len(seen) == total or (cap and len(seen) >= cap):
                    break
                if page >= scope["max_pages_per_year"]:
                    raise CollectionError("Page safety cap reached before requested scope completed")
                page += 1
            chosen = list(seen.items())[:cap or None]
            selected.update(chosen)
            years.append({"year": year, "reported_total": total, "fetched": len(seen),
                          "selected": len(chosen), "complete": len(chosen) == total})
        return self.finish("movie_list", objects, movies=selected, years=years,
                           movie_count=len(selected), complete=all(y["complete"] for y in years))

    def details(self):
        if self.existing("details"):
            return self.manifest_key("details")
        source = self.existing("movie_list")
        if source is None:
            raise CollectionError("Movie list manifest is required")
        objects = []
        for code in source["movies"]:
            data, ref = self.capture("kofic", "movie/searchMovieInfo", {"movieCd": code})
            if str(data["movieInfoResult"]["movieInfo"]["movieCd"]) != code:
                raise CollectionError("Detail ID differs from requested movie ID")
            objects.append({**ref, "movie_cd": code})
        return self.finish("details", objects, movie_count=len(objects))

    def kmdb_candidates(self):
        if self.existing("kmdb_candidates"):
            return self.manifest_key("kmdb_candidates")
        details = self.existing("details")
        if details is None:
            raise CollectionError("Detail manifest is required")
        objects, outcomes = [], []
        for ref in details["objects"]:
            movie = self.store.get(ref["key"])["payload"]["movieInfoResult"]["movieInfo"]
            # Keep original titles and all candidates; no heuristic cross-source join at Bronze.
            titles = list(dict.fromkeys(t.strip() for t in (movie.get("movieNm", ""), movie.get("movieNmEn", "")) if t.strip()))
            if not titles:
                raise CollectionError("Cannot search KMDb without a movie title")
            candidate_ids, truncated, requests_count = set(), False, 0
            for title in titles:
                total, fetched = None, 0
                for page in range(self.plan["scope"]["kmdb_max_pages"]):
                    params = {"collection": "kmdb_new2", "detail": "Y", "query": title,
                              "listCount": 100, "startCount": page * 100}
                    data, obj = self.capture("kmdb", "search", params)
                    objects.append({**obj, "movie_cd": ref["movie_cd"]})
                    requests_count += 1
                    reported = int(data["TotalCount"])
                    if total is not None and total != reported:
                        raise CollectionError("KMDb result changed during pagination; start new run")
                    total = reported
                    rows = data["Data"][0].get("Result", []) if data["Data"] else []
                    fetched += len(rows)
                    for row in rows:
                        identity = row.get("DOCID") or f"{row.get('movieId','')}|{row.get('movieSeq','')}"
                        candidate_ids.add(identity)
                    if fetched >= total:
                        break
                truncated = truncated or fetched < total
            outcomes.append({"movie_cd": ref["movie_cd"], "candidate_count": len(candidate_ids),
                             "search_requests": requests_count, "truncated": truncated,
                             "mapping_status": "CANDIDATES" if candidate_ids else "NO_RESULTS"})
        return self.finish("kmdb_candidates", objects, outcomes=outcomes, movie_count=len(outcomes),
                           truncated_movies=sum(o["truncated"] for o in outcomes), matching_performed=False)

    def boxoffice(self):
        if self.existing("boxoffice"):
            return self.manifest_key("boxoffice")
        objects = []
        for target in self.plan["scope"]["boxoffice_dates"]:
            data, ref = self.capture("kofic", "boxoffice/searchDailyBoxOfficeList", {"targetDt": target})
            objects.append({**ref, "target_date": target,
                            "row_count": len(data["boxOfficeResult"]["dailyBoxOfficeList"])})
        return self.finish("boxoffice", objects, day_count=len(objects),
                           observation_scope="daily_boxoffice_api_top_list; not all screenings")

    def validate_run(self):
        stages = ("movie_list", "details", "kmdb_candidates", "boxoffice")
        manifests = {s: self.existing(s) for s in stages}
        if not all(manifests.values()):
            raise CollectionError("All stage manifests are required")
        n = manifests["movie_list"]["movie_count"]
        if any(manifests[s]["movie_count"] != n for s in ("details", "kmdb_candidates")):
            raise CollectionError("Cross-stage movie count mismatch")
        for manifest in manifests.values():
            if manifest["scope"] != self.plan["scope"] or manifest["run_id"] != self.plan["run_id"]:
                raise CollectionError("Manifest belongs to a different run/scope")
            for ref in manifest["objects"]:
                obj = self.store.get(ref["key"])
                if obj is None or digest(packed(obj["payload"])) != ref["sha256"]:
                    raise CollectionError("Referenced raw object is missing or corrupt")
        key = self.manifest_key("SUCCESS")
        if self.store.get(key) is None:
            self.store.put(key, {"schema_version": VERSION, "run_id": self.plan["run_id"],
                "airflow_run_id": self.plan["airflow_run_id"], "status": "SUCCESS",
                "scope": self.plan["scope"], "completed_at": utcnow(), "movie_count": n,
                "catalog_complete": manifests["movie_list"]["complete"],
                "kmdb_truncated_movies": manifests["kmdb_candidates"]["truncated_movies"],
                "stage_manifests": {s: self.manifest_key(s) for s in stages}})
        return key
