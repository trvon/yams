#!/usr/bin/env python3
"""Minimal SigV4 fixture helper for the internal-only MinIO integration arm."""

import argparse
import datetime
import hashlib
import hmac
import json
import os
import sys
import urllib.parse
import urllib.request


def sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def request(
    method: str, endpoint: str, bucket: str, key: str, body: bytes = b""
) -> bytes:
    access = os.environ["AWS_ACCESS_KEY_ID"]
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    region = os.environ.get("AWS_REGION", "us-east-1")
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    encoded = "/" + urllib.parse.quote(bucket + "/" + key, safe="/-_.~")
    host = urllib.parse.urlsplit(endpoint).netloc
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{stamp}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"{method}\n{encoded}\n\n{headers}\n{signed_headers}\n{payload_hash}"
    scope = f"{day}/{region}/s3/aws4_request"
    string_to_sign = (
        "AWS4-HMAC-SHA256\n"
        + stamp
        + "\n"
        + scope
        + "\n"
        + hashlib.sha256(canonical.encode()).hexdigest()
    )
    signing_key = sign(
        sign(sign(sign(("AWS4" + secret).encode(), day), region), "s3"), "aws4_request"
    )
    signature = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    auth = (
        f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    req = urllib.request.Request(
        endpoint + encoded, data=body if method == "PUT" else None, method=method
    )
    req.add_header("Authorization", auth)
    req.add_header("x-amz-content-sha256", payload_hash)
    req.add_header("x-amz-date", stamp)
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.read()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "operation",
        choices=("put", "head", "head-migrated", "put-legacy", "put-corrupt"),
    )
    parser.add_argument("--endpoint", default="http://minio:9000")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--key")
    parser.add_argument("--value")
    args = parser.parse_args()

    if args.operation in ("put", "head"):
        if args.key is None or (args.operation == "put" and args.value is None):
            parser.error(
                f"{args.operation} requires --key"
                + (" and --value" if args.operation == "put" else "")
            )
        request(
            "PUT" if args.operation == "put" else "HEAD",
            args.endpoint,
            args.bucket,
            args.prefix + "/" + args.key,
            (args.value or "").encode(),
        )
        return 0

    if args.operation == "head-migrated":
        payload_hash = hashlib.sha256(
            (args.value or "legacy-value").encode()
        ).hexdigest()
        origin = "123e4567-e89b-42d3-a456-426614174009"
        upgraded = json.dumps(
            {
                "schemaVersion": 3,
                "entryHash": payload_hash,
                "ts": {"physicalMs": 123, "logical": 4},
                "origin": origin,
                "vv": {"counters_": {origin: 7}},
                "corpusId": "matrix-corpus",
                "corpusEpoch": 1,
                "operationId": origin + ":7",
                "logicalKey": "user/legacy",
                "recordKind": "value",
                "tombstonePayload": "",
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        request(
            "HEAD",
            args.endpoint,
            args.bucket,
            f"{args.prefix}/index/user/legacy/{hashlib.sha256(upgraded).hexdigest()}",
        )
        return 0

    if args.operation == "put-corrupt":
        body = b"{not-a-valid-envelope"
        digest = hashlib.sha256(body).hexdigest()
        request(
            "PUT",
            args.endpoint,
            args.bucket,
            f"{args.prefix}/index/user/corrupt/{digest}",
            body,
        )
        return 0

    payload = (args.value or "legacy-value").encode()
    payload_hash = hashlib.sha256(payload).hexdigest()
    origin = "123e4567-e89b-42d3-a456-426614174009"
    envelope = json.dumps(
        {
            "entryHash": payload_hash,
            "ts": {"physicalMs": 123, "logical": 4, "origin": origin},
            "version": {"counters_": {origin: 7}},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    envelope_hash = hashlib.sha256(envelope).hexdigest()
    request(
        "PUT", args.endpoint, args.bucket, f"{args.prefix}/blob/{payload_hash}", payload
    )
    request(
        "PUT",
        args.endpoint,
        args.bucket,
        f"{args.prefix}/index/user/legacy/{envelope_hash}",
        envelope,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"s3 fixture failed: {error}", file=sys.stderr)
        raise
