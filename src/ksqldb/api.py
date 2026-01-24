import base64
import json

import httpx
import requests


class BaseAPI:
    def __init__(self, url, api_key=None, api_secret=None, https_cfg=None):
        self.url = url
        self.headers = {"Accept": "application/vnd.ksql.v1+json",
                        "Content-Type": "application/vnd.ksql.v1+json"}
        if api_key and api_secret:
            b64string = base64.b64encode(bytes(f"{api_key}:{api_secret}"))
            self.headers["Authorization"] = f"Basic {b64string}"
        
        if https_cfg is None:
            self.verify = True
            self.cert = None
        elif isinstance(https_cfg, bool):
            self.verify = https_cfg
            self.cert = None
        elif isinstance(https_cfg, (list, tuple)):
            self.verify = True
            self.cert = https_cfg
        else:
            raise ValueError("https_cfg must be None, bool, or a list/tuple")

    def ksql(self, ksql_string, stream_properties=None):
        body = {
            "ksql": ksql_string,
            "streamsProperties": stream_properties if stream_properties else {},
        }
        r = requests.post(f"{self.url}/ksql", json=body, headers=self.headers, verify=self.verify, cert=self.cert)
        r.raise_for_status()
        return r.json()

    async def query(self, query_string, timeout, stream_properties=None):
        body = {
            "sql": query_string,
            "properties": stream_properties if stream_properties else {},
        }

        client = httpx.AsyncClient(http1=False, http2=True, headers=self.headers, verify=self.verify, cert=self.cert)
        async with client.stream("POST", f"{self.url}/query-stream", json=body, timeout=timeout) as r:
            async for line in r.aiter_lines():
                # Response is a multi-line json array, so remove the array wrapper so that we can
                # parse individual lines and return them.
                yield json.loads(line.strip('[],'))

    def close_query(self, query_id):
        response = requests.post(f"{self.url}/close-query", json={"queryId": query_id}, verify=self.verify, cert=self.cert)
        return response.ok

    def inserts_stream(self, stream_name, rows):
        body = json.dumps({"target": stream_name}) + "\n"
        for row in rows:
            body += f"{json.dumps(row)}\n"

        client = httpx.Client(http1=False, http2=True, verify=self.verify, cert=self.cert)
        with client.stream("POST", f"{self.url}/inserts-stream", content=body, headers=self.headers) as r:
            response_data = [json.loads(x) for x in r.iter_lines()]

        return response_data
