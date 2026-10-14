#!/usr/bin/env python3
"""Small live OpenMetrics endpoint for manual Collector/Grafana verification."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import math
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--host', default='127.0.0.1')
parser.add_argument('--port', type=int, default=18080)
args = parser.parse_args()
started = time.monotonic()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/metrics':
            self.send_error(404)
            return
        elapsed = time.monotonic() - started
        count = 100 + int(elapsed * 12)
        errors = 3 + int(elapsed * .3)
        text = f'''# HELP demo_requests Completed requests.
# TYPE demo_requests counter
demo_requests_total{{method="GET",status="200"}} {count}
demo_requests_total{{method="GET",status="500"}} {errors}
# HELP demo_queue_depth Work waiting to run.
# TYPE demo_queue_depth gauge
demo_queue_depth{{worker="one"}} {20 + 10 * math.sin(elapsed / 25):.6f}
demo_queue_depth{{worker="two"}} {12 + 5 * math.cos(elapsed / 20):.6f}
# HELP demo_request_duration_seconds Request duration.
# TYPE demo_request_duration_seconds histogram
demo_request_duration_seconds_bucket{{le="0.1"}} {int(count * .5)}
demo_request_duration_seconds_bucket{{le="0.5"}} {int(count * .85)}
demo_request_duration_seconds_bucket{{le="1"}} {int(count * .95)}
demo_request_duration_seconds_bucket{{le="+Inf"}} {count}
demo_request_duration_seconds_sum {count * .22:.2f}
demo_request_duration_seconds_count {count}
# HELP demo_payload_bytes Payload size.
# TYPE demo_payload_bytes summary
demo_payload_bytes{{quantile="0.5"}} 512
demo_payload_bytes{{quantile="0.9"}} 2048
demo_payload_bytes{{quantile="0.99"}} 4096
demo_payload_bytes_sum {count * 800}
demo_payload_bytes_count {count}
# EOF
'''
        body = text.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/openmetrics-text; version=1.0.0; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *_args):
        pass

ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
