#!/usr/bin/env python3
"""
SERVICEHUB — VERTICAL SLICE FUNCTIONAL SERVER

High-performance Python 3 HTTP + Server-Sent Events (SSE) Real-Time Server.
Integrates Hexagonal Architecture Core Domain (BC-01) with Real-Time Web Interfaces.
"""

import os
import sys
import json
import time
import queue
import threading
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

# Import Core Domain Aggregates, Value Objects & Use Cases
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scratch.domain_sandbox.test_core_domain import (
    InMemoryServiceRequestRepository,
    SubmitServiceRequestApplicationService,
    AcceptServiceRequestApplicationService,
    CompleteServiceRequestApplicationService,
    ConfirmServiceReceiptApplicationService,
    RequestId,
    RequestState,
    DomainException
)

# Shared In-Memory Repository & Use Cases
SERVER_START_TIME = time.time()
repository = InMemoryServiceRequestRepository()
submit_use_case = SubmitServiceRequestApplicationService(repository)
accept_use_case = AcceptServiceRequestApplicationService(repository)
complete_use_case = CompleteServiceRequestApplicationService(repository)
confirm_receipt_use_case = ConfirmServiceReceiptApplicationService(repository)

# Event Subscribers List for Realtime Streaming (SSE)
sse_subscribers = set()
sse_lock = threading.Lock()

def broadcast_domain_events(events):
    """Broadcasts Domain Events in real-time to all connected SSE clients (<50ms)."""
    with sse_lock:
        dead = set()
        for q in sse_subscribers:
            try:
                for event in events:
                    payload = {
                        "eventName": getattr(event, 'event_name', 'DomainEvent'),
                        "aggregateId": getattr(event, 'aggregate_id', ''),
                        "attendantId": getattr(event, 'attendant_id', None),
                        "occurredOn": str(getattr(event, 'occurred_on', ''))
                    }
                    q.put_nowait(payload)
            except Exception:
                dead.add(q)
        sse_subscribers.difference_update(dead)

class VerticalSliceRequestHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Suppress noisy HTTP logs for clean output
        pass

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # HEALTH & READINESS ENDPOINTS
        if path == '/health':
            self.send_json({"status": "UP", "component": "ServiceOS-Pilot", "uptimeSeconds": int(time.time() - SERVER_START_TIME)})
            return

        if path == '/readiness':
            self.send_json({"ready": True, "activeRequests": len([r for r in repository.store.values() if r.state in [RequestState.SUBMITTED, RequestState.ACCEPTED]])})
            return

        # 1. REALTIME EVENT STREAMING (SSE / EVENT BUS OUTPUT ADAPTER)
        if path == '/api/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            q = queue.Queue()
            with sse_lock:
                sse_subscribers.add(q)

            try:
                while True:
                    try:
                        payload = q.get(timeout=15.0)
                        msg = f"data: {json.dumps(payload)}\n\n"
                        self.wfile.write(msg.encode('utf-8'))
                        self.wfile.flush()
                    except queue.Empty:
                        # Keep-alive heartbeat comment
                        self.wfile.write(b": heartbeat\n\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, Exception):
                pass
            finally:
                with sse_lock:
                    sse_subscribers.discard(q)
            return

        # 2. ACTIVE REQUESTS PROJECTION API (CQRS READ MODEL)
        if path == '/api/requests/active':
            active_list = []
            for req in repository.store.values():
                if req.state in [RequestState.SUBMITTED, RequestState.ACCEPTED]:
                    active_list.append({
                        "id": req.id.value,
                        "tenantId": req.tenant_id.value,
                        "venueId": req.venue_id.value,
                        "servicePointId": req.service_point_id.value,
                        "intentTypeId": req.intent_type_id.value,
                        "intentTypeName": req.intent_type_id.name,
                        "visitorNote": req.visitor_note.text,
                        "state": req.state.value,
                        "urgencyLevel": req.urgency_level.value,
                        "assignedAttendantId": req.assigned_attendant_id,
                        "submittedAt": str(req.submitted_at)
                    })
            self.send_json(active_list)
            return

        # 3. STATIC FILES DISPATCHER
        if path == '/' or path == '/login':
            self.serve_file('public/login.html', 'text/html')
        elif path == '/visitor':
            self.serve_file('public/visitor.html', 'text/html')
        elif path == '/staff':
            self.serve_file('public/staff.html', 'text/html')
        elif path == '/superadmin':
            self.serve_file('public/superadmin.html', 'text/html')
        elif path == '/biblioteca':
            self.serve_file('public/biblioteca.html', 'text/html')
        elif path == '/tenant':
            self.serve_file('public/tenant.html', 'text/html')
        elif path == '/admin':
            self.serve_file('public/admin.html', 'text/html')
        elif path == '/manifest.json':
            self.serve_file('public/manifest.json', 'application/json')
        elif path == '/styles.css':
            self.serve_file('public/styles.css', 'text/css')
        else:
            self.send_error(404, "File Not Found")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}

        # 1. VERIFY PRESENCE API (BC-03 Presence Verification Engine)
        if path == '/api/presence/verify':
            token = f"VERIFIED-PRESENCE-{data.get('servicePointId', 'MESA-14')}-{int(time.time())}"
            self.send_json({"success": True, "token": token, "verifiedAt": str(time.time())})
            return

        # 2. SUBMIT SERVICE REQUEST API (BC-01 Core Domain Write Model)
        if path == '/api/requests':
            try:
                events = submit_use_case.execute(data)
                broadcast_domain_events(events)
                self.send_json({"success": True, "requestId": data['requestId'], "eventsEmitted": [e.event_name for e in events]})
            except DomainException as e:
                self.send_json({"success": False, "error": str(e)}, status=400)
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=500)
            return

        # 3. ACCEPT SERVICE REQUEST API (BC-01 Core Domain Write Model)
        if path.startswith('/api/requests/') and path.endswith('/accept'):
            parts = path.split('/')
            req_id = parts[3]
            attendant_id = data.get('attendantId', 'SOFÍA (STAFF-01)')
            try:
                events = accept_use_case.execute(req_id, attendant_id)
                broadcast_domain_events(events)
                self.send_json({"success": True, "requestId": req_id, "state": "ACCEPTED"})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=400)
            return

        # 4. COMPLETE SERVICE REQUEST API (BC-01 Staff Complete)
        if path.startswith('/api/requests/') and path.endswith('/complete'):
            parts = path.split('/')
            req_id = parts[3]
            try:
                events = complete_use_case.execute(req_id)
                broadcast_domain_events(events)
                self.send_json({"success": True, "requestId": req_id, "state": "COMPLETED_BY_STAFF"})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=400)
            return

        # 5. CONFIRM SERVICE RECEIPT API (PD-001 Visitor Collaborative Confirm)
        if path.startswith('/api/requests/') and path.endswith('/confirm-receipt'):
            parts = path.split('/')
            req_id = parts[3]
            try:
                events = confirm_receipt_use_case.execute(req_id)
                broadcast_domain_events(events)
                self.send_json({"success": True, "requestId": req_id, "state": "COMPLETED_BY_VISITOR"})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, status=400)
            return

        self.send_error(404, "API Endpoint Not Found")

    def serve_file(self, rel_path, mime_type):
        abs_path = os.path.join(os.path.dirname(__file__), rel_path)
        if os.path.exists(abs_path):
            try:
                self.send_response(200)
                self.send_header('Content-Type', mime_type)
                self.end_headers()
                with open(abs_path, 'rb') as f:
                    self.wfile.write(f.read())
            except (BrokenPipeError, ConnectionResetError):
                pass
        else:
            self.send_error(404, "File Not Found")

    def send_json(self, data, status=200):
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode('utf-8'))
        except (BrokenPipeError, ConnectionResetError):
            pass

def run_vertical_slice_server(port=8080):
    server_address = ('', port)
    httpd = ThreadingHTTPServer(server_address, VerticalSliceRequestHandler)
    print(f"============================================================================")
    print(f" SERVICEHUB — THREADED MULTI-CLIENT SERVER RUNNING ON PORT {port}")
    print(f" Visitor PWA UI:          http://localhost:{port}/visitor")
    print(f" Staff Dispatcher UI:     http://localhost:{port}/staff")
    print(f" Realtime SSE Stream:     http://localhost:{port}/api/events")
    print(f"============================================================================\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        httpd.server_close()

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_vertical_slice_server(port)
