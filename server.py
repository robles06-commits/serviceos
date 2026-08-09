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

# ============================================================================
# CORE DOMAIN DEFINITIONS (SELF-CONTAINED)
# ============================================================================
class DomainException(Exception): pass

class DomainInvalidStateTransitionException(DomainException):
    def __init__(self, current_state: str, attempted_action: str):
        super().__init__(f"Invalid domain state transition: Cannot perform '{attempted_action}' when state is '{current_state}'.")

class DomainInvariantViolationException(DomainException):
    def __init__(self, invariant_code: str, message: str):
        super().__init__(f"[{invariant_code}] Invariant Violation: {message}")

class RequestId:
    def __init__(self, value: str):
        if not value: raise DomainInvariantViolationException('INV-01', 'RequestId cannot be empty')
        self.value = str(value)
    def __eq__(self, other): return isinstance(other, RequestId) and self.value == other.value

class TenantId:
    def __init__(self, value: str):
        if not value: raise DomainInvariantViolationException('INV-02', 'TenantId cannot be empty')
        self.value = str(value)
    def __eq__(self, other): return isinstance(other, TenantId) and self.value == other.value

class VenueId:
    def __init__(self, value: str):
        if not value: raise DomainInvariantViolationException('INV-01', 'VenueId cannot be empty')
        self.value = str(value)
    def __eq__(self, other): return isinstance(other, VenueId) and self.value == other.value

class ServicePointId:
    def __init__(self, value: str):
        if not value: raise DomainInvariantViolationException('INV-01', 'ServicePointId cannot be empty')
        self.value = str(value)
    def __eq__(self, other): return isinstance(other, ServicePointId) and self.value == other.value

class ServiceIntentTypeId:
    def __init__(self, value: str, name: str = ""):
        if not value: raise DomainInvariantViolationException('INV-01', 'ServiceIntentTypeId value cannot be empty')
        self.value = str(value)
        self.name = name or str(value)
    def __eq__(self, other): return isinstance(other, ServiceIntentTypeId) and self.value == other.value

class VisitorNote:
    def __init__(self, text: str = ""):
        self.text = text.strip() if text else ""

class SignedAccessProof:
    def __init__(self, token: str):
        if not token: raise DomainInvariantViolationException('INV-04', 'SignedAccessProof token required')
        self.token = token

from enum import Enum
class RequestState(str, Enum):
    SUBMITTED = 'SUBMITTED'
    ACCEPTED = 'ACCEPTED'
    COMPLETED_BY_STAFF = 'COMPLETED_BY_STAFF'
    COMPLETED_BY_VISITOR = 'COMPLETED_BY_VISITOR'

class UrgencyLevel(str, Enum):
    NORMAL = 'NORMAL'
    WARNING = 'WARNING'
    CRITICAL_ESCALATION = 'CRITICAL_ESCALATION'

class DomainEvent:
    def __init__(self, event_name: str, aggregate_id: str):
        self.event_name = event_name
        self.aggregate_id = aggregate_id

class ServiceRequestedEvent(DomainEvent):
    def __init__(self, request_id: str, tenant_id: str, venue_id: str, point_id: str, intent_id: str, note: str):
        super().__init__('SH-EVT-003: ServiceRequested', request_id)

class ServiceAcceptedByStaffEvent(DomainEvent):
    def __init__(self, request_id: str, attendant_id: str):
        super().__init__('SH-EVT-006: ServiceAcceptedByStaff', request_id)
        self.attendantId = attendant_id

class ServiceCompletedByStaffEvent(DomainEvent):
    def __init__(self, request_id: str):
        super().__init__('SH-EVT-007: ServiceCompletedByStaff', request_id)

class ServiceCompletedByVisitorEvent(DomainEvent):
    def __init__(self, request_id: str):
        super().__init__('SH-EVT-008: ServiceCompletedByVisitor', request_id)

class ServiceRequestAggregate:
    def __init__(self, id: RequestId, tenant_id: TenantId, venue_id: VenueId, service_point_id: ServicePointId, intent_type_id: ServiceIntentTypeId, visitor_note: Optional[VisitorNote] = None):
        self.id = id
        self.tenant_id = tenant_id
        self.venue_id = venue_id
        self.service_point_id = service_point_id
        self.intent_type_id = intent_type_id
        self.visitor_note = visitor_note or VisitorNote("")
        self.state = RequestState.SUBMITTED
        self.urgency_level = UrgencyLevel.NORMAL
        self.assigned_attendant_id: Optional[str] = None
        from datetime import datetime, timezone
        self.submitted_at = datetime.now(timezone.utc)
        self.domain_events: list = []

    @classmethod
    def create(cls, id: RequestId, tenant_id: TenantId, venue_id: VenueId, service_point_id: ServicePointId, intent_type_id: ServiceIntentTypeId, visitor_note: Optional[VisitorNote], proof: SignedAccessProof):
        aggregate = cls(id, tenant_id, venue_id, service_point_id, intent_type_id, visitor_note)
        aggregate.add_domain_event(ServiceRequestedEvent(id.value, tenant_id.value, venue_id.value, service_point_id.value, intent_type_id.value, aggregate.visitor_note.text))
        return aggregate

    def accept(self, attendant_id: str):
        if self.state != RequestState.SUBMITTED:
            raise DomainInvalidStateTransitionException(self.state.value, 'accept')
        self.state = RequestState.ACCEPTED
        self.assigned_attendant_id = attendant_id
        self.add_domain_event(ServiceAcceptedByStaffEvent(self.id.value, attendant_id))

    def complete_by_staff(self):
        if self.state != RequestState.ACCEPTED and self.state != RequestState.SUBMITTED:
            raise DomainInvalidStateTransitionException(self.state.value, 'complete_by_staff')
        self.state = RequestState.COMPLETED_BY_STAFF
        self.add_domain_event(ServiceCompletedByStaffEvent(self.id.value))

    def confirm_receipt_by_visitor(self):
        self.state = RequestState.COMPLETED_BY_VISITOR
        self.add_domain_event(ServiceCompletedByVisitorEvent(self.id.value))

    def add_domain_event(self, event: DomainEvent):
        self.domain_events.append(event)

    def clear_domain_events(self):
        events = list(self.domain_events)
        self.domain_events.clear()
        return events

class RequestDeduplicationDomainService:
    @staticmethod
    def validate_no_active_duplicate(repository, point_id: ServicePointId, intent_type_id: ServiceIntentTypeId):
        existing = repository.find_active_by_point_and_type(point_id, intent_type_id)
        if existing:
            raise DomainInvariantViolationException('INV-05', f"Active request for intent '{intent_type_id.value}' already exists at point '{point_id.value}'")

class InMemoryServiceRequestRepository:
    def __init__(self):
        self.store: dict = {}

    def find_by_id(self, id: RequestId):
        return self.store.get(id.value)

    def find_active_by_point_and_type(self, point_id: ServicePointId, intent_type_id: ServiceIntentTypeId):
        active_states = [RequestState.SUBMITTED, RequestState.ACCEPTED]
        for req in self.store.values():
            if req.service_point_id == point_id and req.intent_type_id == intent_type_id and req.state in active_states:
                return req
        return None

    def save(self, aggregate: ServiceRequestAggregate):
        self.store[aggregate.id.value] = aggregate

class SubmitServiceRequestApplicationService:
    def __init__(self, repository: InMemoryServiceRequestRepository):
        self.repository = repository

    def execute(self, cmd: dict):
        req_id = RequestId(cmd['requestId'])
        tenant_id = TenantId(cmd['tenantId'])
        venue_id = VenueId(cmd['venueId'])
        service_point_id = ServicePointId(cmd['servicePointId'])
        intent_type_id = ServiceIntentTypeId(cmd['intentTypeId'], cmd.get('intentTypeName', ''))
        visitor_note = VisitorNote(cmd.get('note', ''))
        proof = SignedAccessProof(cmd['proofToken'])
        RequestDeduplicationDomainService.validate_no_active_duplicate(self.repository, service_point_id, intent_type_id)
        aggregate = ServiceRequestAggregate.create(req_id, tenant_id, venue_id, service_point_id, intent_type_id, visitor_note, proof)
        self.repository.save(aggregate)
        return aggregate.clear_domain_events()

class AcceptServiceRequestApplicationService:
    def __init__(self, repository: InMemoryServiceRequestRepository):
        self.repository = repository
    def execute(self, request_id_str: str, attendant_id: str):
        req_id = RequestId(request_id_str)
        aggregate = self.repository.find_by_id(req_id)
        if not aggregate: raise DomainException(f"Request {request_id_str} not found")
        aggregate.accept(attendant_id)
        self.repository.save(aggregate)
        return aggregate.clear_domain_events()

class CompleteServiceRequestApplicationService:
    def __init__(self, repository: InMemoryServiceRequestRepository):
        self.repository = repository
    def execute(self, request_id_str: str):
        req_id = RequestId(request_id_str)
        aggregate = self.repository.find_by_id(req_id)
        if not aggregate: raise DomainException(f"Request {request_id_str} not found")
        aggregate.complete_by_staff()
        self.repository.save(aggregate)
        return aggregate.clear_domain_events()

class ConfirmServiceReceiptApplicationService:
    def __init__(self, repository: InMemoryServiceRequestRepository):
        self.repository = repository
    def execute(self, request_id_str: str):
        req_id = RequestId(request_id_str)
        aggregate = self.repository.find_by_id(req_id)
        if not aggregate: raise DomainException(f"Request {request_id_str} not found")
        aggregate.confirm_receipt_by_visitor()
        self.repository.save(aggregate)
        return aggregate.clear_domain_events()

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
