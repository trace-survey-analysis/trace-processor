"""
HTTP Server for health checks to support Kubernetes liveness and readiness probes.
"""

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from utils.logging import Logger


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health check endpoints"""

    # Class-level variable to track application readiness
    is_ready = False
    logger = None

    def _send_response(self, status_code, content):
        """Helper method to send HTTP response"""
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def log_message(self, format, *args):
        """Override to use application logger instead of default handler"""
        if self.logger:
            self.logger.debug(f"Health check: {format % args}")

    def do_GET(self):
        """Handle GET requests"""
        if self.path == "/healthz/live":
            # Liveness probe - always return 200 if the server is running
            self._send_response(200, "OK")
        elif self.path == "/healthz/ready":
            # Readiness probe - return 200 only if the application is fully initialized
            if HealthCheckHandler.is_ready:
                self._send_response(200, "Ready")
            else:
                self._send_response(503, "Not Ready")
        else:
            self._send_response(404, "Not Found")


class HealthCheckServer:
    """Server for handling health check requests"""

    def __init__(self, port: int, logger: Logger):
        """Initialize the health check server"""
        self.port = port
        self.logger = logger
        self.server = None
        self.thread = None
        self.is_running = False

        # Set the logger in the handler class
        HealthCheckHandler.logger = logger

    def start(self):
        """Start the health check server in a separate thread"""
        if self.is_running:
            return

        self.logger.info(f"Starting health check server on port {self.port}")

        try:
            self.server = HTTPServer(("0.0.0.0", self.port), HealthCheckHandler)
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.thread.daemon = True
            self.thread.start()
            self.is_running = True
            self.logger.info("Health check server started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start health check server: {str(e)}", e)

    def set_ready(self, is_ready: bool):
        """Set the readiness state of the application"""
        HealthCheckHandler.is_ready = is_ready
        self.logger.info(f"Application readiness set to: {is_ready}")

    def stop(self):
        """Stop the health check server"""
        if self.server and self.is_running:
            self.logger.info("Stopping health check server")
            self.server.shutdown()
            self.server.server_close()
            self.is_running = False
            self.logger.info("Health check server stopped")
