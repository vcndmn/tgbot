#!/usr/bin/env python3
"""
Simple HTTP server for Telegram Mini App
"""

import http.server
import socketserver
import os
import json
from urllib.parse import urlparse, parse_qs
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MiniAppHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.dirname(os.path.abspath(__file__)), **kwargs)
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/':
            # Serve the main HTML file
            self.path = '/index.html'
        elif parsed_path.path == '/api/status':
            # API endpoint for bot status
            self.send_api_response({
                'status': 'online',
                'total_tasks': 5,
                'active_tasks': 3,
                'total_messages': 1247,
                'bot_running': True
            })
            return
        elif parsed_path.path == '/api/tasks':
            # API endpoint for tasks list
            self.send_api_response([
                {
                    'id': 'R1234567890',
                    'name': 'News Forwarding',
                    'source_chat_id': '-1001234567890',
                    'destination_chat_id': '-1001234567891',
                    'enabled': True,
                    'message_count': 245,
                    'keywords': 'news,update',
                    'exclude_keywords': 'spam',
                    'forward_replies': True,
                    'forward_forwards': True,
                    'delay_seconds': 0
                },
                {
                    'id': 'R1234567891',
                    'name': 'Group Messages',
                    'source_chat_id': '-1001234567892',
                    'destination_chat_id': '-1001234567893',
                    'enabled': False,
                    'message_count': 89,
                    'keywords': '',
                    'exclude_keywords': '',
                    'forward_replies': True,
                    'forward_forwards': False,
                    'delay_seconds': 5
                }
            ])
            return
        elif parsed_path.path == '/api/chats':
            # API endpoint for user's chats
            self.send_api_response([
                {
                    'id': '-1001234567890',
                    'name': 'News Channel',
                    'type': 'channel',
                    'username': '@news'
                },
                {
                    'id': '-1001234567891',
                    'name': 'My Group',
                    'type': 'group',
                    'username': None
                },
                {
                    'id': '123456789',
                    'name': 'John Doe',
                    'type': 'user',
                    'username': '@johndoe'
                }
            ])
            return
        
        # Serve static files
        super().do_GET()
    
    def do_POST(self):
        """Handle POST requests"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/api/tasks':
            # Create new task
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            task_data = json.loads(post_data.decode('utf-8'))
            
            logger.info(f"Creating task: {task_data}")
            
            # Here you would normally save to database
            # For now, just return success
            self.send_api_response({
                'success': True,
                'message': 'Task created successfully',
                'task_id': f"R{int(__import__('time').time())}"
            })
            return
        
        elif parsed_path.path == '/api/tasks/toggle':
            # Toggle task status
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            logger.info(f"Toggling task {data.get('task_id')}")
            
            self.send_api_response({
                'success': True,
                'message': 'Task status updated'
            })
            return
        
        elif parsed_path.path == '/api/tasks/delete':
            # Delete task
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            logger.info(f"Deleting task {data.get('task_id')}")
            
            self.send_api_response({
                'success': True,
                'message': 'Task deleted successfully'
            })
            return
        
        elif parsed_path.path == '/api/bot/start':
            # Start bot
            logger.info("Starting bot")
            self.send_api_response({
                'success': True,
                'message': 'Bot started successfully'
            })
            return
        
        elif parsed_path.path == '/api/bot/stop':
            # Stop bot
            logger.info("Stopping bot")
            self.send_api_response({
                'success': True,
                'message': 'Bot stopped successfully'
            })
            return
        
        else:
            self.send_error(404, "Not Found")
    
    def send_api_response(self, data):
        """Send JSON API response"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
        response = json.dumps(data, indent=2)
        self.wfile.write(response.encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to use our logger"""
        logger.info(f"{self.address_string()} - {format % args}")

def run_server(port=8080):
    """Run the HTTP server"""
    with socketserver.TCPServer(("", port), MiniAppHandler) as httpd:
        logger.info(f"🚀 Telegram Mini App server running on http://localhost:{port}")
        logger.info("📱 Open this URL in your browser to test the Mini App")
        logger.info("🔗 To use with Telegram Bot, set this as your web_app_url")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Server stopped")

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_server(port)
