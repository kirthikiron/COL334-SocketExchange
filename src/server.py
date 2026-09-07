import socket
import threading
import sys
from protocol import LineBuffer, send_message

# --- Thread-Safe Connection Wrapper ---
class Conn:
    def __init__(self, sock):
        self.sock = sock
        self.lock = threading.Lock()
        self.alive = True

    def send(self, msg):
        with self.lock:
            if not self.alive:
                return
            try:
                send_message(self.sock, msg)
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.alive = False

# --- Global Shared State ---
exchange_lock = threading.Lock()
next_order_id = 0

order_book = {
    'JNST': {'BUY': [], 'SELL': []},
    'IMCT': {'BUY': [], 'SELL': []}
}

active_traders = {}             # Maps username -> Conn object
md_subscribers = {              # Maps instrument -> set of Conn objects
    'JNST': set(), 
    'IMCT': set()
}

def cleanup_client(conn_obj, role, username):
    """Handles connection termination safely without cancelling open orders."""
    with exchange_lock:
        conn_obj.alive = False
        if role == "TRADER" and username in active_traders:
            if active_traders.get(username) == conn_obj:
                del active_traders[username]
        elif role == "MARKET_DATA":
            for instrument in md_subscribers:
                if conn_obj in md_subscribers[instrument]:
                    md_subscribers[instrument].remove(conn_obj)
    try:
        conn_obj.sock.close()
    except:
        pass

def process_message(msg, conn_obj, client_state):
    """
    Validates state and commands under lock, returning a list of 
    (target_conn_obj, response_string) pairs to send *after* unlocking.
    """
    parts = msg.strip().split()
    if not parts:
        return [(conn_obj, "ERROR empty message")]
    
    command = parts[0].upper()
    role = client_state["role"]
    outbound_messages = []
    
    with exchange_lock:
        # --- 1. UNKNOWN CLIENTS ---
        if role == "UNKNOWN":
            if command == "LOGIN":
                if len(parts) != 2:
                    return [(conn_obj, "ERROR usage: LOGIN <username>")]
                username = parts[1]
                if username in active_traders:
                    return [(conn_obj, "ERROR username taken")]
                
                active_traders[username] = conn_obj
                client_state["role"] = "TRADER"
                client_state["username"] = username
                return [(conn_obj, "OK")]
                
            elif command == "SUBSCRIBE":
                if len(parts) != 2:
                    return [(conn_obj, "ERROR usage: SUBSCRIBE <instrument>")]
                instrument = parts[1]
                if instrument not in md_subscribers:
                    return [(conn_obj, "ERROR unknown instrument")]
                
                md_subscribers[instrument].add(conn_obj)
                client_state["role"] = "MARKET_DATA"
                return [(conn_obj, "OK")]
            else:
                return [(conn_obj, "ERROR must LOGIN or SUBSCRIBE first")]

        # --- 2. MARKET-DATA CLIENTS ---
        elif role == "MARKET_DATA":
            if command == "SUBSCRIBE":
                if len(parts) != 2:
                    return [(conn_obj, "ERROR usage: SUBSCRIBE <instrument>")]
                instrument = parts[1]
                if instrument not in md_subscribers:
                    return [(conn_obj, "ERROR unknown instrument")]
                md_subscribers[instrument].add(conn_obj)
                return [(conn_obj, "OK")]
                
            elif command == "UNSUBSCRIBE":
                if len(parts) != 2:
                    return [(conn_obj, "ERROR usage: UNSUBSCRIBE <instrument>")]
                instrument = parts[1]
                if instrument not in md_subscribers:
                    return [(conn_obj, "ERROR unknown instrument")]
                if conn_obj in md_subscribers[instrument]:
                    md_subscribers[instrument].remove(conn_obj)
                return [(conn_obj, "OK")]
                
            elif command == "QUIT":
                return "QUIT"
            else:
                return [(conn_obj, "ERROR illegal command for market-data client")]

        # --- 3. TRADER CLIENTS ---
        elif role == "TRADER":
            if command in ["BUY", "SELL"]:
                # Matching engine goes here in the next step
                return [(conn_obj, "ERROR order matching not implemented yet")]
            elif command == "CANCEL":
                return [(conn_obj, "ERROR cancel not implemented yet")]
            elif command == "QUIT":
                return "QUIT"
            else:
                return [(conn_obj, "ERROR illegal command for trader")]

    return outbound_messages

def handle_client(sock, addr):
    conn_obj = Conn(sock)
    buffer = LineBuffer()
    client_state = {"role": "UNKNOWN", "username": None}

    while True:
        try:
            data = sock.recv(4096)
            if not data:
                break
            
            messages = buffer.feed(data)
            for msg in messages:
                result = process_message(msg, conn_obj, client_state)
                
                if result == "QUIT":
                    cleanup_client(conn_obj, client_state["role"], client_state["username"])
                    return
                
                # Send all collected responses safely after unlocking
                for target_conn, response_str in result:
                    target_conn.send(response_str)
                    
        except (ConnectionResetError, BrokenPipeError):
            break
        except Exception as e:
            print(f"Error handling client {addr}: {e}", file=sys.stderr)
            break

    cleanup_client(conn_obj, client_state["role"], client_state["username"])

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 server.py <host> <port>", file=sys.stderr)
        sys.exit(1)
        
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen(128)
    
    print(f"Exchange Server listening on {host}:{port}...", file=sys.stderr)
    
    try:
        while True:
            sock, addr = server_sock.accept()
            client_thread = threading.Thread(target=handle_client, args=(sock, addr), daemon=True)
            client_thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    finally:
        server_sock.close()

if __name__ == "__main__":
    main()