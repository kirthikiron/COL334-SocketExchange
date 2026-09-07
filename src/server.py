import socket
import threading
import sys
from protocol import LineBuffer, send_message

# --- Global Shared State ---
exchange_lock = threading.Lock()
next_order_id = 0

order_book = {
    'JNST': {'BUY': [], 'SELL': []},
    'IMCT': {'BUY': [], 'SELL': []}
}

active_traders = {}             # Maps username -> socket object
md_subscribers = {              # Maps instrument -> set of socket objects
    'JNST': set(), 
    'IMCT': set()
}

def cleanup_client(conn, role, username):
    """
    Handles connection termination safely.
    Leaves outstanding orders in the order book.
    """
    with exchange_lock:
        if role == "TRADER" and username in active_traders:
            del active_traders[username]
        elif role == "MARKET_DATA":
            for instrument in md_subscribers:
                if conn in md_subscribers[instrument]:
                    md_subscribers[instrument].remove(conn)
    conn.close()

def process_message(msg, conn, client_state):
    parts = msg.strip().split()
    if not parts:
        return "ERROR empty message"
    
    command = parts[0].upper()
    role = client_state["role"]
    
    # Handling New Clients (UNKNOWN role)
    if role == "UNKNOWN":
        if command == "LOGIN":
            if len(parts) != 2:
                return "ERROR usage: LOGIN <username>"
            username = parts[1]
            
            with exchange_lock:
                if username in active_traders:
                    return "ERROR username taken"
                active_traders[username] = conn
                client_state["role"] = "TRADER"
                client_state["username"] = username
            return "OK"
            
        elif command == "SUBSCRIBE":
            if len(parts) != 2:
                return "ERROR usage: SUBSCRIBE <instrument>"
            instrument = parts[1]
            if instrument not in md_subscribers:
                return "ERROR unknown instrument"
                
            with exchange_lock:
                md_subscribers[instrument].add(conn)
                client_state["role"] = "MARKET_DATA"
            return "OK"
        else:
            return "ERROR must LOGIN or SUBSCRIBE first"

    # --- 2. HANDLING MARKET-DATA CLIENTS ---
    elif role == "MARKET_DATA":
        if command == "SUBSCRIBE":
            if len(parts) != 2:
                return "ERROR usage: SUBSCRIBE <instrument>"
            instrument = parts[1]
            if instrument not in md_subscribers:
                return "ERROR unknown instrument"
            with exchange_lock:
                md_subscribers[instrument].add(conn)
            return "OK"
            
        elif command == "UNSUBSCRIBE":
            if len(parts) != 2:
                return "ERROR usage: UNSUBSCRIBE <instrument>"
            instrument = parts[1]
            if instrument in md_subscribers and conn in md_subscribers[instrument]:
                md_subscribers[instrument].remove(conn)
            return "OK"
            
        elif command == "QUIT":
            return "QUIT"
        else:
            return "ERROR illegal command for market-data client"

    # --- 3. HANDLING TRADER CLIENTS ---
    elif role == "TRADER":
        if command in ["BUY", "SELL", "CANCEL"]:
            return "ERROR order matching not implemented yet"
        elif command == "QUIT":
            return "QUIT"
        else:
            return "ERROR illegal command for trader"

    return "ERROR unknown state"

def handle_client(conn, addr):
    buffer = LineBuffer()
    client_state = {"role": "UNKNOWN", "username": None}

    while True:
        try:
            data = conn.recv(4096)
            if not data:
                break
            
            messages = buffer.feed(data)
            for msg in messages:
                response = process_message(msg, conn, client_state)
                
                if response == "QUIT":
                    break
                elif response:
                    send_message(conn, response)
                    
        except (ConnectionResetError, BrokenPipeError):
            break
        except Exception as e:
            print(f"Error handling client {addr}: {e}", file=sys.stderr)
            break

    cleanup_client(conn, client_state["role"], client_state["username"])

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 server.py <host> <port>", file=sys.stderr)
        sys.exit(1)
        
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen()
    
    print(f"Exchange Server listening on {host}:{port}...", file=sys.stderr)
    
    try:
        while True:
            conn, addr = server_sock.accept()
            client_thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            client_thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    finally:
        server_sock.close()

if __name__ == "__main__":
    main()