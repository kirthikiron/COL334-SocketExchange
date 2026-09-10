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

def cancel_order(username, order_id):
    """
    Searches the order book to cancel an active order.
    Returns the appropriate response string for the submitter.
    """
    for instrument in order_book:
        for side in ['BUY', 'SELL']:
            for i, order in enumerate(order_book[instrument][side]):
                if order['id'] == order_id:
                    if order['username'] == username:
                        order_book[instrument][side].pop(i)
                        return f"ORDER_CANCELLED {order_id}"
                    else:
                        return "ERROR not owner"
    return "ERROR unknown order"

def match_order(new_order):
    """
    Executes exact-price matching for a new order.
    Returns a list of (Conn, response_string) tuples to send asynchronously.
    """
    instrument = new_order['instrument']
    side = new_order['side']
    opposing_side = 'SELL' if side == 'BUY' else 'BUY'
    
    outbound_messages = []
    
    # 1. Acknowledge the order to the submitter immediately
    outbound_messages.append((new_order['conn'], f"ORDER_ACCEPTED {new_order['id']}"))
    
    resting_orders = order_book[instrument][opposing_side]
    i = 0
    
    # 2. Iterate through opposing orders (FIFO order is implicitly maintained by list append)
    while i < len(resting_orders) and new_order['remaining'] > 0:
        resting = resting_orders[i]
        
        # Exact-price match condition
        if resting['price'] == new_order['price']:
            trade_qty = min(new_order['remaining'], resting['remaining'])
            
            # Deduct quantities
            new_order['remaining'] -= trade_qty
            resting['remaining'] -= trade_qty
            
            # Format buyer and seller notifications
            buyer_conn = new_order['conn'] if side == 'BUY' else resting['conn']
            seller_conn = new_order['conn'] if side == 'SELL' else resting['conn']
            
            if buyer_conn.alive:
                outbound_messages.append((buyer_conn, f"BOUGHT {instrument} {trade_qty} {new_order['price']}"))
            if seller_conn.alive:
                outbound_messages.append((seller_conn, f"SOLD {instrument} {trade_qty} {new_order['price']}"))
                
            # Broadcast to Market-Data subscribers
            for md_conn in md_subscribers[instrument]:
                if md_conn.alive:
                    outbound_messages.append((md_conn, f"TRADE {instrument} {trade_qty} {new_order['price']}"))
            
            # Remove resting order if fully consumed
            if resting['remaining'] == 0:
                resting_orders.pop(i)
            else:
                i += 1
        else:
            i += 1

    # 3. If the incoming order still has quantity, add it to the book
    if new_order['remaining'] > 0:
        order_book[instrument][side].append(new_order)
        
    return outbound_messages



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
            username = client_state["username"]
            
            if command in ["BUY", "SELL"]:
                if len(parts) != 4:
                    return [(conn_obj, f"ERROR usage: {command} <instrument> <quantity> <price>")]
                
                instrument = parts[1]
                if instrument not in order_book:
                    return [(conn_obj, "ERROR unknown instrument")]
                
                try:
                    quantity = int(parts[2])
                    price = int(parts[3])
                    # Strictly enforce the 32-bit integer limits required by Section 2.1
                    if not (1 <= quantity <= 2147483647 and 1 <= price <= 2147483647):
                        return [(conn_obj, "ERROR quantity and price out of range")]
                except ValueError:
                    return [(conn_obj, "ERROR quantity and price must be integers")]
                
                global next_order_id
                
                # Construct the order dictionary
                new_order = {
                    'id': next_order_id,
                    'username': username,
                    'conn': conn_obj,
                    'instrument': instrument,
                    'side': command,
                    'price': price,
                    'quantity': quantity,
                    'remaining': quantity
                }
                next_order_id += 1
                
                # Execute matching and collect all resulting network messages
                matching_messages = match_order(new_order)
                outbound_messages.extend(matching_messages)
                
            elif command == "CANCEL":
                if len(parts) != 2:
                    return [(conn_obj, "ERROR usage: CANCEL <order_id>")]
                try:
                    target_id = int(parts[1])
                except ValueError:
                    return [(conn_obj, "ERROR invalid order_id")]
                    
                response = cancel_order(username, target_id)
                outbound_messages.append((conn_obj, response))
                
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
            try:
                sock, addr = server_sock.accept()
            except (ConnectionAbortedError, OSError) as e:
                print(f"accept() failed, continuing: {e}", file=sys.stderr)
                continue
            client_thread = threading.Thread(target=handle_client, args=(sock, addr), daemon=True)
            client_thread.start()
    except KeyboardInterrupt:
        print("\nShutting down server...", file=sys.stderr)
    finally:
        server_sock.close()

if __name__ == "__main__":
    main()